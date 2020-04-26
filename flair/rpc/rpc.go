// Package rpc implements the load-balancing logic for forwarding all the RPCs.
package rpc

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"

	apb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/peterebden/go-cli-init"
	"github.com/peterebden/go-sri"
	"golang.org/x/sync/errgroup"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/grpc"
	"github.com/thought-machine/please-servers/flair/trie"
	rpb "github.com/thought-machine/please-servers/proto/record"
)

var log = cli.MustGetLogger()

// ServeForever serves on the given port until terminated.
func ServeForever(host string, port int, casReplicator, assetReplicator, executorReplicator *trie.Replicator, keyFile, certFile string) {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatalf("Failed to listen on %s:%d: %v", host, port, err)
	}
	srv := &server{
		replicator:      casReplicator,
		assetReplicator: assetReplicator,
		exeReplicator:   executorReplicator,
		bytestreamRe:    regexp.MustCompile("(?:uploads/[0-9a-f-]+/)?blobs/([0-9a-f]+)/([0-9]+)"),
	}
	s := grpc.NewServer(creds.OptionalTLS(keyFile, certFile,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			creds.LogUnaryRequests,
			grpc_recovery.UnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			creds.LogStreamRequests,
			grpc_recovery.StreamServerInterceptor(),
		)),
		grpc.MaxRecvMsgSize(419430400), // 400MB
		grpc.MaxSendMsgSize(419430400),
	)...)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterActionCacheServer(s, srv)
	pb.RegisterContentAddressableStorageServer(s, srv)
	bs.RegisterByteStreamServer(s, srv)
	rpb.RegisterRecorderServer(s, srv)
	if assetReplicator != nil {
		apb.RegisterFetchServer(s, srv)
	}
	if executorReplicator != nil {
		pb.RegisterExecutionServer(s, srv)
	}
	reflection.Register(s)
	log.Notice("Serving on %s:%d", host, port)
	err = s.Serve(lis)
	log.Fatalf("%s", err)
}

type server struct {
	replicator, assetReplicator, exeReplicator *trie.Replicator
	bytestreamRe                               *regexp.Regexp
}

func (s *server) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	// This always does the same thing as Elan.
	// We might consider upping some of the size limits though since it will multiplex batch requests so will be more
	// efficient with bigger ones than Elan would be on its own.
	caps := &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunction: []pb.DigestFunction_Value{
				pb.DigestFunction_SHA1,
				pb.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			MaxBatchTotalSizeBytes: 4048000, // 4000 Kelly-Bootle standard units
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 1}, // optimistic
	}
	if s.exeReplicator != nil {
		caps.ExecutionCapabilities = &pb.ExecutionCapabilities{
			DigestFunction: pb.DigestFunction_SHA256,
			ExecEnabled:    true,
		}
	}
	return caps, nil
}

func (s *server) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (ar *pb.ActionResult, err error) {
	err = s.replicator.Sequential(req.ActionDigest.Hash, func(s *trie.Server) error {
		a, e := s.AC.GetActionResult(ctx, req)
		if e == nil {
			ar = a
		}
		return e
	})
	return
}

func (s *server) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (ar *pb.ActionResult, err error) {
	err = s.replicator.Parallel(req.ActionDigest.Hash, func(s *trie.Server) error {
		a, e := s.AC.UpdateActionResult(ctx, req)
		if e == nil {
			ar = a
		}
		return e
	})
	return
}

func (s *server) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	// Note that the replication strategy here assumes that the set of blobs to go to each replica is the same as for
	// the primary (basically that the replication offset is an integer multiple of the size of each hash block, and those
	// blocks are of a consistent size). This currently fits our setup.
	blobs := map[*trie.Server][]*pb.Digest{}
	for _, d := range req.BlobDigests {
		s := s.replicator.Trie.Get(d.Hash)
		blobs[s] = append(blobs[s], d)
	}
	resp := &pb.FindMissingBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	for srv, b := range blobs {
		srv := srv
		b := b
		g.Go(func() error {
			return s.replicator.Sequential(srv.Start, func(srv *trie.Server) error {
				r, err := srv.CAS.FindMissingBlobs(ctx, &pb.FindMissingBlobsRequest{
					InstanceName: req.InstanceName,
					BlobDigests:  b,
				})
				if err != nil {
					return err
				}
				mutex.Lock()
				defer mutex.Unlock()
				resp.MissingBlobDigests = append(resp.MissingBlobDigests, r.MissingBlobDigests...)
				return nil
			})
		})
	}
	return resp, g.Wait()
}

func (s *server) BatchUpdateBlobs(ctx context.Context, req *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	blobs := map[*trie.Server][]*pb.BatchUpdateBlobsRequest_Request{}
	for _, d := range req.Requests {
		s := s.replicator.Trie.Get(d.Digest.Hash)
		blobs[s] = append(blobs[s], d)
	}
	resp := &pb.BatchUpdateBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	for srv, rs := range blobs {
		srv := srv
		rs := rs
		g.Go(func() error {
			return s.replicator.Parallel(srv.Start, func(srv *trie.Server) error {
				r, err := srv.CAS.BatchUpdateBlobs(ctx, &pb.BatchUpdateBlobsRequest{
					InstanceName: req.InstanceName,
					Requests:     rs,
				})
				if err != nil {
					return err
				}
				mutex.Lock()
				defer mutex.Unlock()
				resp.Responses = append(resp.Responses, r.Responses...)
				return nil
			})
		})
	}
	return resp, g.Wait()
}

func (s *server) BatchReadBlobs(ctx context.Context, req *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {
	blobs := map[*trie.Server][]*pb.Digest{}
	for _, d := range req.Digests {
		s := s.replicator.Trie.Get(d.Hash)
		blobs[s] = append(blobs[s], d)
	}
	resp := &pb.BatchReadBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	for srv, d := range blobs {
		srv := srv
		d := d
		g.Go(func() error {
			return s.replicator.Sequential(srv.Start, func(s *trie.Server) error {
				r, err := s.CAS.BatchReadBlobs(ctx, &pb.BatchReadBlobsRequest{
					InstanceName: req.InstanceName,
					Digests:      d,
				})
				if err != nil {
					return err
				}
				mutex.Lock()
				defer mutex.Unlock()
				resp.Responses = append(resp.Responses, r.Responses...)
				return nil
			})
		})
	}
	return resp, g.Wait()
}

func (s *server) GetTree(req *pb.GetTreeRequest, srv pb.ContentAddressableStorage_GetTreeServer) error {
	if req.PageSize > 0 {
		return status.Errorf(codes.Unimplemented, "page_size not implemented for GetTree")
	} else if req.PageToken != "" {
		return status.Errorf(codes.Unimplemented, "page tokens not implemented for GetTree")
	} else if req.RootDigest == nil {
		return status.Errorf(codes.InvalidArgument, "missing root_digest field")
	}
	// The individual directories need to be split up too...
	var g errgroup.Group
	var mutex sync.Mutex
	ctx := srv.Context()
	r := &pb.GetTreeResponse{}

	var fetchDir func(digest *pb.Digest) error
	fetchDir = func(digest *pb.Digest) error {
		var resp *pb.BatchReadBlobsResponse
		if err := s.replicator.Sequential(digest.Hash, func(s *trie.Server) error {
			r, err := s.CAS.BatchReadBlobs(ctx, &pb.BatchReadBlobsRequest{
				InstanceName: req.InstanceName,
				Digests:      []*pb.Digest{digest},
			})
			if err != nil {
				return err
			} else if len(r.Responses) != 1 {
				return fmt.Errorf("missing blob in response") // shouldn't happen...
			} else if s := r.Responses[0].Status; s.Code != int32(codes.OK) {
				return status.Errorf(codes.Code(s.Code), s.Message)
			}
			resp = r
			return nil
		}); err != nil {
			return err
		}
		dir := &pb.Directory{}
		if err := proto.Unmarshal(resp.Responses[0].Data, dir); err != nil {
			return err
		}
		for _, dir := range dir.Directories {
			digest := dir.Digest
			g.Go(func() error {
				return fetchDir(digest)
			})
		}
		mutex.Lock()
		defer mutex.Unlock()
		r.Directories = append(r.Directories, dir)
		return nil
	}

	g.Go(func() error {
		return fetchDir(req.RootDigest)
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return srv.Send(r)
}

func (s *server) Read(req *bs.ReadRequest, srv bs.ByteStream_ReadServer) error {
	hash, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	// This needs a little bookkeeping since we can fail partway and restart on another server.
	// That's actually OK as long as we jiggle the ReadOffset appropriately.
	return s.replicator.Sequential(hash, func(s *trie.Server) error {
		client, err := s.BS.Read(srv.Context(), req)
		if err != nil {
			return err
		}
		for {
			resp, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			} else if err := srv.Send(resp); err != nil {
				return err
			}
			req.ReadOffset += int64(len(resp.Data))
		}
	})
}

func (s *server) Write(srv bs.ByteStream_WriteServer) error {
	// This is a bit different to (and rather more complex than) Read since we have to perform all writes in parallel,
	// but we only receive from the client once so we have to fan out the messages.
	req, err := srv.Recv()
	if err != nil {
		return err
	}
	hash, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	chs := make([]chan *bs.WriteRequest, s.replicator.Replicas)
	chch := make(chan chan *bs.WriteRequest, s.replicator.Replicas)
	for i := range chs {
		ch := make(chan *bs.WriteRequest, 10) // Bit of arbitrary buffering so they can get a little out of sync if needed.
		ch <- req
		chs[i] = ch
		chch <- ch
	}
	var g errgroup.Group
	g.Go(func() error {
		for {
			req, err := srv.Recv()
			if err != nil {
				for _, ch := range chs {
					close(ch)
				}
				if err == io.EOF {
					return nil
				}
				return err
			}
			for _, ch := range chs {
				ch <- req
			}
		}
	})
	var resp *bs.WriteResponse
	if err := s.replicator.Parallel(hash, func(s *trie.Server) error {
		ch := <-chch
		client, err := s.BS.Write(srv.Context())
		if err != nil {
			return err
		}
		for req := range ch {
			if err := client.Send(req); err != nil {
				return err
			}
		}
		r, err := client.CloseAndRecv()
		if err != nil {
			return err
		}
		resp = r
		return nil
	}); err != nil {
		return err
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return srv.SendAndClose(resp)
}

func (s *server) QueryWriteStatus(ctx context.Context, req *bs.QueryWriteStatusRequest) (resp *bs.QueryWriteStatusResponse, err error) {
	hash, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return nil, err
	}
	err = s.replicator.Sequential(hash, func(s *trie.Server) error {
		r, e := s.BS.QueryWriteStatus(ctx, req)
		if e == nil {
			resp = r
		}
		return e
	})
	return resp, err
}

// bytestreamBlobName returns the hash corresponding to a bytestream resource name.
func (s *server) bytestreamBlobName(bytestream string) (string, error) {
	matches := s.bytestreamRe.FindStringSubmatch(bytestream)
	if matches == nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid ResourceName: %s", bytestream)
	}
	return matches[1], nil
}

func (s *server) FetchDirectory(ctx context.Context, req *apb.FetchDirectoryRequest) (resp *apb.FetchDirectoryResponse, err error) {
	err = s.assetReplicator.Sequential(s.assetHash(req.Qualifiers), func(s *trie.Server) error {
		resp, err = s.Fetch.FetchDirectory(ctx, req)
		return err
	})
	return resp, err
}

func (s *server) FetchBlob(ctx context.Context, req *apb.FetchBlobRequest) (resp *apb.FetchBlobResponse, err error) {
	err = s.assetReplicator.Sequential(s.assetHash(req.Qualifiers), func(s *trie.Server) error {
		resp, err = s.Fetch.FetchBlob(ctx, req)
		return err
	})
	return resp, err
}

// assetHash returns a hash used to key requests to the asset service.
// It attempts to work it out from any subresource integrity qualifiers in order to put the fetch closer to the
// CAS server (assuming their geometries match...) but on failure just picks something random.
func (s *server) assetHash(quals []*apb.Qualifier) string {
	for _, q := range quals {
		if q.Name == "checksum.sri" {
			if c, err := sri.NewChecker(q.Value); err == nil {
				if hashes := c.Expected("sha256"); len(hashes) > 0 {
					// Need to convert from base64 to hex...
					if b, err := base64.StdEncoding.DecodeString(hashes[0]); err == nil {
						return hex.EncodeToString(b)
					}
				}
			}
		}
	}
	// Didn't find anything above, for whatever reason. Make up something random.
	key := [32]byte{}
	rand.Read(key[:])
	return hex.EncodeToString(key[:])
}

func (s *server) Execute(req *pb.ExecuteRequest, stream pb.Execution_ExecuteServer) error {
	return s.exeReplicator.Sequential(req.ActionDigest.Hash, func(srv *trie.Server) error {
		client, err := srv.Exe.Execute(stream.Context(), req)
		if err != nil {
			return err
		}
		return s.streamExecution(client, stream)
	})
}

func (s *server) WaitExecution(req *pb.WaitExecutionRequest, stream pb.Execution_WaitExecutionServer) error {
	return s.exeReplicator.Sequential(req.Name, func(srv *trie.Server) error {
		client, err := srv.Exe.WaitExecution(stream.Context(), req)
		if err != nil {
			return err
		}
		return s.streamExecution(client, stream)
	})
}

func (s *server) streamExecution(client pb.Execution_ExecuteClient, server pb.Execution_ExecuteServer) error {
	for {
		resp, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		} else if err := server.Send(resp); err != nil {
			return err
		}
	}
}

// Record and Query are silently unimplemented for now. They are nontrivial in terms of how we
// distribute & later retrieve the recorded digests (e.g. Query probably needs to be able to hit all servers).
// Right now we don't actually need it to work in this scenario anyway.
func (s *server) Record(ctx context.Context, req *rpb.RecordRequest) (*rpb.RecordResponse, error) {
	return &rpb.RecordResponse{}, nil
}

func (s *server) Query(ctx context.Context, req *rpb.QueryRequest) (*rpb.QueryResponse, error) {
	return &rpb.QueryResponse{}, nil
}
