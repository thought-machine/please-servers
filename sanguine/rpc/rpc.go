// Package rpc implements the load-balancing logic for forwarding all the RPCs.
package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/golang/protobuf/proto"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/peterebden/go-cli-init"
	"golang.org/x/sync/errgroup"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/creds"
	rpb "github.com/thought-machine/please-servers/proto/record"
	"github.com/thought-machine/please-servers/sanguine/trie"
)

var log = cli.MustGetLogger()

// ServeForever serves on the given port until terminated.
func ServeForever(port int, trie *trie.Trie, keyFile, certFile string) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}
	srv := &server{
		trie:         trie,
		bytestreamRe: regexp.MustCompile("(?:uploads/[0-9a-f-]+/)?blobs/([0-9a-f]+)/([0-9]+)"),
	}
	s := grpc.NewServer(creds.OptionalTLS(keyFile, certFile,
		grpc.UnaryInterceptor(grpc_recovery.UnaryServerInterceptor()),
		grpc.StreamInterceptor(grpc_recovery.StreamServerInterceptor()),
		grpc.MaxRecvMsgSize(419430400), // 400MB
		grpc.MaxSendMsgSize(419430400),
	)...)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterActionCacheServer(s, srv)
	pb.RegisterContentAddressableStorageServer(s, srv)
	bs.RegisterByteStreamServer(s, srv)
	rpb.RegisterRecorderServer(s, srv)
	reflection.Register(s)
	log.Notice("Serving on :%d", port)
	err = s.Serve(lis)
	log.Fatalf("%s", err)
}

type server struct {
	trie         *trie.Trie
	bytestreamRe *regexp.Regexp
}

func (s *server) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	// This always does the same thing as Elan.
	// We might consider upping some of the size limits though since it will multiplex batch requests so will be more
	// efficient with bigger ones than Elan would be on its own.
	return &pb.ServerCapabilities{
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
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 0},
	}, nil
}

func (s *server) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	return s.trie.Get(req.ActionDigest.Hash).AC.GetActionResult(ctx, req)
}

func (s *server) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	return s.trie.Get(req.ActionDigest.Hash).AC.UpdateActionResult(ctx, req)
}

func (s *server) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	blobs := map[*trie.Server][]*pb.Digest{}
	for _, d := range req.BlobDigests {
		s := s.trie.Get(d.Hash)
		blobs[s] = append(blobs[s], d)
	}
	resp := &pb.FindMissingBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	for s, b := range blobs {
		s := s
		b := b
		g.Go(func() error {
			r, err := s.CAS.FindMissingBlobs(ctx, &pb.FindMissingBlobsRequest{
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
	}
	return resp, g.Wait()
}

func (s *server) BatchUpdateBlobs(ctx context.Context, req *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	blobs := map[*trie.Server][]*pb.BatchUpdateBlobsRequest_Request{}
	for _, d := range req.Requests {
		s := s.trie.Get(d.Digest.Hash)
		blobs[s] = append(blobs[s], d)
	}
	resp := &pb.BatchUpdateBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	for s, rs := range blobs {
		s := s
		rs := rs
		g.Go(func() error {
			r, err := s.CAS.BatchUpdateBlobs(ctx, &pb.BatchUpdateBlobsRequest{
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
	}
	return resp, g.Wait()
}

func (s *server) BatchReadBlobs(ctx context.Context, req *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {
	blobs := map[*trie.Server][]*pb.Digest{}
	for _, d := range req.Digests {
		s := s.trie.Get(d.Hash)
		blobs[s] = append(blobs[s], d)
	}
	resp := &pb.BatchReadBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	for s, d := range blobs {
		s := s
		d := d
		g.Go(func() error {
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
		resp, err := s.trie.Get(digest.Hash).CAS.BatchReadBlobs(ctx, &pb.BatchReadBlobsRequest{
			InstanceName: req.InstanceName,
			Digests:      []*pb.Digest{digest},
		})
		if err != nil {
			return err
		} else if len(resp.Responses) != 1 {
			return fmt.Errorf("missing blob in response") // shouldn't happen...
		} else if s := resp.Responses[0].Status; s.Code != int32(codes.OK) {
			return status.Errorf(codes.Code(s.Code), s.Message)
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
	client, err := s.trie.Get(hash).BS.Read(srv.Context(), req)
	if err != nil {
		return err
	}
	for {
		if resp, err := client.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		} else if err := srv.Send(resp); err != nil {
			return err
		}
	}
}

func (s *server) Write(srv bs.ByteStream_WriteServer) error {
	var client bs.ByteStream_WriteClient
	for {
		req, err := srv.Recv()
		if err != nil {
			if err == io.EOF {
				resp, err := client.CloseAndRecv()
				if err != nil {
					return err
				}
				return srv.SendAndClose(resp)
			}
			return err
		} else if client == nil {
			hash, err := s.bytestreamBlobName(req.ResourceName)
			if err != nil {
				return err
			}
			client, err = s.trie.Get(hash).BS.Write(srv.Context())
			if err != nil {
				return err
			}
		}
		if err := client.Send(req); err != nil {
			log.Warning("here: %s %s", err, req)
			return err
		}
	}
}

func (s *server) QueryWriteStatus(ctx context.Context, req *bs.QueryWriteStatusRequest) (*bs.QueryWriteStatusResponse, error) {
	hash, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return nil, err
	}
	return s.trie.Get(hash).BS.QueryWriteStatus(ctx, req)
}

// bytestreamBlobName returns the hash corresponding to a bytestream resource name.
func (s *server) bytestreamBlobName(bytestream string) (string, error) {
	matches := s.bytestreamRe.FindStringSubmatch(bytestream)
	if matches == nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid ResourceName: %s", bytestream)
	}
	return matches[1], nil
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
