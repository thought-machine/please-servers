// Package rpc implements the gRPC server for Elan.
// This contains implementations of the ContentAddressableStorage and ActionCache
// services, but not Execution (even by proxy).
package rpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"regexp"
	"strconv"
	"sync"

	// Necessary to register providers that we'll use.
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	bs "google.golang.org/genproto/googleapis/bytestream"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/op/go-logging.v1"

	"github.com/thought-machine/please-servers/creds"
)

var log = logging.MustGetLogger("rpc")

var bytesReceived = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "bytes_received_total",
})
var bytesServed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "bytes_served_total",
})

func init() {
	prometheus.MustRegister(bytesReceived)
	prometheus.MustRegister(bytesServed)
}

// ServeForever serves on the given port until terminated.
func ServeForever(port int, storage, keyFile, certFile string) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}
	bucket, err := blob.OpenBucket(context.Background(), storage)
	if err != nil {
		log.Fatalf("Failed to open storage %s: %v", storage, err)
	}
	srv := &server{
		bytestreamRe: regexp.MustCompile("(?:uploads/[0-9a-f-]+/)?blobs/([0-9a-f]+)/([0-9]+)"),
		bucket:       bucket,
	}
	s := grpc.NewServer(creds.OptionalTLS(keyFile, certFile,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			logUnaryRequests,
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			logStreamRequests,
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(),
		)),
	)...)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterActionCacheServer(s, srv)
	pb.RegisterContentAddressableStorageServer(s, srv)
	bs.RegisterByteStreamServer(s, srv)
	grpc_prometheus.Register(s)
	err = s.Serve(lis)
	log.Fatalf("%s", err)
}

type server struct {
	bucket       *blob.Bucket
	bytestreamRe *regexp.Regexp
}

func (s *server) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunction: []pb.DigestFunction_Value{
				pb.DigestFunction_SHA1,
				pb.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			MaxBatchTotalSizeBytes: 4012000, // 4000 Kelly-Bootle standard units
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 0},
	}, nil
}

func (s *server) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	ar := &pb.ActionResult{}
	if err := s.readBlobIntoMessage(ctx, "ac", req.ActionDigest, ar); err != nil {
		return nil, err
	}
	if req.InlineStdout && ar.StdoutDigest != nil {
		b, err := s.readAllBlob(ctx, "cas", ar.StdoutDigest)
		ar.StdoutRaw = b
		return ar, err
	}
	return ar, nil
}

func (s *server) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	return req.ActionResult, s.writeMessage(ctx, "ac", req.ActionDigest, req.ActionResult)
}

func (s *server) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	resp := &pb.FindMissingBlobsResponse{}
	var wg sync.WaitGroup
	wg.Add(len(req.BlobDigests))
	var mutex sync.Mutex
	for _, d := range req.BlobDigests {
		if len(d.Hash) != 64 {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid hash '%s'", d.Hash)
		}
		go func(d *pb.Digest) {
			if exists, _ := s.bucket.Exists(ctx, s.key("cas", d)); !exists {
				mutex.Lock()
				resp.MissingBlobDigests = append(resp.MissingBlobDigests, d)
				mutex.Unlock()
				log.Debug("Blob %s found to be missing", d.Hash)
			}
			wg.Done()
		}(d)
	}
	wg.Wait()
	return resp, nil
}

func (s *server) BatchUpdateBlobs(ctx context.Context, req *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	resp := &pb.BatchUpdateBlobsResponse{
		Responses: make([]*pb.BatchUpdateBlobsResponse_Response, len(req.Requests)),
	}
	var wg sync.WaitGroup
	wg.Add(len(req.Requests))
	for i, r := range req.Requests {
		go func(i int, r *pb.BatchUpdateBlobsRequest_Request) {
			rr := &pb.BatchUpdateBlobsResponse_Response{
				Status: &rpcstatus.Status{},
			}
			resp.Responses[i] = rr
			if len(r.Data) != int(r.Digest.SizeBytes) {
				rr.Status.Code = int32(codes.InvalidArgument)
				rr.Status.Message = fmt.Sprintf("Blob sizes do not match (%d / %d)", len(r.Data), r.Digest.SizeBytes)
			} else if err := s.writeBlob(ctx, "cas", r.Digest, bytes.NewReader(r.Data)); err != nil {
				rr.Status.Code = int32(status.Code(err))
				rr.Status.Message = err.Error()
			} else {
				log.Debug("Stored blob with digest %s", r.Digest.Hash)
			}
			wg.Done()
		}(i, r)
	}
	wg.Wait()
	return resp, nil
}

func (s *server) BatchReadBlobs(ctx context.Context, req *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {
	resp := &pb.BatchReadBlobsResponse{
		Responses: make([]*pb.BatchReadBlobsResponse_Response, len(req.Digests)),
	}
	var wg sync.WaitGroup
	wg.Add(len(req.Digests))
	for i, d := range req.Digests {
		go func(i int, d *pb.Digest) {
			rr := &pb.BatchReadBlobsResponse_Response{
				Status: &rpcstatus.Status{},
				Digest: d,
			}
			resp.Responses[i] = rr
			if data, err := s.readAllBlob(ctx, "cas", d); err != nil {
				rr.Status.Code = int32(status.Code(err))
				rr.Status.Message = err.Error()
			} else {
				rr.Data = data
			}
			wg.Done()
		}(i, d)
	}
	wg.Wait()
	return resp, nil
}

func (s *server) GetTree(*pb.GetTreeRequest, pb.ContentAddressableStorage_GetTreeServer) error {
	return status.Errorf(codes.Unimplemented, "GetTree not implemented")
}

func (s *server) Read(req *bs.ReadRequest, srv bs.ByteStream_ReadServer) error {
	digest, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	if req.ReadLimit == 0 {
		req.ReadLimit = -1
	}
	r, err := s.readBlob(srv.Context(), "cas", digest, req.ReadOffset, req.ReadLimit)
	if err != nil {
		return err
	}
	defer r.Close()
	buf := make([]byte, 64*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if err := srv.Send(&bs.ReadResponse{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (s *server) Write(srv bs.ByteStream_WriteServer) error {
	req, err := srv.Recv()
	if err != nil {
		return err
	} else if req.ResourceName == "" {
		return status.Errorf(codes.InvalidArgument, "missing ResourceName")
	}
	digest, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	r := &bytestreamReader{stream: srv, buf: req.Data}
	if err := s.writeBlob(srv.Context(), "cas", digest, r); err != nil {
		return err
	} else if r.TotalSize != digest.SizeBytes {
		return status.Errorf(codes.InvalidArgument, "invalid digest size (digest: %d, wrote: %d)", digest.SizeBytes, r.TotalSize)
	}
	log.Debug("Stored blob with hash %s", digest.Hash)
	return srv.SendAndClose(&bs.WriteResponse{
		CommittedSize: r.TotalSize,
	})
}

func (s *server) QueryWriteStatus(ctx context.Context, req *bs.QueryWriteStatusRequest) (*bs.QueryWriteStatusResponse, error) {
	// We don't track partial writes or allow resuming them. Might add later if plz gains
	// the ability to do this as a client.
	return nil, status.Errorf(codes.NotFound, "write %s not found", req.ResourceName)
}

func (s *server) readBlob(ctx context.Context, prefix string, digest *pb.Digest, offset, length int64) (io.ReadCloser, error) {
	r, err := s.bucket.NewRangeReader(ctx, s.key(prefix, digest), offset, length, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, status.Errorf(codes.NotFound, "Blob %s not found", digest.Hash)
		}
		return nil, err
	}
	return &countingReader{r: r}, err
}

func (s *server) readAllBlob(ctx context.Context, prefix string, digest *pb.Digest) ([]byte, error) {
	r, err := s.readBlob(ctx, prefix, digest, 0, -1)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

func (s *server) readBlobIntoMessage(ctx context.Context, prefix string, digest *pb.Digest, message proto.Message) error {
	if b, err := s.readAllBlob(ctx, prefix, digest); err != nil {
		return err
	} else if err := proto.Unmarshal(b, message); err != nil {
		return status.Errorf(codes.Unknown, "%s", err)
	}
	return nil
}

func (s *server) writeBlob(ctx context.Context, prefix string, digest *pb.Digest, r io.Reader) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := s.bucket.NewWriter(ctx, s.key(prefix, digest), nil)
	if err != nil {
		return err
	}
	n, err := io.Copy(w, r)
	bytesReceived.Add(float64(n))
	if err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (s *server) writeMessage(ctx context.Context, prefix string, digest *pb.Digest, message proto.Message) error {
	b, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return s.writeBlob(ctx, prefix, digest, bytes.NewReader(b))
}

func (s *server) key(prefix string, digest *pb.Digest) string {
	return fmt.Sprintf("%s/%c%c/%s", prefix, digest.Hash[0], digest.Hash[1], digest.Hash)
}

// bytestreamDigest returns the digest corresponding to a bytestream resource name.
func (s *server) bytestreamBlobName(bytestream string) (*pb.Digest, error) {
	matches := s.bytestreamRe.FindStringSubmatch(bytestream)
	if matches == nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid ResourceName: %s", bytestream)
	}
	size, _ := strconv.Atoi(matches[2])
	return &pb.Digest{
		Hash:      matches[1],
		SizeBytes: int64(size),
	}, nil
}

// A bytestreamReader wraps the incoming byte stream into an io.Reader
type bytestreamReader struct {
	stream    bs.ByteStream_WriteServer
	buf       []byte
	TotalSize int64
}

func (r *bytestreamReader) Read(buf []byte) (int, error) {
	n, err := r.read(buf)
	r.TotalSize += int64(n)
	return n, err
}

func (r *bytestreamReader) read(buf []byte) (int, error) {
	for {
		if n := len(buf); len(r.buf) >= n {
			// can fulfil entire read out of existing buffer
			copy(buf, r.buf[:n])
			r.buf = r.buf[n:]
			return n, nil
		}
		// need to read more to fulfil request
		req, err := r.stream.Recv()
		if err != nil {
			if err == io.EOF {
				// at the end, so copy whatever we have left.
				copy(buf, r.buf)
				return len(r.buf), io.EOF
			}
			return 0, err
		} else if req.WriteOffset != r.TotalSize {
			return 0, status.Errorf(codes.InvalidArgument, "incorrect WriteOffset (was %d, should be %d)", req.WriteOffset, r.TotalSize)
		}
		r.buf = append(r.buf, req.Data...)
	}
}

// A countingReader wraps a ReadCloser and counts bytes read from it.
type countingReader struct {
	r io.ReadCloser
}

func (r *countingReader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)
	bytesServed.Add(float64(n))
	return n, err
}

func (r *countingReader) Close() error {
	return r.r.Close()
}

func logUnaryRequests(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		if status.Code(err) != codes.NotFound {
			log.Error("Error handling %s: %s", info.FullMethod, err)
		} else {
			log.Debug("Not found on %s: %s", info.FullMethod, err)
		}
	} else {
		log.Debug("Handled %s successfully", info.FullMethod)
	}
	return resp, err
}

func logStreamRequests(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, ss)
	if err != nil {
		log.Error("Error handling %s: %s", info.FullMethod, err)
	} else {
		log.Debug("Handled %s successfully", info.FullMethod)
	}
	return err
}
