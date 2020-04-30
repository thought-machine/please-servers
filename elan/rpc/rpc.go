// Package rpc implements the gRPC server for Elan.
// This contains implementations of the ContentAddressableStorage and ActionCache
// services, but not Execution (even by proxy).
package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"path"
	"regexp"
	"strings"
	"strconv"
	"sync"
	"time"

	// Necessary to register providers that we'll use.
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/golang/protobuf/proto"
	"github.com/peterebden/go-cli-init"
	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	bs "google.golang.org/genproto/googleapis/bytestream"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rpb "github.com/thought-machine/please-servers/proto/record"
	"github.com/thought-machine/please-servers/grpcutil"
)

const timeout = 2 * time.Minute

var log = cli.MustGetLogger()

var bytesReceived = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "bytes_received_total",
})
var bytesServed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "bytes_served_total",
})
var readLatencies = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "elan",
	Name:      "read_latency_seconds",
	Buckets:   prometheus.DefBuckets,
})
var writeLatencies = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "elan",
	Name:      "write_latency_seconds",
	Buckets:   prometheus.DefBuckets,
})
var readDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "elan",
	Name:      "read_duration_seconds",
	Buckets:   prometheus.DefBuckets,
})
var writeDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "elan",
	Name:      "write_duration_seconds",
	Buckets:   prometheus.DefBuckets,
})

func init() {
	prometheus.MustRegister(mainBytesTotal)
	prometheus.MustRegister(mainItemsTotal)
	prometheus.MustRegister(mainGetsTotal)
	prometheus.MustRegister(mainHitsTotal)
	prometheus.MustRegister(mainEvictionsTotal)
	prometheus.MustRegister(hotBytesTotal)
	prometheus.MustRegister(hotItemsTotal)
	prometheus.MustRegister(hotGetsTotal)
	prometheus.MustRegister(hotHitsTotal)
	prometheus.MustRegister(hotEvictionsTotal)
	prometheus.MustRegister(bytesReceived)
	prometheus.MustRegister(bytesServed)
	prometheus.MustRegister(readLatencies)
	prometheus.MustRegister(writeLatencies)
	prometheus.MustRegister(readDurations)
	prometheus.MustRegister(writeDurations)
}

// ServeForever serves on the given port until terminated.
func ServeForever(host string, port, cachePort int, storage, keyFile, certFile, self string, peers []string, maxCacheSize, maxCacheItemSize int64, fileCachePath string, maxFileCacheSize int64, parallelism int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}
	bucket, err := blob.OpenBucket(context.Background(), storage)
	if err != nil {
		log.Fatalf("Failed to open storage %s: %v", storage, err)
	}
	srv := &server{
		bytestreamRe:     regexp.MustCompile("(?:uploads/[0-9a-f-]+/)?blobs/([0-9a-f]+)/([0-9]+)"),
		bucket:           bucket,
		maxCacheItemSize: maxCacheItemSize,
		limiter:          make(chan struct{}, parallelism),
	}
	srv.cache = newCache(srv, host, cachePort, self, peers, maxCacheSize, maxCacheItemSize)
	if fileCachePath != "" && maxFileCacheSize > 0 {
		c, err := newFileCache(fileCachePath, maxFileCacheSize)
		if err != nil {
			log.Fatalf("Failed to create file cache: %s", err)
		}
		srv.fileCache = c
	}
	s := grpcutil.NewServer(keyFile, certFile)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterActionCacheServer(s, srv)
	pb.RegisterContentAddressableStorageServer(s, srv)
	bs.RegisterByteStreamServer(s, srv)
	rpb.RegisterRecorderServer(s, srv)
	err = s.Serve(lis)
	log.Fatalf("%s", err)
}

type server struct {
	bucket           *blob.Bucket
	bytestreamRe     *regexp.Regexp
	limiter          chan struct{}
	cache            *cache
	maxCacheItemSize int64
	fileCache        *fileCache
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
			MaxBatchTotalSizeBytes: 4048000, // 4000 Kelly-Bootle standard units
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 0},
	}, nil
}

func (s *server) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ar := &pb.ActionResult{}
	if err := s.readBlobIntoMessage(ctx, "ac", req.ActionDigest, ar); err != nil {
		return nil, err
	}
	if req.InlineStdout && ar.StdoutDigest != nil {
		// The docs say that the server MAY omit inlining, even if asked. Hence we assume that if we can't find it here
		// we might be in a sharded setup where we don't have the stdout digest, and it's better to return what we can
		// (the client can still request the actual blob themselves).
		if b, err := s.readAllBlob(ctx, "cas", ar.StdoutDigest); err == nil {
			ar.StdoutRaw = b
		}
	}
	return ar, nil
}

func (s *server) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	return req.ActionResult, s.writeMessage(ctx, "ac", req.ActionDigest, req.ActionResult)
}

func (s *server) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp := &pb.FindMissingBlobsResponse{}
	var wg sync.WaitGroup
	wg.Add(len(req.BlobDigests))
	var mutex sync.Mutex
	for _, d := range req.BlobDigests {
		if len(d.Hash) != 64 {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid hash '%s'", d.Hash)
		}
		go func(d *pb.Digest) {
			key := s.key("cas", d)
			if !s.blobExists(ctx, key) {
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

// blobExists returns true if this blob has been previously uploaded.
func (s *server) blobExists(ctx context.Context, key string) bool {
	if s.cache.Has(key) {
		return true
	}
	return s.blobExistsUncached(ctx, key)
}

// blobExistsUncached returns true if this blob exists in the underlying storage (but not the cache)
func (s *server) blobExistsUncached(ctx context.Context, key string) bool {
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	exists, _ := s.bucket.Exists(ctx, key)
	return exists
}

func (s *server) BatchUpdateBlobs(ctx context.Context, req *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp := &pb.BatchUpdateBlobsResponse{
		Responses: make([]*pb.BatchUpdateBlobsResponse_Response, len(req.Requests)),
	}
	var wg sync.WaitGroup
	wg.Add(len(req.Requests))
	for i, r := range req.Requests {
		go func(i int, r *pb.BatchUpdateBlobsRequest_Request) {
			rr := &pb.BatchUpdateBlobsResponse_Response{
				Digest: r.Digest,
				Status: &rpcstatus.Status{},
			}
			resp.Responses[i] = rr
			if len(r.Data) != int(r.Digest.SizeBytes) {
				rr.Status.Code = int32(codes.InvalidArgument)
				rr.Status.Message = fmt.Sprintf("Blob sizes do not match (%d / %d)", len(r.Data), r.Digest.SizeBytes)
			} else if s.blobExistsUncached(ctx, s.key("cas", r.Digest)) {
				log.Debug("Blob %s already exists remotely", r.Digest.Hash)
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
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp := &pb.BatchReadBlobsResponse{
		Responses: make([]*pb.BatchReadBlobsResponse_Response, len(req.Digests)),
	}
	var wg sync.WaitGroup
	var size int64
	wg.Add(len(req.Digests))
	for i, d := range req.Digests {
		size += d.SizeBytes
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
	log.Debug("Served BatchReadBlobs request of %d blobs, total %d bytes in %s", len(req.Digests), size, time.Since(start))
	return resp, nil
}

func (s *server) GetTree(req *pb.GetTreeRequest, srv pb.ContentAddressableStorage_GetTreeServer) error {
	if req.PageSize > 0 {
		return status.Errorf(codes.Unimplemented, "page_size not implemented for GetTree")
	} else if req.PageToken != "" {
		return status.Errorf(codes.Unimplemented, "page tokens not implemented for GetTree")
	} else if req.RootDigest == nil {
		return status.Errorf(codes.InvalidArgument, "missing root_digest field")
	}
	key := s.key("tree", req.RootDigest)
	resp := &pb.GetTreeResponse{}
	if s.cache.GetProto(key, resp) {
		return srv.Send(resp)
	}
	resp, err := s.getWholeTree(req.RootDigest)
	if err != nil {
		return err
	}
	return srv.Send(resp)
}

// getWholeTree builds a directory tree proto with no caching.
// It's also called from the cache.
func (s *server) getWholeTree(digest *pb.Digest) (*pb.GetTreeResponse, error) {
	resp := &pb.GetTreeResponse{}
	for r := range s.getTree(digest) {
		if r.Err != nil {
			return nil, r.Err
		}
		resp.Directories = append(resp.Directories, r.Dir)
	}
	return resp, nil
}

func (s *server) Read(req *bs.ReadRequest, srv bs.ByteStream_ReadServer) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(srv.Context(), timeout)
	defer cancel()
	defer func() { readDurations.Observe(time.Since(start).Seconds()) }()
	digest, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	if req.ReadLimit == 0 {
		req.ReadLimit = -1
	}
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	r, err := s.readBlob(ctx, "cas", digest, req.ReadOffset, req.ReadLimit)
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
				log.Debug("Completed ByteStream.Read request of %d bytes in %s", digest.SizeBytes, time.Since(start))
				return nil
			}
			return err
		}
	}
}

func (s *server) Write(srv bs.ByteStream_WriteServer) error {
	ctx, cancel := context.WithTimeout(srv.Context(), timeout)
	defer cancel()
	start := time.Now()
	defer func() { writeDurations.Observe(time.Since(start).Seconds()) }()
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
	if err := s.writeBlob(ctx, "cas", digest, r); err != nil {
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

func (s *server) Record(ctx context.Context, req *rpb.RecordRequest) (*rpb.RecordResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	b, _ := proto.Marshal(req.Digest)
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	if err := s.bucket.WriteAll(ctx, s.labelKey(req.Name), b, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to write record: %s", err)
	}
	return &rpb.RecordResponse{}, nil
}

func (s *server) Query(ctx context.Context, req *rpb.QueryRequest) (*rpb.QueryResponse, error) {
	resp := &rpb.QueryResponse{}
	for _, query := range req.Queries {
		digests, err := s.query(ctx, s.labelKey(query))
		if err != nil {
			return nil, err
		}
		resp.Digests = append(resp.Digests, digests...)
	}
	return resp, nil
}

func (s *server) query(ctx context.Context, key string) ([]*rpb.Digest, error) {
	if !strings.HasSuffix(key, "/...") {
		return s.queryOne(ctx, key)
	}
	ctx, cancel := context.WithTimeout(ctx, 10 * time.Minute)
	defer cancel()
	iter := s.bucket.List(&blob.ListOptions{Prefix: strings.TrimSuffix(key, "...")})
	ret := []*rpb.Digest{}
	var g errgroup.Group
	var mtx sync.Mutex
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		} else if obj.IsDir {
			continue
		}
		key := obj.Key
		g.Go(func() error {
			digest, err := s.queryOne(ctx, key)
			if err != nil {
				return err
			}
			mtx.Lock()
			defer mtx.Unlock()
			ret = append(ret, digest...)
			return nil
		})
	}
	return ret, g.Wait()
}

func (s *server) queryOne(ctx context.Context, key string) ([]*rpb.Digest, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	digest := &rpb.Digest{}
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	b, err := s.bucket.ReadAll(ctx, key)
	if err != nil {
		return nil, err
	} else if err := proto.Unmarshal(b, digest); err != nil {
		return nil, err
	}
	return []*rpb.Digest{digest}, nil
}

func (s *server) readBlob(ctx context.Context, prefix string, digest *pb.Digest, offset, length int64) (io.ReadCloser, error) {
	key := s.key(prefix, digest)
	if s.fileCache != nil {
		if r := s.fileCache.Get(key); r != nil {
			return r, nil
		}
	}
	if blob, present := s.cachedBlob(key, digest); present {
		if length > 0 {
			blob = blob[offset : offset+length]
		}
		return &countingReader{ioutil.NopCloser(bytes.NewReader(blob))}, nil
	}
	return s.readBlobUncached(ctx, key, digest, offset, length)
}

func (s *server) readBlobUncached(ctx context.Context, key string, digest *pb.Digest, offset, length int64) (io.ReadCloser, error) {
	start := time.Now()
	defer func() { readLatencies.Observe(time.Since(start).Seconds()) }()
	r, err := s.bucket.NewRangeReader(ctx, key, offset, length, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, status.Errorf(codes.NotFound, "Blob %s not found", digest.Hash)
		}
		return nil, err
	}
	return &countingReader{r: r}, nil
}

func (s *server) readAllBlob(ctx context.Context, prefix string, digest *pb.Digest) ([]byte, error) {
	key := s.key(prefix, digest)
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	if s.fileCache != nil {
		if b := s.fileCache.GetAll(key); b != nil {
			return b, nil
		}
	}
	if blob, present := s.cachedBlob(key, digest); present {
		return blob, nil
	}
	start := time.Now()
	defer func() { readDurations.Observe(time.Since(start).Seconds()) }()
	r, err := s.readBlobUncached(ctx, key, digest, 0, -1)
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

func (s *server) key(prefix string, digest *pb.Digest) string {
	return fmt.Sprintf("%s/%c%c/%s", prefix, digest.Hash[0], digest.Hash[1], digest.Hash)
}

func (s *server) labelKey(label string) string {
	return path.Join("rec", strings.Replace(strings.TrimLeft(label, "/"), ":", "/", -1))
}

func (s *server) cachedBlob(key string, digest *pb.Digest) ([]byte, bool) {
	if digest.SizeBytes < s.maxCacheItemSize {
		if blob := s.cache.Get(key); blob != nil {
			return blob, true
		}
	}
	return nil, false
}

func (s *server) writeBlob(ctx context.Context, prefix string, digest *pb.Digest, r io.Reader) error {
	key := s.key(prefix, digest)
	if prefix == "cas" && s.blobExistsUncached(ctx, key) {
		// Read and discard entire content; there is no need to update.
		// There seems to be no way for the server to signal the caller to abort in this way, so
		// this seems like the most compatible way.
		_, err := io.Copy(ioutil.Discard, r)
		return err
	}
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	start := time.Now()
	defer writeLatencies.Observe(time.Since(start).Seconds())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := s.bucket.NewWriter(ctx, key, &blob.WriterOptions{BufferSize: s.bufferSize(digest)})
	if err != nil {
		return err
	}
	defer w.Close()
	var buf bytes.Buffer
	var wc io.WriteCloser = w
	var wr io.Writer = w
	if digest.SizeBytes < s.maxCacheItemSize {
		buf.Grow(int(digest.SizeBytes))
		wr = io.MultiWriter(w, &buf)
	}
	if s.fileCache != nil {
		if w := s.fileCache.Set(ctx, key, digest.SizeBytes); w != nil {
			defer w.Close()
			wr = io.MultiWriter(wr, w)
		}
	}
	h := sha256.New()
	if prefix == "cas" { // The action cache does not have contents equivalent to their digest.
		wr = io.MultiWriter(wr, h)
	}
	n, err := io.Copy(wr, r)
	bytesReceived.Add(float64(n))
	if err != nil {
		cancel()  // ensure this happens before w.Close()
		return err
	}
	if prefix == "cas" {
		if receivedDigest := hex.EncodeToString(h.Sum(nil)); receivedDigest != digest.Hash {
			cancel()
			return fmt.Errorf("Rejecting write of %s; actual received digest was %s", digest.Hash, receivedDigest)
		}
	}
	if err := wc.Close(); err != nil {
		return err
	}
	if digest.SizeBytes < s.maxCacheItemSize {
		s.cache.Set(key, buf.Bytes())
	}
	return nil
}

func (s *server) writeMessage(ctx context.Context, prefix string, digest *pb.Digest, message proto.Message) error {
	start := time.Now()
	defer writeDurations.Observe(time.Since(start).Seconds())
	b, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return s.writeBlob(ctx, prefix, digest, bytes.NewReader(b))
}

// bytestreamBlobName returns the digest corresponding to a bytestream resource name.
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

// bufferSize returns the buffer size for a digest, or a default if it's too big.
func (s *server) bufferSize(digest *pb.Digest) int {
	if digest.SizeBytes < googleapi.DefaultUploadChunkSize {
		return int(digest.SizeBytes)
	}
	return googleapi.DefaultUploadChunkSize
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
