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
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	// Necessary to register providers that we'll use.
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"

	"github.com/klauspost/compress/zstd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/dgraph-io/ristretto"
	"github.com/golang/protobuf/proto"
	"github.com/peterebden/go-cli-init/v2"
	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/api/googleapi"
	bs "google.golang.org/genproto/googleapis/bytestream"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/grpcutil"
	ppb "github.com/thought-machine/please-servers/proto/purity"
)

const timeout = 2 * time.Minute

var log = cli.MustGetLogger()

// emptyHash is the sha256 hash of the empty file.
var emptyHash = digest.Empty.Hash

// uncompressed is a constant we use in a few places to indicate we're not compressing blobs.
const uncompressed = false

var bytesReceived = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "bytes_received_total",
})
var bytesServed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "bytes_served_total",
})
var streamBytesReceived = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "stream_bytes_received_total",
})
var streamBytesServed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "stream_bytes_served_total",
})
var batchBytesReceived = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "batch_bytes_received_total",
})
var batchBytesServed = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "batch_bytes_served_total",
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
var actionCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "action_cache_hits_total",
})
var actionCacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "action_cache_misses_total",
})
var dirCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "dir_cache_hits_total",
})
var dirCacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "dir_cache_misses_total",
})
var knownBlobCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "known_blob_cache_hits_total",
})
var knownBlobCacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "known_blob_cache_misses_total",
})
var blobsServed = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "blobs_served_total",
	Help:      "Number of blobs served, partitioned by batching, compressor required & used.",
}, []string{"batched", "compressor_requested", "compressor_used"})
var blobsReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "blobs_received_total",
	Help:      "Number of blobs received, partitioned by batching and compressor used.",
}, []string{"batched", "compressor_used"})

func init() {
	prometheus.MustRegister(bytesReceived)
	prometheus.MustRegister(bytesServed)
	prometheus.MustRegister(streamBytesReceived)
	prometheus.MustRegister(streamBytesServed)
	prometheus.MustRegister(batchBytesReceived)
	prometheus.MustRegister(batchBytesServed)
	prometheus.MustRegister(readLatencies)
	prometheus.MustRegister(writeLatencies)
	prometheus.MustRegister(readDurations)
	prometheus.MustRegister(writeDurations)
	prometheus.MustRegister(actionCacheHits)
	prometheus.MustRegister(actionCacheMisses)
	prometheus.MustRegister(dirCacheHits)
	prometheus.MustRegister(dirCacheMisses)
	prometheus.MustRegister(knownBlobCacheHits)
	prometheus.MustRegister(knownBlobCacheMisses)
	prometheus.MustRegister(blobsServed)
	prometheus.MustRegister(blobsReceived)
}

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, storage string, parallelism int, maxDirCacheSize, maxKnownBlobCacheSize int64) {
	dec, _ := zstd.NewReader(nil)
	enc, _ := zstd.NewWriter(nil)
	srv := &server{
		bytestreamRe:  regexp.MustCompile("(?:uploads/[0-9a-f-]+/)?(blobs|compressed-blobs/zstd)/([0-9a-f]+)/([0-9]+)"),
		storageRoot:   strings.TrimPrefix(strings.TrimPrefix(storage, "file://"), "gzfile://"),
		isFileStorage: strings.HasPrefix(storage, "file://"),
		bucket:        mustOpenStorage(storage),
		limiter:       make(chan struct{}, parallelism),
		dirCache:       mustCache(maxDirCacheSize),
		knownBlobCache: mustCache(maxKnownBlobCacheSize),
		compressor:     enc,
		decompressor:   dec,
	}
	lis, s := grpcutil.NewServer(opts)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterActionCacheServer(s, srv)
	pb.RegisterContentAddressableStorageServer(s, srv)
	bs.RegisterByteStreamServer(s, srv)
	ppb.RegisterGCServer(s, srv)
	grpcutil.ServeForever(lis, s)
}

func mustCache(size int64) *ristretto.Cache {
	if size == 0 {
		return nil
	}
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: size / 10, // bit of a guess
		MaxCost:     size,
		BufferItems: 64, // recommended by upstream
	})
	if err != nil {
		log.Fatalf("Failed to construct cache: %s", err)
	}
	return cache
}

type server struct {
	storageRoot              string
	isFileStorage            bool
	bucket                   bucket
	bytestreamRe             *regexp.Regexp
	limiter                  chan struct{}
	maxCacheItemSize         int64
	dirCache, knownBlobCache *ristretto.Cache
	compressor               *zstd.Encoder
	decompressor             *zstd.Decoder
}

func (s *server) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunction: []pb.DigestFunction_Value{
				pb.DigestFunction_SHA1,
				pb.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: false,
			},
			MaxBatchTotalSizeBytes: 4048000, // 4000 Kelly-Bootle standard units
			SupportedCompressor: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 1},
	}, nil
}

func (s *server) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ar := &pb.ActionResult{}
	if err := s.readBlobIntoMessage(ctx, "ac", req.ActionDigest, ar); err != nil {
		actionCacheMisses.Inc()
		return nil, err
	}
	actionCacheHits.Inc()
	if req.InlineStdout && ar.StdoutDigest != nil {
		// The docs say that the server MAY omit inlining, even if asked. Hence we assume that if we can't find it here
		// we might be in a sharded setup where we don't have the stdout digest, and it's better to return what we can
		// (the client can still request the actual blob themselves).
		if b, err := s.readAllBlob(ctx, "cas", ar.StdoutDigest, false, false); err == nil {
			ar.StdoutRaw = b
		}
	}
	if s.isFileStorage && req.InstanceName != "purity-gc" {
		now := time.Now()
		if err := os.Chtimes(path.Join(s.storageRoot, s.key("ac", req.ActionDigest)), now, now); err != nil {
			log.Warning("Failed to change times on file: %s", err)
		}
	}
	return ar, nil
}

func (s *server) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	if ar, err := s.GetActionResult(ctx, &pb.GetActionResultRequest{
		InstanceName: req.InstanceName,
		ActionDigest: req.ActionDigest,
	}); err == nil {
		log.Debug("Returning existing action result for UpdateActionResult request for %s", req.ActionDigest.Hash)
		return ar, nil
	}
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
		} else if s.isEmpty(d) {
			wg.Done()
			continue // Ignore the empty blob.
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

// blobExists returns true if this blob exists in the underlying storage.
func (s *server) blobExists(ctx context.Context, key string) bool {
	if s.knownBlobCache != nil {
		if _, present := s.knownBlobCache.Get(key); present {
			knownBlobCacheHits.Inc()
			return true
		}
		knownBlobCacheMisses.Inc()
	}
	// N.B. if the blob is not known in the cache we still have to check, since something
	//      else could have written it when we weren't looking.
	if !s.blobExistsUncached(ctx, key) {
		return false
	}
	s.markKnownBlob(key)
	return true
}

func (s *server) blobExistsUncached(ctx context.Context, key string) bool {
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	exists, _ := s.bucket.Exists(ctx, key)
	return exists
}

// markKnownBlob marks a blob as one we know exists.
func (s *server) markKnownBlob(key string) {
	if s.knownBlobCache != nil {
		s.knownBlobCache.Set(key, nil, int64(len(key)))
	}
}

// isEmpty returns true if this digest is for the empty blob.
func (s *server) isEmpty(digest *pb.Digest) bool {
	return digest.SizeBytes == 0 && digest.Hash == emptyHash
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
			if s.isEmpty(r.Digest) {
				log.Debug("Ignoring empty blob in BatchUpdateBlobs")
			} else if len(r.Data) != int(r.Digest.SizeBytes) && r.Compressor == pb.Compressor_IDENTITY {
				rr.Status.Code = int32(codes.InvalidArgument)
				rr.Status.Message = fmt.Sprintf("Blob sizes do not match (%d / %d)", len(r.Data), r.Digest.SizeBytes)
			} else if s.blobExists(ctx, s.key("cas", r.Digest)) {
				log.Debug("Blob %s already exists remotely", r.Digest.Hash)
			} else if err := s.writeBlob(ctx, "cas", r.Digest, bytes.NewReader(r.Data), r.Compressor == pb.Compressor_ZSTD); err != nil {
				rr.Status.Code = int32(status.Code(err))
				rr.Status.Message = err.Error()
				blobsReceived.WithLabelValues(batchLabel(true), compressorLabel(r.Compressor == pb.Compressor_ZSTD)).Inc()
			} else {
				log.Debug("Stored blob with digest %s", r.Digest.Hash)
			}
			wg.Done()
			batchBytesReceived.Add(float64(r.Digest.SizeBytes))
		}(i, r)
	}
	wg.Wait()
	return resp, nil
}

func (s *server) BatchReadBlobs(ctx context.Context, req *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	n := len(req.Digests)
	m := n + len(req.Requests)
	resp := &pb.BatchReadBlobsResponse{
		Responses: make([]*pb.BatchReadBlobsResponse_Response, m),
	}
	var wg sync.WaitGroup
	var size int64
	wg.Add(m)
	for i, d := range req.Digests {
		size += d.SizeBytes
		go func(i int, d *pb.Digest) {
			resp.Responses[i] = s.batchReadBlob(ctx, &pb.BatchReadBlobsRequest_Request{Digest: d})
			wg.Done()
		}(i, d)
	}
	for i, r := range req.Requests {
		size += r.Digest.SizeBytes
		go func(i int, r *pb.BatchReadBlobsRequest_Request) {
			resp.Responses[n + i] = s.batchReadBlob(ctx, r)
			wg.Done()
		}(i, r)
	}
	wg.Wait()
	log.Debug("Served BatchReadBlobs request of %d blobs, total %d bytes in %s", len(req.Digests), size, time.Since(start))
	return resp, nil
}

func (s *server) batchReadBlob(ctx context.Context, req *pb.BatchReadBlobsRequest_Request) *pb.BatchReadBlobsResponse_Response {
	r := &pb.BatchReadBlobsResponse_Response{
		Status: &rpcstatus.Status{},
		Digest: req.Digest,
	}
	if data, err := s.readAllBlob(ctx, "cas", req.Digest, true, req.Compressor == pb.Compressor_ZSTD); err != nil {
		r.Status.Code = int32(status.Code(err))
		r.Status.Message = err.Error()
	} else {
		r.Data = data
		r.Compressor = req.Compressor
		batchBytesServed.Add(float64(req.Digest.SizeBytes))
	}
	return r
}

func (s *server) GetTree(req *pb.GetTreeRequest, srv pb.ContentAddressableStorage_GetTreeServer) error {
	if req.PageSize > 0 {
		return status.Errorf(codes.Unimplemented, "page_size not implemented for GetTree")
	} else if req.PageToken != "" {
		return status.Errorf(codes.Unimplemented, "page tokens not implemented for GetTree")
	} else if req.RootDigest == nil {
		return status.Errorf(codes.InvalidArgument, "missing root_digest field")
	}
	dirs, err := s.getTree(req.RootDigest)
	if err != nil {
		return err
	}
	return srv.Send(&pb.GetTreeResponse{Directories: dirs})
}

func (s *server) Read(req *bs.ReadRequest, srv bs.ByteStream_ReadServer) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(srv.Context(), timeout)
	defer cancel()
	defer func() { readDurations.Observe(time.Since(start).Seconds()) }()
	digest, compressed, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	if req.ReadOffset < 0 || req.ReadOffset > digest.SizeBytes {
		return status.Errorf(codes.OutOfRange, "Invalid Read() request; offset %d is outside the range of blob %s which is %d bytes long", req.ReadOffset, digest.Hash, digest.SizeBytes)
	} else if req.ReadLimit == 0 {
		req.ReadLimit = -1
		streamBytesServed.Add(float64(digest.SizeBytes - req.ReadOffset))
	} else if req.ReadOffset+req.ReadLimit > digest.SizeBytes {
		req.ReadLimit = digest.SizeBytes - req.ReadOffset
		streamBytesServed.Add(float64(req.ReadLimit))
	}
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	r, needCompression, err := s.readCompressed(ctx, "cas", digest, false, compressed, req.ReadOffset, req.ReadLimit)
	if err != nil {
		return err
	}
	defer r.Close()
	var w io.Writer = &bytestreamWriter{stream: srv}
	if needCompression {
		zw, err := zstd.NewWriter(w)
		if err != nil {
			return err
		}
		defer zw.Close()
		w = zw
	}
	_, err = io.Copy(w, r)
	if err == nil {
		log.Debug("Completed ByteStream.Read request of %d bytes in %s", digest.SizeBytes, time.Since(start))
	}
	return err
}

func (s *server) readCompressed(ctx context.Context, prefix string, digest *pb.Digest, batched, compressed bool, offset, limit int64) (io.ReadCloser, bool, error) {
	if prefix != "cas" {
		if compressed {
			return nil, false, fmt.Errorf("Attempted to do a compressed read for non-CAS prefix %s", prefix)  // This is a programming error and shouldn't happen.
		}
		r, err := s.readBlob(ctx, s.key(prefix, digest), offset, limit)
		return r, false, err
	}
	r, err := s.readBlob(ctx, s.compressedKey(prefix, digest, compressed), offset, limit)
	if err != nil {
		if r, err := s.readBlob(ctx, s.compressedKey(prefix, digest, !compressed), offset, limit); err == nil {
			blobsServed.WithLabelValues(batchLabel(batched), compressorLabel(compressed), compressorLabel(!compressed)).Inc()
			return compressedReader(r, compressed, !compressed)
		}
		return nil, false, err
	}
	blobsServed.WithLabelValues(batchLabel(batched), compressorLabel(compressed), compressorLabel(compressed)).Inc()
	return compressedReader(r, compressed, compressed)
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
	digest, compressed, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	r := &bytestreamReader{stream: srv, buf: req.Data}
	if err := s.writeBlob(ctx, "cas", digest, r, compressed); err != nil {
		return err
	}
	streamBytesReceived.Add(float64(r.TotalSize))
	log.Debug("Stored blob with hash %s", digest.Hash)
	blobsReceived.WithLabelValues(batchLabel(false), compressorLabel(compressed)).Inc()
	return srv.SendAndClose(&bs.WriteResponse{
		CommittedSize: r.TotalSize,
	})
}

func (s *server) QueryWriteStatus(ctx context.Context, req *bs.QueryWriteStatusRequest) (*bs.QueryWriteStatusResponse, error) {
	// We don't track partial writes or allow resuming them. Might add later if plz gains
	// the ability to do this as a client.
	return nil, status.Errorf(codes.NotFound, "write %s not found", req.ResourceName)
}

func (s *server) readBlob(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if length == 0 || strings.Contains(key, digest.Empty.Hash) {
		// Special case any empty read request
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}
	start := time.Now()
	defer func() { readLatencies.Observe(time.Since(start).Seconds()) }()
	r, err := s.bucket.NewRangeReader(ctx, key, offset, length, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, status.Errorf(codes.NotFound, "Blob %s not found", key)
		}
		return nil, err
	}
	s.markKnownBlob(key)
	return &countingReader{r: r}, nil
}

func (s *server) readAllBlob(ctx context.Context, prefix string, digest *pb.Digest, batched, compressed bool) ([]byte, error) {
	if len(digest.Hash) < 2 {
		return nil, fmt.Errorf("Invalid hash: [%s]", digest.Hash)
	} else if s.isEmpty(digest) {
		return nil, nil
	}
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	start := time.Now()
	defer func() { readDurations.Observe(time.Since(start).Seconds()) }()
	r, needCompression, err := s.readCompressed(ctx, prefix, digest, batched, compressed, 0, -1)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	if err != nil || !needCompression {
		return data, err
	}
	return s.compressor.EncodeAll(data, make([]byte, 0, digest.SizeBytes)), nil
}

func (s *server) readBlobIntoMessage(ctx context.Context, prefix string, digest *pb.Digest, message proto.Message) error {
	if b, err := s.readAllBlob(ctx, prefix, digest, false, false); err != nil {
		return err
	} else if err := proto.Unmarshal(b, message); err != nil {
		return status.Errorf(codes.Unknown, "%s", err)
	}
	return nil
}

// key returns the key for storing a blob. It's always uncompressed.
func (s *server) key(prefix string, digest *pb.Digest) string {
	return fmt.Sprintf("%s/%c%c/%s", prefix, digest.Hash[0], digest.Hash[1], digest.Hash)
}

// compressedKey returns a key for storing a blob, optionally compressed.
func (s *server) compressedKey(prefix string, digest *pb.Digest, compressed bool) string {
	if compressed {
		return s.key("zstd_"+prefix, digest)
	}
	return s.key(prefix, digest)
}

func (s *server) labelKey(label string) string {
	return path.Join("rec", strings.Replace(strings.TrimLeft(label, "/"), ":", "/", -1))
}

func (s *server) writeBlob(ctx context.Context, prefix string, digest *pb.Digest, r io.Reader, compressed bool) error {
	key := s.key(prefix, digest)
	if s.isEmpty(digest) || (prefix == "cas" && s.blobExists(ctx, key)) {
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
	var wc io.WriteCloser = w
	var wr io.Writer = w
	if prefix == "cas" { // The action cache does not have contents equivalent to their digest.
		if compressed {
			zr, err := zstd.NewReader(r)
			if err != nil {
				return err
			}
			defer zr.Close()
			r = newVerifyingReader(zr, digest.Hash)
		} else {
			r = newVerifyingReader(r, digest.Hash)
		}
	}
	n, err := io.Copy(wr, r)
	bytesReceived.Add(float64(n))
	if err != nil {
		cancel() // ensure this happens before w.Close()
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	s.markKnownBlob(key)
	return nil
}

func (s *server) writeMessage(ctx context.Context, prefix string, digest *pb.Digest, message proto.Message) error {
	start := time.Now()
	defer writeDurations.Observe(time.Since(start).Seconds())
	b, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return s.writeBlob(ctx, prefix, digest, bytes.NewReader(b), uncompressed)
}

// bytestreamBlobName returns the digest corresponding to a bytestream resource name
// and whether or not it is compressed (the digest is always uncompressed).
func (s *server) bytestreamBlobName(bytestream string) (*pb.Digest, bool, error) {
	matches := s.bytestreamRe.FindStringSubmatch(bytestream)
	if matches == nil {
		return nil, false, status.Errorf(codes.InvalidArgument, "invalid ResourceName: %s", bytestream)
	}
	size, _ := strconv.Atoi(matches[3])
	return &pb.Digest{
		Hash:      matches[2],
		SizeBytes: int64(size),
	}, matches[1] == "compressed-blobs/zstd", nil
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

// A bytestreamWriter wraps an outgoing byte stream into an io.Writer
type bytestreamWriter struct {
	stream bs.ByteStream_ReadServer
}

func (r *bytestreamWriter) Write(buf []byte) (int, error) {
	if err := r.stream.Send(&bs.ReadResponse{Data: buf}); err != nil {
		return 0, err
	}
	return len(buf), nil
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

// compressorLabel returns a label to use for Prometheus metrics to represent compression.
func compressorLabel(compressed bool) string {
	if compressed {
		return "zstd"
	}
	return "identity"
}

// batchLabel returns a label to use for Prometheus metrics to represent batching.
func batchLabel(batched bool) string {
	if batched {
		return "batched"
	}
	return "single"
}
