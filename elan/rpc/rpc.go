// Package rpc implements the gRPC server for Elan.
// This contains implementations of the ContentAddressableStorage and ActionCache
// services, but not Execution (even by proxy).
package rpc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net"
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

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/dgraph-io/ristretto"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/go-multierror"
	"github.com/klauspost/compress/zstd"
	"github.com/peterebden/go-cli-init/v4/logging"
	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/api/googleapi"
	bs "google.golang.org/genproto/googleapis/bytestream"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/grpcutil"
	ppb "github.com/thought-machine/please-servers/proto/purity"
	"github.com/thought-machine/please-servers/rexclient"
)

const timeout = 2 * time.Minute

var log = logging.MustGetLogger()

// emptyHash is the sha256 hash of the empty file.
var emptyHash = digest.Empty.Hash

var bytesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "bytes_received_total",
}, []string{"batched", "compressed"})
var bytesServed = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "bytes_served_total",
}, []string{"batched", "compressed"})
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
var blobSizeMismatches = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "blob_size_mismatches_total",
})
var blobsServed = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "blobs_served_total",
	Help:      "Number of blobs served, partitioned by batching, compressor required & used.",
}, []string{"batched", "compressor_used", "compressor_preferred"})
var blobsReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "blobs_received_total",
	Help:      "Number of blobs received, partitioned by batching and compressor used.",
}, []string{"batched", "compressor_used"})

func init() {
	prometheus.MustRegister(bytesReceived)
	prometheus.MustRegister(bytesServed)
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
	prometheus.MustRegister(blobSizeMismatches)
	prometheus.MustRegister(blobsServed)
	prometheus.MustRegister(blobsReceived)
}

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, storage string, storageRetries, parallelism int, maxDirCacheSize, maxKnownBlobCacheSize int64) {
	lis, s := startServer(opts, storage, storageRetries, parallelism, maxDirCacheSize, maxKnownBlobCacheSize)
	grpcutil.ServeForever(lis, s)
}

func createServer(storage string, storageRetries, parallelism int, maxDirCacheSize, maxKnownBlobCacheSize int64) *server {
	dec, _ := zstd.NewReader(nil)
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	return &server{
		bytestreamRe:   regexp.MustCompile("(?:uploads/[0-9a-f-]+/)?(blobs|compressed-blobs/zstd)/([0-9a-f]+)/([0-9]+)"),
		storageRoot:    strings.TrimPrefix(storage, "file://"),
		isFileStorage:  strings.HasPrefix(storage, "file://"),
		bucket:         mustOpenStorage(storage, storageRetries),
		limiter:        make(chan struct{}, parallelism),
		dirCache:       mustCache(maxDirCacheSize),
		knownBlobCache: mustCache(maxKnownBlobCacheSize),
		compressor:     enc,
		decompressor:   dec,
	}
}

func startServer(opts grpcutil.Opts, storage string, storageRetries, parallelism int, maxDirCacheSize, maxKnownBlobCacheSize int64) (net.Listener, *grpc.Server) {
	srv := createServer(storage, storageRetries, parallelism, maxDirCacheSize, maxKnownBlobCacheSize)
	lis, s := grpcutil.NewServer(opts)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterActionCacheServer(s, srv)
	pb.RegisterContentAddressableStorageServer(s, srv)
	bs.RegisterByteStreamServer(s, srv)
	ppb.RegisterGCServer(s, srv)
	return lis, s
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
			CachePriorityCapabilities: &pb.PriorityCapabilities{
				Priorities: []*pb.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 0,
					},
					{
						MinPriority: -1,
						MaxPriority: -1,
					},
				},
			},
			MaxBatchTotalSizeBytes: 4048000, // 4000 Kelly-Bootle standard units
			SupportedCompressor: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
			BatchCompression: true,
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
	// If priority is < 0 that indicates an 'urgent' update which will overwrite the existing one
	// regardless of what is currently there.
	if req.ResultsCachePolicy == nil || req.ResultsCachePolicy.Priority >= 0 {
		if ar, err := s.GetActionResult(ctx, &pb.GetActionResultRequest{
			InstanceName: req.InstanceName,
			ActionDigest: req.ActionDigest,
		}); err == nil {
			log.Debug("Returning existing action result for UpdateActionResult request for %s", req.ActionDigest.Hash)
			return ar, nil
		}
	}
	b, err := proto.Marshal(req.ActionResult)
	if err != nil {
		return req.ActionResult, err
	}
	return req.ActionResult, s.bucket.WriteAll(ctx, s.key("ac", req.ActionDigest), b)
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
			if !s.blobExists(ctx, s.key("cas", d)) && !s.blobExists(ctx, s.compressedKey("cas", d, true)) {
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
			compressed := r.Compressor == pb.Compressor_ZSTD
			if s.isEmpty(r.Digest) {
				log.Debug("Ignoring empty blob in BatchUpdateBlobs")
			} else if len(r.Data) != int(r.Digest.SizeBytes) && !compressed {
				rr.Status.Code = int32(codes.InvalidArgument)
				rr.Status.Message = fmt.Sprintf("Blob sizes do not match (%d / %d)", len(r.Data), r.Digest.SizeBytes)
				blobSizeMismatches.Inc()
			} else if s.blobExists(ctx, s.compressedKey("cas", r.Digest, compressed)) {
				log.Debug("Blob %s already exists remotely", r.Digest.Hash)
			} else if err := s.writeAll(ctx, r.Digest, r.Data, compressed); err != nil {
				rr.Status.Code = int32(status.Code(err))
				rr.Status.Message = err.Error()
				blobsReceived.WithLabelValues(batchLabel(true, false), compressorLabel(compressed)).Inc()
			} else {
				log.Debug("Stored blob with digest %s", r.Digest.Hash)
			}
			wg.Done()
			bytesReceived.WithLabelValues(batchLabel(true, false), compressorLabel(compressed)).Add(float64(r.Digest.SizeBytes))
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
			resp.Responses[n+i] = s.batchReadBlob(ctx, r)
			wg.Done()
		}(i, r)
	}
	wg.Wait()
	log.Debug("Served BatchReadBlobs request of %d blobs, total %d bytes in %s", len(resp.Responses), size, time.Since(start))
	return resp, nil
}

func (s *server) batchReadBlob(ctx context.Context, req *pb.BatchReadBlobsRequest_Request) *pb.BatchReadBlobsResponse_Response {
	r := &pb.BatchReadBlobsResponse_Response{
		Status: &rpcstatus.Status{},
		Digest: req.Digest,
	}
	compressed := req.Compressor == pb.Compressor_ZSTD
	if data, err := s.readAllBlob(ctx, "cas", req.Digest, true, compressed); err != nil {
		r.Status.Code = int32(status.Code(err))
		r.Status.Message = err.Error()
	} else {
		r.Data = data
		r.Compressor = req.Compressor
		bytesServed.WithLabelValues(batchLabel(true, false), compressorLabel(compressed)).Add(float64(req.Digest.SizeBytes))
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
	dirs, err := s.getTree(req.RootDigest, rexclient.ShouldStopAtPack(srv.Context()))
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
	} else if req.ReadOffset == digest.SizeBytes {
		// We know there is nothing left to read, just return immediately.
		log.Debug("Completed ByteStream.Read request immediately at final byte %d of %s", digest.SizeBytes, digest.Hash)
		return nil
	} else if req.ReadLimit == 0 || req.ReadOffset+req.ReadLimit >= digest.SizeBytes {
		req.ReadLimit = -1
	}
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	r, needCompression, err := s.readCompressed(ctx, "cas", digest, compressed, req.ReadOffset, req.ReadLimit)
	if err != nil {
		return err
	}
	defer r.Close()
	bw := bufio.NewWriterSize(&bytestreamWriter{stream: srv}, 65536)
	defer bw.Flush()
	w := s.compressWriter(bw, needCompression)
	defer w.Close()
	n, err := io.Copy(w, r)
	if err != nil {
		return err
	}
	bytesServed.WithLabelValues(batchLabel(false, true), compressorLabel(compressed)).Add(float64(n))
	log.Debug("Completed ByteStream.Read request of %d bytes (starting at %d) for %s in %s", n, req.ReadOffset, digest.Hash, time.Since(start))
	return nil
}

func (s *server) readCompressed(ctx context.Context, prefix string, digest *pb.Digest, compressed bool, offset, limit int64) (io.ReadCloser, bool, error) {
	if prefix != "cas" {
		if compressed {
			return nil, false, fmt.Errorf("Attempted to do a compressed read for non-CAS prefix %s", prefix) // This is a programming error and shouldn't happen.
		}
		r, err := s.readBlob(ctx, s.key(prefix, digest), offset, limit)
		return r, false, err
	}
	if s.isEmpty(digest) {
		return ioutil.NopCloser(bytes.NewReader(nil)), compressed, nil
	}
	r, err := s.readBlob(ctx, s.compressedKey(prefix, digest, compressed), offset, limit)
	if err == nil {
		blobsServed.WithLabelValues(batchLabel(false, true), compressorLabel(compressed), compressorLabel(compressed)).Inc()
		return s.compressedReader(r, compressed, compressed, offset)
	}
	r, err2 := s.readBlob(ctx, s.compressedKey(prefix, digest, !compressed), bucketOffset(!compressed, offset), limit)
	if err2 == nil {
		blobsServed.WithLabelValues(batchLabel(false, true), compressorLabel(compressed), compressorLabel(!compressed)).Inc()
		return s.compressedReader(r, compressed, !compressed, offset)
	}
	// Bit of fiddling around to provide the most interesting error.
	if isNotFound(err2) {
		return nil, false, err
	} else if isNotFound(err) {
		return nil, false, err2
	}
	return nil, false, multierror.Append(err, err2)
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
	if err := s.writeBlob(ctx, "cas", digest, bufio.NewReaderSize(r, 65536), compressed); err != nil {
		return err
	}
	bytesReceived.WithLabelValues(batchLabel(false, true), compressorLabel(compressed)).Add(float64(r.TotalSize))
	log.Debug("Stored blob with hash %s", digest.Hash)
	blobsReceived.WithLabelValues(batchLabel(false, true), compressorLabel(compressed)).Inc()
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
		return nil, handleNotFound(err, key)
	}
	s.markKnownBlob(key)
	return r, nil
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
	if b, err := s.bucket.ReadAll(ctx, s.compressedKey(prefix, digest, compressed)); err == nil {
		blobsServed.WithLabelValues(batchLabel(batched, false), compressorLabel(compressed), compressorLabel(compressed)).Inc()
		return b, nil
	} else if prefix != "cas" {
		return nil, handleNotFound(err, digest.Hash) // No point going on, the AC is never compressed.
	}
	// If we get here, it failed, so we need to check the compressed / non-compressed version.
	b, err := s.bucket.ReadAll(ctx, s.compressedKey(prefix, digest, !compressed))
	if err != nil {
		return nil, handleNotFound(err, digest.Hash)
	}
	// We now need to either compress or decompress the payload, since we read the other version.
	buf := make([]byte, 0, digest.SizeBytes)
	if compressed {
		return s.compressor.EncodeAll(b, buf), nil
	}
	b, err = s.decompressor.DecodeAll(b, buf)
	if err != nil {
		return nil, err
	}
	blobsServed.WithLabelValues(batchLabel(batched, false), compressorLabel(compressed), compressorLabel(!compressed)).Inc()
	return b, nil
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

func (s *server) writeBlob(ctx context.Context, prefix string, digest *pb.Digest, r io.Reader, compressed bool) error {
	key := s.compressedKey(prefix, digest, compressed)
	if s.isEmpty(digest) || s.blobExists(ctx, key) {
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
	tr := io.TeeReader(r, w)
	r = tr
	h := sha256.New()
	if compressed {
		zr, err := zstd.NewReader(r)
		if err != nil {
			return err
		}
		defer zr.Close()
		r = zr
	}
	if _, err := io.Copy(h, r); err != nil {
		cancel() // ensure this happens before w.Close()
		return err
	} else if actual := hex.EncodeToString(h.Sum(nil)); actual != digest.Hash {
		cancel()
		return fmt.Errorf("Rejecting write of %s; actual received digest was %s", digest.Hash, actual)
	}
	if err := w.Close(); err != nil {
		cancel()
		return err
	}
	s.markKnownBlob(key)
	return nil
}

func (s *server) writeAll(ctx context.Context, digest *pb.Digest, data []byte, compressed bool) error {
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()
	canonical := data
	if compressed {
		decompressed, err := s.decompressor.DecodeAll(canonical, make([]byte, 0, digest.SizeBytes))
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Invalid zstd data: %s", err)
		}
		canonical = decompressed
	}
	h := sha256.Sum256(canonical)
	if actual := hex.EncodeToString(h[:]); actual != digest.Hash {
		return fmt.Errorf("Rejecting write of %s; actual received digest was %s", digest.Hash, actual)
	}
	return s.bucket.WriteAll(ctx, s.compressedKey("cas", digest, compressed), data)
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

// compressorLabel returns a label to use for Prometheus metrics to represent compression.
func compressorLabel(compressed bool) string {
	if compressed {
		return "zstd"
	}
	return "identity"
}

// batchLabel returns a label to use for Prometheus metrics to represent batching & streaming.
func batchLabel(batched, streamed bool) string {
	if streamed {
		return "streamed"
	} else if batched {
		return "batched"
	}
	return "single"
}

// handleNotFound converts an error from a gocloud error to a gRPC one for NotFound errors.
func handleNotFound(err error, key string) error {
	if isNotFound(err) {
		return status.Errorf(codes.NotFound, "Blob %s not found", key)
	}
	return err
}

// isNotFound returns true if the given error is for a blob not being found.
func isNotFound(err error) bool {
	return gcerrors.Code(err) == gcerrors.NotFound || status.Code(err) == codes.NotFound
}

// bucketOffset returns the offset we'd use into the underlying bucket (which may be zero if compressed)
func bucketOffset(compressed bool, offset int64) int64 {
	if compressed {
		return 0
	}
	return offset
}
