// Package rpc implements the gRPC server for Zeal.
// This implements only the FetchBlob RPC.
package rpc

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	rpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/peterebden/go-cli-init/v2"
	"github.com/peterebden/go-sri"
	"github.com/prometheus/client_golang/prometheus"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/grpcutil"
)

var log = cli.MustGetLogger()

var bytesReceived = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "zeal",
	Name:      "bytes_downloaded_total",
})
var downloadDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "zeal",
	Name:      "download_duration_seconds",
	Buckets:   prometheus.DefBuckets,
})

func init() {
	prometheus.MustRegister(bytesReceived)
	prometheus.MustRegister(downloadDurations)
}

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, storage string, secureStorage bool, parallelism int) {
	client, err := client.NewClient(context.Background(), "mettle", client.DialParams{
		Service:            storage,
		NoSecurity:         !secureStorage,
		TransportCredsOnly: secureStorage,
		DialOpts:           grpcutil.DialOptions(opts.TokenFile),
	}, client.UseBatchOps(true), client.RetryTransient())
	if err != nil {
		log.Fatalf("Failed to connect to storage backend: %s", err)
	}
	srv := &server{
		client:        retryablehttp.NewClient(),
		storageClient: client,
		limiter:       make(chan struct{}, parallelism),
	}
	srv.client.HTTPClient.Timeout = 5 * time.Minute // Always put some kind of limit on
	srv.client.RequestLogHook = srv.logHTTPRequests
	srv.client.Logger = logger{}
	lis, s := grpcutil.NewServer(opts)
	pb.RegisterFetchServer(s, srv)
	grpcutil.ServeForever(lis, s)
}

type server struct {
	client        *retryablehttp.Client
	storageClient *client.Client
	limiter       chan struct{}
	downloads     sync.Map
}

type download struct {
	once   sync.Once
	digest *rpb.Digest
	err    error
}

func (s *server) FetchDirectory(ctx context.Context, req *pb.FetchDirectoryRequest) (*pb.FetchDirectoryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "The FetchDirectory RPC is not implemented by this server")
}

func (s *server) FetchBlob(ctx context.Context, req *pb.FetchBlobRequest) (*pb.FetchBlobResponse, error) {
	if _, err := s.sriChecker(req.Qualifiers); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid sri.checksum qualifier: %s", err)
	}
	var me error
	for _, u := range req.Uris {
		digest, err := s.fetchOne(ctx, u, req.Timeout, req.Qualifiers)
		if err == nil {
			return &pb.FetchBlobResponse{
				Status:     &rpcstatus.Status{},
				BlobDigest: digest,
			}, nil
		}
		me = multierror.Append(me, fmt.Errorf("Failed fetching from %s: %s", u, err))
	}
	// TODO(peterebden): Really we should convert this into the Status field, but it's fiddly and we don't do much
	//                   with it on the client side anyway.
	return nil, me
}

// fetchOne makes a single HTTP request. It checks against the given subresource
// integrity constraints to verify the content is as expected.
func (s *server) fetchOne(ctx context.Context, url string, timeout *duration.Duration, qualifiers []*pb.Qualifier) (*rpb.Digest, error) {
	if d, err := ptypes.Duration(timeout); err != nil {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return s.singleflightFetchURL(ctx, url, qualifiers)
	}
	return s.singleflightFetchURL(ctx, url, qualifiers)
}

// singleflightFetchURL fetches a single URL, ensuring we are only fetching the same one once at a time.
func (s *server) singleflightFetchURL(ctx context.Context, url string, qualifiers []*pb.Qualifier) (*rpb.Digest, error) {
	v, _ := s.downloads.LoadOrStore(url, &download{})
	d := v.(*download)
	d.once.Do(func() {
		d.digest, d.err = s.fetchURL(ctx, url, qualifiers)
	})
	// Don't keep failed downloads around in the singleflight map, but we can remember successful ones.
	if d.err != nil {
		s.downloads.Delete(url)
		return d.digest, d.err
	}
	// Verify that the CAS still contains this resource.
	if resp, err := s.storageClient.FindMissingBlobs(ctx, &rpb.FindMissingBlobsRequest{
		InstanceName: s.storageClient.InstanceName,
		BlobDigests:  []*rpb.Digest{d.digest},
	}); err != nil || len(resp.MissingBlobDigests) > 0 {
		log.Warning("CAS does not still contain blob for %s, re-triggering download.", url)
		s.downloads.Delete(url)
		return s.singleflightFetchURL(ctx, url, qualifiers)
	}
	return d.digest, d.err
}

func (s *server) fetchURL(ctx context.Context, url string, qualifiers []*pb.Qualifier) (*rpb.Digest, error) {
	s.limiter <- struct{}{}
	defer func() { <-s.limiter }()

	// N.B. We must construct a new SRI checker each time here since it is stateful per request.
	//      We've already checked it for errors though.
	sri, _ := s.sriChecker(qualifiers)
	start := time.Now()
	req, err := retryablehttp.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("Error making request: %s", err)
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Request failed: %s", resp.Status)
	}
	var buf bytes.Buffer
	n, err := io.Copy(io.MultiWriter(&buf, sri), resp.Body)
	bytesReceived.Add(float64(n))
	downloadDurations.Observe(time.Since(start).Seconds())
	if err != nil {
		return nil, fmt.Errorf("Error reading response: %s", err)
	} else if err := sri.Check(); err != nil {
		return nil, fmt.Errorf("Invalid content received: %s", err)
	}
	blob := buf.Bytes()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	digest, err := s.storageClient.WriteCompressedBlob(ctx, blob, s.compressor(blob))
	return digest.ToProto(), err
}

// compressor returns the compressor we should use for uploading this blob.
func (s *server) compressor(blob []byte) rpb.Compressor_Value {
	mime := http.DetectContentType(blob)
	switch mime {
	case "application/x-rar-compressed", "application/x-gzip", "application/zip", "video/webm", "image/gif", "image/webp", "image/png", "image/jpeg":
		return rpb.Compressor_IDENTITY
	}
	// xz and bzip2 aren't detected by the net/http package
	if bytes.HasPrefix(blob, []byte{0xFD, '7', 'z', 'X', 'Z', 0x00}) {
		return rpb.Compressor_IDENTITY
	} else if bytes.HasPrefix(blob, []byte{0x42, 0x5A, 0x68}) {
		return rpb.Compressor_IDENTITY
	}
	return rpb.Compressor_ZSTD
}

func (s *server) logHTTPRequests(logger retryablehttp.Logger, req *http.Request, n int) {
	if n == 0 {
		log.Notice("Making HTTP request to %s", req.URL)
	} else {
		log.Warning("Retrying HTTP request to %s (%d of %d)", req.URL, n+1, s.client.RetryMax)
	}
}

// sriChecker returns a new SRI checker from the checksum.sri qualifier from a request,
// or one that always succeeds
func (s *server) sriChecker(qualifiers []*pb.Qualifier) (checker, error) {
	for _, q := range qualifiers {
		if q.Name == "checksum.sri" {
			return sri.NewCheckerForHashes(q.Value, map[string]sri.HashFunc{
				"sha256": sha256.New,
				"sha1":   newDoubleSHA1,
			})
		}
	}
	return nopChecker{}, nil
}

type checker interface {
	io.Writer
	Check() error
}

type nopChecker struct{}

func (n nopChecker) Write(b []byte) (int, error) {
	return len(b), nil
}
func (n nopChecker) Check() error {
	return nil
}

// doubleSHA1 is an implementation of the hash algorithm that "plz hash" uses for its default
// SHA1 config. Because the output of a rule is not necessarily a single file it hashes all
// files and then hashes the results of those together. Something has to be done since there is
// otherwise no clearly-defined approach to hashing more than one file, but here we have to be
// aware of it to produce the expected results.
type doubleSHA1 struct {
	h hash.Hash
}

func newDoubleSHA1() hash.Hash {
	return &doubleSHA1{h: sha1.New()}
}

func (d *doubleSHA1) Write(b []byte) (int, error) { return d.h.Write(b) }
func (d *doubleSHA1) Reset()                      { d.h.Reset() }
func (d *doubleSHA1) Size() int                   { return d.h.Size() }
func (d *doubleSHA1) BlockSize() int              { return d.h.BlockSize() }
func (d *doubleSHA1) Sum(b []byte) []byte {
	s := sha1.Sum(d.h.Sum(b))
	return s[:]
}

// A logger implements the retryablehttp.Logger interface
type logger struct{}

func (l logger) Printf(msg string, args ...interface{}) {
	log.Infof(msg, args...)
}
