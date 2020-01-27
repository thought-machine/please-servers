// Package rpc implements the gRPC server for Zeal.
// This implements only the FetchBlob RPC.
package rpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	rpb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/peterebden/go-sri"
	"github.com/prometheus/client_golang/prometheus"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/op/go-logging.v1"

	"github.com/thought-machine/please-servers/creds"
)

var log = logging.MustGetLogger("rpc")

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
	grpc_prometheus.EnableHandlingTimeHistogram()
}

// ServeForever serves on the given port until terminated.
func ServeForever(port int, keyFile, certFile, storage string, secureStorage bool) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}
	client, err := client.NewClient(context.Background(), "mettle", client.DialParams{
		Service:            storage,
		NoSecurity:         !secureStorage,
		TransportCredsOnly: secureStorage,
		DialOpts:           []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(419430400))},
	})
	if err != nil {
		log.Fatalf("Failed to connect to storage backend: %s", err)
	}
	srv := &server{
		client:        retryablehttp.NewClient(),
		storageClient: client,
	}
	srv.client.HTTPClient.Timeout = 5 * time.Minute // Always put some kind of limit on
	srv.client.RequestLogHook = srv.logHTTPRequests
	s := grpc.NewServer(creds.OptionalTLS(keyFile, certFile,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(),
		)),
		grpc.MaxRecvMsgSize(419430400), // 400MB
		grpc.MaxSendMsgSize(419430400),
	)...)
	pb.RegisterFetchServer(s, srv)
	grpc_prometheus.Register(s)
	err = s.Serve(lis)
	log.Fatalf("%s", err)
}

type server struct {
	client        *retryablehttp.Client
	storageClient *client.Client
}

func (s *server) FetchDirectory(ctx context.Context, req *pb.FetchDirectoryRequest) (*pb.FetchDirectoryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "The FetchDirectory RPC is not implemented by this server")
}

func (s *server) FetchBlob(ctx context.Context, req *pb.FetchBlobRequest) (*pb.FetchBlobResponse, error) {
	sri, err := sri.NewChecker(s.sriQualifier(req.Qualifiers))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid sri.checksum qualifier: %s", err)
	}
	var me error
	for _, u := range req.Uris {
		digest, err := s.fetchOne(ctx, u, req.Timeout, sri)
		if err == nil {
			return &pb.FetchBlobResponse{
				Status:     &rpcstatus.Status{},
				BlobDigest: digest,
			}, nil
		}
		me = multierror.Append(me, err)
	}
	// TODO(peterebden): Really we should convert this into the Status field, but it's fiddly and we don't do much
	//                   with it on the client side anyway.
	return nil, me
}

// fetchOne makes a single HTTP request. It checks against the given subresource integrity constraints
// to verify the content is as expected (note that any one matching is sufficient, not all are required tO)
func (s *server) fetchOne(ctx context.Context, url string, timeout *duration.Duration, sri *sri.Checker) (*rpb.Digest, error) {
	if d, err := ptypes.Duration(timeout); err != nil {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return s.fetchURL(ctx, url, sri)
	}
	return s.fetchURL(ctx, url, sri)
}

func (s *server) fetchURL(ctx context.Context, url string, sri *sri.Checker) (*rpb.Digest, error) {
	start := time.Now()
	req, err := retryablehttp.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("Error making request: %s", err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	digest, err := s.storageClient.WriteBlob(ctx, buf.Bytes())
	return digest.ToProto(), err
}

func (s *server) logHTTPRequests(logger retryablehttp.Logger, req *http.Request, n int) {
	if n == 0 {
		log.Notice("Making HTTP request to %s", req.URL)
	} else {
		log.Warning("Retrying HTTP request to %s (%d of %d)", req.URL, n+1, s.client.RetryMax)
	}
}

// sriQualifier returns the checksum.sri qualifier from a request.
func (s *server) sriQualifier(qualifiers []*pb.Qualifier) string {
	for _, q := range qualifiers {
		if q.Name == "checksum.sri" {
			return q.Value
		}
	}
	return ""
}
