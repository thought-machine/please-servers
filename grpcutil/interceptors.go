package grpcutil

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	// Register the gzip compressor on the server side.
	_ "google.golang.org/grpc/encoding/gzip"
)

// Opts is the set of common options for gRPC servers.
type Opts struct {
	Host      string `long:"host" description:"Host to listen on"`
	Port      int    `short:"p" long:"port" default:"7777" description:"Port to serve on"`
	KeyFile   string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
	CertFile  string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
	TokenFile string `long:"token_file" description:"File containing a pre-shared token that clients must provide as authentication."`
	AuthAll   bool   `long:"auth_all" description:"Require authentication on all RPCs (by default only on RPCs that mutate state)"`
}

// NewServer creates a new gRPC server with a standard set of interceptors.
// It opens the relevant port and returns a listener for it, but does not begin serving.
func NewServer(opts Opts) (net.Listener, *grpc.Server) {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port))
	if err != nil {
		log.Fatalf("Failed to listen on %s:%d: %v", opts.Host, opts.Port, err)
	}
	log.Notice("Listening on %s:%d", opts.Host, opts.Port)
	s := grpc.NewServer(OptionalTLS(opts.KeyFile, opts.CertFile,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(append([]grpc.UnaryServerInterceptor{
			LogUnaryRequests,
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(),
		}, unaryAuthInterceptor(opts)...)...)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(append([]grpc.StreamServerInterceptor{
			LogStreamRequests,
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(),
		}, streamAuthInterceptor(opts)...)...)),
		grpc.MaxRecvMsgSize(419430400), // 400MB
		grpc.MaxSendMsgSize(419430400),
	)...)
	grpc_prometheus.Register(s)
	reflection.Register(s)
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	return lis, s
}

func init() {
	grpc_prometheus.EnableHandlingTimeHistogram()
}

func unaryAuthInterceptor(opts Opts) []grpc.UnaryServerInterceptor {
	if opts.TokenFile == "" {
		return nil
	}
	contents, err := ioutil.ReadFile(opts.TokenFile)
	if err != nil {
		log.Fatalf("Failed to read token file: %s", err)
	}
	token := strings.TrimSpace(string(contents))
	return []grpc.UnaryServerInterceptor{
		func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			if err := authenticate(ctx, info.FullMethod, token, opts.AuthAll); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		},
	}
}

func streamAuthInterceptor(opts Opts) []grpc.StreamServerInterceptor {
	if opts.TokenFile == "" {
		return nil
	}
	token := mustLoadToken(opts.TokenFile)
	return []grpc.StreamServerInterceptor{
		func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := authenticate(ss.Context(), info.FullMethod, token, opts.AuthAll); err != nil {
				return err
			}
			return handler(srv, ss)
		},
	}
}

var (
	errMissingToken    = status.Errorf(codes.Unauthenticated, "No auth token provided")
	errTooManyTokens   = status.Errorf(codes.Unauthenticated, "More than one auth token provided")
	errUnauthenticated = status.Errorf(codes.Unauthenticated, "Invalid auth token")
)

var authenticatedMethods = map[string]bool{
	"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities":               false,
	"/build.bazel.remote.asset.v1.Fetch/FetchBlob":                                true,
	"/build.bazel.remote.asset.v1.Fetch/FetchDirectory":                           true,
	"/build.bazel.remote.execution.v2.ActionCache/GetActionResult":                false,
	"/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult":             true,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs":   false,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs": true,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs": false,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree":          false,
	"/build.bazel.remote.execution.v2.Execution/Execute":                          true,
	"/build.bazel.remote.execution.v2.Execution/WaitExecution":                    true,
	"/build.please.remote.purity.GC/Info":                                         false,
	"/build.please.remote.purity.GC/List":                                         false,
	"/build.please.remote.purity.GC/Delete":                                       true,
	"/google.bytestream.ByteStream/QueryWriteStatus":                              true,
	"/google.bytestream.ByteStream/Read":                                          false,
	"/google.bytestream.ByteStream/Write":                                         true,
	"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo":              false,
}

// authenticate authenticates an incoming RPC and returns an error if it's not permitted.
func authenticate(ctx context.Context, method, token string, authAll bool) error {
	if !authAll && !needsAuthentication(method) {
		return nil
	} else if md, ok := metadata.FromIncomingContext(ctx); !ok {
		return errMissingToken
	} else if hdr, present := md["authorization"]; !present {
		return errMissingToken
	} else if len(hdr) == 0 {
		return errMissingToken
	} else if len(hdr) != 1 {
		return errTooManyTokens
	} else if tok := strings.TrimPrefix(hdr[0], "Bearer "); tok != token {
		return errUnauthenticated
	}
	return nil
}

// needsAuthentication returns true if the given method needs to be authenticated.
func needsAuthentication(method string) bool {
	needsAuth, present := authenticatedMethods[method]
	if !present {
		log.Error("Unregistered method %s", method)
		return true
	}
	return needsAuth
}
