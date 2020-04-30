package grpcutil

import (
	"fmt"
	"net"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Opts is the set of common options for gRPC servers.
type Opts struct {
	Host        string `long:"host" description:"Host to listen on"`
	Port        int    `short:"p" long:"port" default:"7777" description:"Port to serve on"`
	KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
	CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
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
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			LogUnaryRequests,
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			LogStreamRequests,
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(),
		)),
		grpc.MaxRecvMsgSize(419430400), // 400MB
		grpc.MaxSendMsgSize(419430400),
	)...)
	grpc_prometheus.Register(s)
	reflection.Register(s)
	return lis, s
}

func init() {
	grpc_prometheus.EnableHandlingTimeHistogram()
}
