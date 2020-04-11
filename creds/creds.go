// Package creds implements some common functionality for loading TLS credentials.
package creds

import (
	"github.com/peterebden/go-cli-init"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"gopkg.in/op/go-logging.v1"
)

var log = cli.MustGetLogger()

// OptionalTLS loads TLS certificates from the given files and attaches them as a gRPC ServerOption.
// If both filenames are empty then no credentials will be attached.
func OptionalTLS(keyFile, certFile string, opts ...grpc.ServerOption) []grpc.ServerOption {
	if keyFile == "" && certFile == "" {
		log.Warning("No transport security attached, will communicate in plaintext")
		return opts
	}
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load TLS credentials: %s", err)
	}
	return append(opts, grpc.Creds(creds))
}

// grpcLogMabob is an implementation of grpc's logging interface using our backend.
type grpcLogMabob struct{}

func (g *grpcLogMabob) Info(args ...interface{})                    { log.Info("%s", args) }
func (g *grpcLogMabob) Infof(format string, args ...interface{})    { log.Info(format, args...) }
func (g *grpcLogMabob) Infoln(args ...interface{})                  { log.Info("%s", args) }
func (g *grpcLogMabob) Warning(args ...interface{})                 { log.Warning("%s", args) }
func (g *grpcLogMabob) Warningf(format string, args ...interface{}) { log.Warning(format, args...) }
func (g *grpcLogMabob) Warningln(args ...interface{}) {
	// Lower priority of a gRPC message which is triggered by e.g. k8s TCP healthchecks.
	if len(args) == 2 && args[0] == "grpc: Server.Serve failed to create ServerTransport: " {
		if err, ok := args[1].(error); ok {
			if err.Error() == `connection error: desc = "transport: http2Server.HandleStreams failed to receive the preface from client: EOF"` {
				log.Debug("%s %s", args[0], args[1])
				return
			}
		}
	}
	log.Warning("%s", args)
}
func (g *grpcLogMabob) Error(args ...interface{})                 { log.Error("%s", args) }
func (g *grpcLogMabob) Errorf(format string, args ...interface{}) { log.Errorf(format, args...) }
func (g *grpcLogMabob) Errorln(args ...interface{})               { log.Error("%s", args) }
func (g *grpcLogMabob) Fatal(args ...interface{})                 { log.Fatal(args...) }
func (g *grpcLogMabob) Fatalf(format string, args ...interface{}) { log.Fatalf(format, args...) }
func (g *grpcLogMabob) Fatalln(args ...interface{})               { log.Fatal(args...) }
func (g *grpcLogMabob) V(l int) bool                              { return log.IsEnabledFor(logging.Level(l)) }

func init() {
	// Change grpc to log using our implementation
	grpclog.SetLoggerV2(&grpcLogMabob{})
}
