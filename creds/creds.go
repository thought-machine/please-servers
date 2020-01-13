// Package creds implements some common functionality for loading TLS credentials.
package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/op/go-logging.v1"
)

var log = logging.MustGetLogger("creds")

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
