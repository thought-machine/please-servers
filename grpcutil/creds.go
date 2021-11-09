package grpcutil

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var tlsVersions = map[string]uint16{
	"1.0": tls.VersionTLS10,
	"1.1": tls.VersionTLS11,
	"1.2": tls.VersionTLS12,
	"1.3": tls.VersionTLS13,
}

// OptionalTLS loads TLS certificates from the given files and attaches them as a gRPC ServerOption.
// If both filenames are empty then no credentials will be attached.
func OptionalTLS(keyFile, certFile, tlsMinVersionFlag string, opts ...grpc.ServerOption) []grpc.ServerOption {
	if keyFile == "" && certFile == "" {
		log.Warning("No transport security attached, will communicate in plaintext")
		return opts
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load TLS credentials: %s", err)
	}
	var tlsMinVersion uint16
	if tlsMinVersionFlag != "" {
		var ok bool
		if tlsMinVersion, ok = tlsVersions[tlsMinVersionFlag]; !ok {
			log.Fatalf("TLS version %s unknown", tlsMinVersionFlag)
		}
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tlsMinVersion,
	})
	return append(opts, grpc.Creds(creds))
}
