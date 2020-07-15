package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
)

// Dial is a convenience function wrapping up some common gRPC functionality.
// If the URL is prefixed by a protocol (grpc:// or grpcs://) that overrides the TLS flag.
func Dial(address string, tls bool, caFile, tokenFile string) (*grpc.ClientConn, error) {
	address, tls = parseAddress(address, tls)
	return grpc.Dial(address, append(DialOptions(tokenFile), tlsOpt(tls, caFile))...)
}

// DialOptions returns some common dial options.
func DialOptions(tokenFile string) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(419430400)),
		grpc.WithChainUnaryInterceptor(unaryCompressionInterceptor),
		grpc.WithChainStreamInterceptor(streamCompressionInterceptor),
	}
	if tokenFile == "" {
		return opts
	}
	return append(opts, grpc.WithPerRPCCredentials(tokenCredProvider{
		"authorization": "Bearer " + mustLoadToken(tokenFile),
	}))
}

func tlsOpt(useTLS bool, caFile string) grpc.DialOption {
	if useTLS {
		if caFile != "" {
			return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
				RootCAs: mustLoadCACert(caFile),
			}))
		}
		return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, ""))
	}
	return grpc.WithInsecure()
}

// parseAddress parses a URL with an optional prefix like grpcs:// and returns the URL without it.
func parseAddress(url string, tlsDefault bool) (string, bool) {
	if strings.HasPrefix(url, "grpcs://") {
		return strings.TrimPrefix(url, "grpcs://"), true
	} else if strings.HasPrefix(url, "grpc://") {
		return strings.TrimPrefix(url, "grpc://"), false
	}
	return url, tlsDefault
}

// mustLoadCACert loads a CA cert from a file and dies on any errors.
func mustLoadCACert(filename string) *x509.CertPool {
	ca, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read CA cert from %s: %s", filename, err)
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(ca) {
		log.Fatalf("Failed to append CA cert to pool (invalid PEM file?)")
	}
	return cp
}

func mustLoadToken(tokenFile string) string {
	contents, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		log.Fatalf("Failed to read token file: %s", err)
	}
	return strings.TrimSpace(string(contents))
}

type tokenCredProvider map[string]string

func (cred tokenCredProvider) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return cred, nil
}

func (cred tokenCredProvider) RequireTransportSecurity() bool {
	return false // Allow these to be provided over an insecure channel; this facilitates e.g. service meshes like Istio.
}

// unaryCompressionInterceptor compresses all outgoing RPCs unless it's been skipped via SkipCompression.
func unaryCompressionInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(ctx, method, req, reply, cc, compressionInterceptor(ctx, opts)...)
}

// unaryCompressionInterceptor compresses all outgoing RPCs unless it's been skipped via SkipCompression.
func streamCompressionInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(ctx, desc, cc, method, compressionInterceptor(ctx, opts)...)
}

func compressionInterceptor(ctx context.Context, opts []grpc.CallOption) []grpc.CallOption {
	if ShouldCompress(ctx) {
		return append(opts, grpc.UseCompressor(gzip.Name))
	}
	return opts
}

// skipCompressionKey is a metadata key that can be set by the client indicating that the server-side
// should skip compression of further RPCs because it believes the contents are incompressible.
const skipCompressionKey = "mettle-skip-compression"

// ShouldCompress returns true if an incoming context does not indicate that we shouldn't
// compress the stream (i.e. the default is to compress it)
func ShouldCompress(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return !ok || len(md.Get(skipCompressionKey)) == 0
}

// SkipCompression returns a context that indicates that we should not compress this request.
func SkipCompression(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, skipCompressionKey, "true")
}

// GCKey is a metadata key that we use to identify GC requests which don't extend the
// lifetime of an action result.
const GCKey = "purity-gc"
