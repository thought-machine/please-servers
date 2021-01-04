// Package main implements an server for the Remote Asset API.
package main

import (
	flags "github.com/thought-machine/please-servers/cli"
	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/zeal/rpc"
)

var opts = struct {
	Usage       string
	Logging     flags.LoggingOpts `group:"Options controlling logging output"`
	GRPC        grpcutil.Opts     `group:"Options controlling the gRPC server"`
	Parallelism int               `long:"parallelism" default:"4" description:"Max parallel download tasks to run"`
	Storage     struct {
		Storage string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
		TLS     bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	} `group:"Options controlling communication with the CAS server"`
	Admin flags.AdminOpts `group:"Options controlling HTTP admin server" namespace:"admin"`
}{
	Usage: `
Zeal is a partial implementation of the Remote Asset API.
It supports only the FetchBlob RPC of the Fetch service; FetchDirectory and the Push service
are not implemented.

The only qualifier that is reliably supported is checksum.sri for hash verification. It does
not understand any communication protocol other than HTTP(S); we may add Git support in future.
SHA256 (preferred) and SHA1 (for compatibility) are the only supported hash functions.
Requests without SRI attached will be rejected with extreme prejudice.

It must communicate with a CAS server to store its eventual blobs.

Requests are downloaded entirely into memory before being uploaded, so the user should ensure there is
enough memory available for any likely request.

It is partly named to match the ongoing theme of "qualities a person can have", and partly
for the Paladin skill in Diablo II since its job is to bang things down as fast as possible.
`,
}

func main() {
	flags.ParseFlagsOrDie("Zeal", &opts)
	rpc.ServeForever(opts.GRPC, opts.Storage.Storage, opts.Storage.TLS, opts.Parallelism)
}
