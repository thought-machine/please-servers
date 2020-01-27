// Package main implements an server for the Remote Asset API.
package main

import (
	"github.com/peterebden/go-cli-init"
	"gopkg.in/op/go-logging.v1"

	"github.com/thought-machine/please-servers/metrics"
	"github.com/thought-machine/please-servers/zeal/rpc"
)

var log = logging.MustGetLogger("zeal")

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	Port        int `short:"p" long:"port" default:"7776" description:"Port to serve on"`
	MetricsPort int `short:"m" long:"metrics_port" description:"Port to serve Prometheus metrics on"`
	Storage     struct {
		Storage string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
		TLS     bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	} `group:"Options controlling communication with the CAS server"`
	TLS struct {
		KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
		CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
	} `group:"Options controlling TLS for the gRPC server"`
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
	cli.ParseFlagsOrDie("Zeal", &opts)
	cli.InitFileLogging(opts.Logging.Verbosity, opts.Logging.FileVerbosity, opts.Logging.LogFile)
	go metrics.Serve(opts.MetricsPort)
	rpc.ServeForever(opts.Port, opts.TLS.KeyFile, opts.TLS.CertFile, opts.Storage.Storage, opts.Storage.TLS)
}
