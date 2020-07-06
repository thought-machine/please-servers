// Package main implements a proxy server for the Remote Execution API.
package main

import (
	"github.com/peterebden/go-cli-init"
	"github.com/thought-machine/http-admin"
	"google.golang.org/grpc"

	"github.com/thought-machine/please-servers/flair/rpc"
	"github.com/thought-machine/please-servers/flair/trie"
	"github.com/thought-machine/please-servers/grpcutil"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	GRPC             grpcutil.Opts     `group:"Options controlling the gRPC server"`
	Geometry         map[string]string `short:"g" long:"geometry" required:"true" description:"CAS server geometry to forward requests to (e.g. 0-f:127.0.0.1:443"`
	AssetGeometry    map[string]string `short:"a" long:"asset_geometry" description:"Asset server geometry to forward requests to. If not given then the remote asset API will be unavailable."`
	ExecutorGeometry map[string]string `short:"e" long:"executor_geometry" description:"Executor server geometry to forward request to. If not given then the executor API will be unavailable."`
	Replicas         int               `short:"r" long:"replicas" default:"1" description:"Number of servers to replicate reads/writes to"`
	ConnTLS          bool              `long:"tls" description:"Use TLS for connecting to other servers"`
	CA               string            `long:"ca" description:"File containing PEM-formatted CA certificate to verify TLS connections with"`
	Admin            admin.Opts        `group:"Options controlling HTTP admin server" namespace:"admin"`
}{
	Usage: `
Flair is a proxy server used to forward requests to Elan.

The main usage for it is to load-balance requests to individual CAS servers, where each one
is responsible for a range of the hash space. This is intended for a setup with a number of
them that each handle a subset of storage. The hash space setup is configured statically on
this server so Elan remains unaware of it; this implies that online rebalancing etc is not
possible at present.

Flair continues the increasingly stretched naming scheme, referring to having a special
or instinctive ability for something (hopefully load balancing in this case). You definitely
want to have more than the minimum number of instances of it (hopefully more than fifteen...).
`,
}

func main() {
	cli.ParseFlagsOrDie("Flair", &opts)
	info := cli.MustInitFileLogging(opts.Logging.Verbosity, opts.Logging.FileVerbosity, opts.Logging.LogFile)
	opts.Admin.Logger = cli.MustGetLoggerNamed("github.com.thought-machine.http-admin")
	opts.Admin.LogInfo = info
	go admin.Serve(opts.Admin)
	cr := newReplicator(opts.Geometry, opts.Replicas)
	ar := newReplicator(opts.AssetGeometry, opts.Replicas)
	er := newReplicator(opts.ExecutorGeometry, opts.Replicas)
	rpc.ServeForever(opts.GRPC, cr, ar, er)
}

func newReplicator(geometry map[string]string, replicas int) *trie.Replicator {
	if len(geometry) == 0 {
		return nil
	}
	t := trie.New(func(address string) (*grpc.ClientConn, error) {
		return grpcutil.Dial(address, opts.ConnTLS, opts.CA, opts.GRPC.TokenFile)
	})
	if err := t.AddAll(geometry); err != nil {
		log.Fatalf("Failed to create trie for provided geometry: %s", err)
	} else if err := t.Check(); err != nil {
		log.Fatalf("Failed to construct trie: %s", err)
	}
	return trie.NewReplicator(t, replicas)
}
