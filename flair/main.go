// Package main implements a proxy server for the Remote Execution API.
package main

import (
	"github.com/peterebden/go-cli-init"
	"github.com/thought-machine/http-admin"

	"github.com/thought-machine/please-servers/flair/rpc"
	"github.com/thought-machine/please-servers/flair/trie"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	Port     int               `short:"p" long:"port" default:"7775" description:"Port to serve on"`
	Geometry map[string]string `short:"g" long:"geometry" required:"true" description:"CAS server geometry to forward requests to (e.g. 0-f:127.0.0.1:443"`
	Replicas int               `short:"r" long:"replicas" default:"1" description:"Number of servers to replicate reads/writes to"`
	TLS      struct {
		KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
		CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
	} `group:"Options controlling TLS for the gRPC server"`
	Admin admin.Opts `group:"Options controlling HTTP admin server" namespace:"admin"`
}{
	Usage: `
Flair is a proxy server used to forward requests to Elan.

The main usage for it is to load-balance requests to individual CAS servers, where each one
is responsible for a range of the hash space. This is intended for a setup with a number of
them that each handle a subset of storage. The hash space setup is configured statically on
this server so Elan remains unaware of it; this implies that online rebalancing etc is not
possible at present.

Currently it does not support replication but we may add that later on.

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
	var t trie.Trie
	if err := t.AddAll(opts.Geometry); err != nil {
		log.Fatalf("Failed to create trie for provided geometry: %s", err)
	} else if err := t.Check(); err != nil {
		log.Fatalf("Failed to construct trie: %s", err)
	}
	r := trie.NewReplicator(&t, opts.Replicas)
	rpc.ServeForever(opts.Port, r, opts.TLS.KeyFile, opts.TLS.CertFile)
}
