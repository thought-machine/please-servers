// Package main implements a CAS storage server for the Remote Execution API.
package main

import (
	"github.com/peterebden/go-cli-init"
	"gopkg.in/op/go-logging.v1"

	"github.com/thought-machine/please-servers/elan/rpc"
	"github.com/thought-machine/please-servers/metrics"
)

var log = logging.MustGetLogger("elan")

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	Port        int    `short:"p" long:"port" default:"7777" description:"Port to serve on"`
	Storage     string `short:"s" long:"storage" required:"true" description:"URL defining where to store data, eg. gs://bucket-name."`
	MetricsPort int    `short:"m" long:"metrics_port" description:"Port to serve Prometheus metrics on"`
	TLS         struct {
		KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
		CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
	} `group:"Options controlling TLS for the gRPC server"`
}{
	Usage: `
Elan is an implementation of the content-addressable storage and action cache services
of the Remote Execution API.

It is fairly simple and assumes that it will be backed by a distributed reliable storage
system. Currently the only production-ready backend that is supported is GCS.
Optionally it can be configured to use local file storage (or in-memory if you enjoy
living dangerously) but will not do any sharding, replication or cleanup - these
modes are intended for testing only.
`,
}

func main() {
	cli.ParseFlagsOrDie("Elan", &opts)
	cli.InitFileLogging(opts.Logging.Verbosity, opts.Logging.FileVerbosity, opts.Logging.LogFile)
	go metrics.Serve(opts.MetricsPort)
	log.Notice("Serving on :%d", opts.Port)
	rpc.ServeForever(opts.Port, opts.Storage, opts.TLS.KeyFile, opts.TLS.CertFile)
}
