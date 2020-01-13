// Package main implements an execution server for the Remote Execution API.
package main

import (
	"github.com/peterebden/go-cli-init"
	"gopkg.in/op/go-logging.v1"

	"github.com/thought-machine/please-servers/metrics"
	"github.com/thought-machine/please-servers/mettle/api"
	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/mettle/worker"
)

var log = logging.MustGetLogger("mettle")

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	Queues struct {
		RequestQueue  string `short:"q" long:"request_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending requests, e.g. gcppubsub://my-request-queue"`
		ResponseQueue string `short:"r" long:"response_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending responses, e.g. gcppubsub://my-response-queue"`
	} `group:"Options controlling the pub/sub queues"`
	Storage struct {
		Storage string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
		TLS     bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	} `group:"Options controlling communication with the CAS server"`
	MetricsPort int `short:"m" long:"metrics_port" description:"Port to serve Prometheus metrics on"`
	API         struct {
		Port int `short:"p" long:"port" default:"7778" description:"Port to serve on"`
		TLS  struct {
			KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
			CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
		} `group:"Options controlling TLS for the gRPC server"`
	} `command:"api" description:"Start as an API server"`
	Worker struct {
		Dir     string `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		NoClean bool   `long:"noclean" description:"Don't clean workdirs after actions complete"`
	} `command:"worker" description:"Start as a worker"`
	Dual struct {
		Port       int    `short:"p" long:"port" default:"7778" description:"Port to serve on"`
		Dir        string `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		NoClean    bool   `long:"noclean" env:"METTLE_NO_CLEAN" description:"Don't clean workdirs after actions complete"`
		NumWorkers int    `short:"n" long:"num_workers" default:"1" env:"METTLE_NUM_WORKERS" description:"Number of workers to run in parallel"`
		TLS        struct {
			KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
			CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
		} `group:"Options controlling TLS for the gRPC server"`
	} `command:"dual" description:"Start as both API server and worker. For local testing only."`
}{
	Usage: `
Mettle is an implementation of the execution service of the Remote Execution API.
It does not implement the storage APIs itself; it must be given the location of another
server to provide that.

It can be configured in one of two modes; either as an API server or a worker. The
API server provides the gRPC API that others contact to request execution of tasks.
Meanwhile the workers perform the actual execution of tasks.

The two server types communicate via a pub/sub queue. The only configuration usefully
supported for this is using GCP Cloud Pub/Sub via a gcppubsub:// URL. The queues and
subscriptions must currently be set up manually.
For testing purposes it can be configured in a dual setup where it performs both
roles and in this setup it can be set up with an in-memory queue using a mem:// URL.
Needless to say, this mode does not synchronise with any other servers.

The specific usage of pubsub bears some note; both worker and api servers have long-running
stateful operations and hence should not be casually restarted. In the case of the worker
it will stop receiving new requests when sent a signal, but should be waited for its
current operation to complete before being terminated. Unfortunately the length of time
required is outside of our control (since timeouts on actions are set by clients) so we
cannot give a hard limit of time required.
For the master, it has no persistent storage, but all servers register to receive all
events. Hence when a server restarts it will not have knowledge of all currently running
jobs until an update is sent for each; the suggestion here is to wait for > 1 minute
(after which each live job should have sent an update, and the new server will know about
it). This is easy to arrange in a managed environment like Kubernetes.
`,
}

func main() {
	cmd := cli.ParseFlagsOrDie("Mettle", &opts)
	cli.InitFileLogging(opts.Logging.Verbosity, opts.Logging.FileVerbosity, opts.Logging.LogFile)
	go metrics.Serve(opts.MetricsPort)
	if cmd == "dual" {
		// Must ensure the topics are created ahead of time.
		common.MustOpenTopic(opts.Queues.RequestQueue)
		common.MustOpenTopic(opts.Queues.ResponseQueue)
		for i := 0; i < opts.Dual.NumWorkers; i++ {
			go worker.RunForever(opts.Queues.RequestQueue, opts.Queues.ResponseQueue, opts.Storage.Storage, opts.Dual.Dir, !opts.Dual.NoClean, opts.Storage.TLS)
		}
		api.ServeForever(opts.Dual.Port, opts.Queues.RequestQueue, opts.Queues.ResponseQueue, opts.Storage.Storage, opts.Storage.TLS, opts.Dual.TLS.KeyFile, opts.Dual.TLS.CertFile)
	} else if cmd == "worker" {
		worker.RunForever(opts.Queues.RequestQueue, opts.Queues.ResponseQueue, opts.Storage.Storage, opts.Worker.Dir, !opts.Worker.NoClean, opts.Storage.TLS)
	} else {
		api.ServeForever(opts.API.Port, opts.Queues.RequestQueue, opts.Queues.ResponseQueue, opts.Storage.Storage, opts.Storage.TLS, opts.API.TLS.KeyFile, opts.API.TLS.CertFile)
	}
}
