// Package main implements an execution server for the Remote Execution API.
package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/peterebden/go-cli-init"
	"github.com/thought-machine/http-admin"

	"github.com/thought-machine/please-servers/mettle/api"
	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/mettle/worker"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	InstanceName string `short:"i" long:"instance_name" default:"mettle" description:"Name of this execution instance"`
	API         struct {
		Host             string `long:"host" description:"Host to listen on"`
		Port             int    `short:"p" long:"port" default:"7778" description:"Port to serve on"`
		TLS              struct {
			KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
			CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
		} `group:"Options controlling TLS for the gRPC server"`
		Queues struct {
			RequestQueue        string `short:"q" long:"request_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending requests, e.g. gcppubsub://my-request-queue"`
			ResponseQueue       string `short:"r" long:"response_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending responses, e.g. gcppubsub://my-response-queue"`
			ResponseQueueSuffix string `long:"response_queue_suffix" env:"RESPONSE_QUEUE_SUFFIX" description:"Suffix to apply to the response queue name"`
			PreResponseQueue string `long:"pre_response_queue" required:"true" description:"URL describing the pub/sub queue to connect to for preloading responses to other servers"`
		} `group:"Options controlling the pub/sub queues"`
	} `command:"api" description:"Start as an API server"`
	Worker struct {
		Dir          string       `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		CacheDir     string       `short:"c" long:"cache_dir" description:"Directory of pre-cached blobs"`
		CacheCopy    bool         `long:"cache_copy" description:"Copy blobs from cache rather than attempting to link."`
		NoClean      bool         `long:"noclean" description:"Don't clean workdirs after actions complete"`
		Name         string       `short:"n" long:"name" description:"Name of this worker"`
		Browser      string       `long:"browser" description:"Base URL for browser service (only used to construct informational user messages"`
		Sandbox      string       `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		Timeout      cli.Duration `long:"timeout" default:"3m" description:"Timeout for individual RPCs"`
		CacheMaxSize cli.ByteSize `long:"cache_max_size" default:"100M" description:"Max size of in-memory blob cache"`
		Storage      struct {
			Storage string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
			TLS     bool   `long:"tls" description:"Use TLS for communication with the storage server"`
		} `group:"Options controlling communication with the CAS server"`
		Queues struct {
			RequestQueue        string `short:"q" long:"request_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending requests, e.g. gcppubsub://my-request-queue"`
			ResponseQueue       string `short:"r" long:"response_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending responses, e.g. gcppubsub://my-response-queue"`
		} `group:"Options controlling the pub/sub queues"`
	} `command:"worker" description:"Start as a worker"`
	Dual struct {
		Port         int          `short:"p" long:"port" default:"7778" description:"Port to serve on"`
		Dir          string       `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		NoClean      bool         `long:"noclean" env:"METTLE_NO_CLEAN" description:"Don't clean workdirs after actions complete"`
		NumWorkers   int          `short:"n" long:"num_workers" default:"1" env:"METTLE_NUM_WORKERS" description:"Number of workers to run in parallel"`
		Browser      string       `long:"browser" description:"Base URL for browser service (only used to construct informational user messages"`
		Sandbox      string       `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		Timeout      cli.Duration `long:"timeout" default:"3m" description:"Timeout for individual RPCs"`
		CacheMaxSize cli.ByteSize `long:"cache_max_size" default:"100M" description:"Max size of in-memory blob cache"`
		TLS          struct {
			KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
			CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
		} `group:"Options controlling TLS for the gRPC server"`
		Storage struct {
			Storage string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
			TLS     bool   `long:"tls" description:"Use TLS for communication with the storage server"`
		} `group:"Options controlling communication with the CAS server"`
	} `command:"dual" description:"Start as both API server and worker. For local testing only."`
	Cache struct {
		Args struct {
			Targets []string `positional-arg-name:"targets" required:"true" description:"Targets to watch the sources of for changes"`
		} `positional-args:"true" required:"true"`
		Dir     string `short:"d" long:"dir" required:"true" description:"Directory to copy data into"`
		Storage      struct {
			Storage string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
			TLS     bool   `long:"tls" description:"Use TLS for communication with the storage server"`
		} `group:"Options controlling communication with the CAS server"`
	} `command:"cache" description:"Download artifacts to cache dir"`
	One struct {
		Hash         string       `long:"hash" required:"true" description:"Hash of the action digest to run"`
		Size         int64        `long:"size" required:"true" description:"Size of the action digest to run"`
		Dir          string       `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		CacheDir     string       `short:"c" long:"cache_dir" description:"Directory of pre-cached blobs"`
		CacheCopy    bool         `long:"cache_copy" description:"Copy blobs from cache rather than attempting to link."`
		Sandbox      string       `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		Timeout      cli.Duration `long:"timeout" default:"3m" description:"Timeout for individual RPCs"`
		ProfileFile  string       `long:"profile_file" hidden:"true" description:"Write a CPU profile to this file"`
		Storage      struct {
			Storage string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
			TLS     bool   `long:"tls" description:"Use TLS for communication with the storage server"`
		} `group:"Options controlling communication with the CAS server"`
	} `command:"one" description:"Executes a single build action, identified by its action digest."`
	Admin admin.Opts `group:"Options controlling HTTP admin server" namespace:"admin"`
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

The worker supports a file-based cache; this is populated initially using the 'cache'
command and is not updated by it at runtime. This works off the observation that total
downloaded bytes probably follow a power law distribution with a few relatively rarely
updated blobs dominating much of the data downloaded.
`,
}

func main() {
	cmd := cli.ParseFlagsOrDie("Mettle", &opts)
	info := cli.MustInitFileLogging(opts.Logging.Verbosity, opts.Logging.FileVerbosity, opts.Logging.LogFile)
	if cmd != "one" && cmd != "cache" {
		opts.Admin.Logger = cli.MustGetLoggerNamed("github.com.thought-machine.http-admin")
		opts.Admin.LogInfo = info
		go admin.Serve(opts.Admin)
	}
	if cmd == "dual" {
		// Must ensure the topics are created ahead of time.
		const requests = "mem://requests"
		const responses = "mem://responses"
		common.MustOpenTopic(requests)
		common.MustOpenTopic(responses)
		for i := 0; i < opts.Dual.NumWorkers; i++ {
			go worker.RunForever(opts.InstanceName, requests, responses, fmt.Sprintf("%s-%d", opts.InstanceName, i), opts.Dual.Storage.Storage, opts.Dual.Dir, "", opts.Dual.Browser, opts.Dual.Sandbox, !opts.Dual.NoClean, opts.Dual.Storage.TLS, false, time.Duration(opts.Dual.Timeout), int64(opts.Dual.CacheMaxSize))
		}
		api.ServeForever("127.0.0.1", opts.Dual.Port, requests, responses, responses, opts.Dual.TLS.KeyFile, opts.Dual.TLS.CertFile)
	} else if cmd == "worker" {
		worker.RunForever(opts.InstanceName, opts.Worker.Queues.RequestQueue, opts.Worker.Queues.ResponseQueue, opts.Worker.Name, opts.Worker.Storage.Storage, opts.Worker.Dir, opts.Worker.CacheDir, opts.Worker.Browser, opts.Worker.Sandbox, !opts.Worker.NoClean, opts.Worker.Storage.TLS, opts.Worker.CacheCopy, time.Duration(opts.Worker.Timeout), int64(opts.Worker.CacheMaxSize))
	} else if cmd == "api" {
		api.ServeForever(opts.API.Host, opts.API.Port, opts.API.Queues.RequestQueue, opts.API.Queues.ResponseQueue + opts.API.Queues.ResponseQueueSuffix, opts.API.Queues.PreResponseQueue, opts.API.TLS.KeyFile, opts.API.TLS.CertFile)
	} else if cmd == "cache" {
		worker.NewCache(opts.Cache.Dir, false).MustStoreAll(opts.InstanceName, opts.Cache.Args.Targets, opts.Cache.Storage.Storage, opts.Cache.Storage.TLS)
	} else {
		if err := one(); err != nil {
			log.Fatalf("%s", err)
		}
	}
}

func one() error {
	if opts.One.ProfileFile != "" {
		f, err := os.Create(opts.One.ProfileFile)
		if err != nil {
			log.Fatalf("Failed to open profile file: %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("could not start profiler: %s", err)
		}
		defer f.Close()
		defer pprof.StopCPUProfile()
	}
	return worker.RunOne(opts.InstanceName, "mettle-one", opts.One.Storage.Storage, opts.One.Dir, opts.One.CacheDir, opts.Worker.Sandbox, false, opts.One.Storage.TLS, opts.One.CacheCopy, time.Duration(opts.One.Timeout), opts.One.Hash, opts.One.Size)
}
