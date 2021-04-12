// Package main implements an execution server for the Remote Execution API.
package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/peterebden/go-cli-init/v4/flags"
	"github.com/peterebden/go-cli-init/v4/logging"

	"github.com/thought-machine/please-servers/cli"
	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/api"
	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/mettle/worker"
)

var log = logging.MustGetLogger()

type StorageOpts struct {
	Storage   string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
	TLS       bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	TokenFile string `long:"token_file" description:"File containing a pre-shared token to authenticate to storage server with."`
}

type CacheOpts struct {
	Dir    string         `long:"dir" description:"Directory to cache blobs in"`
	Prefix []string       `long:"prefix" description:"Path prefix for cache items to store"`
	Part   []string       `long:"part" description:"Cache any paths with a component with this name in them"`
	MaxMem flags.ByteSize `long:"max_size" default:"100M" description:"Max size of in-memory blob cache"`
}

var opts = struct {
	Usage        string
	Logging      cli.LoggingOpts `group:"Options controlling logging output"`
	InstanceName string          `short:"i" long:"instance_name" default:"mettle" description:"Name of this execution instance"`
	API          struct {
		API struct {
			URL string `long:"url" description:"URL for communicating with other API servers"`
			TLS bool   `long:"tls" description:"Use TLS for communication between api servers"`
		} `group:"Options controlling communication with other API servers for bootstrapping zero-downtime deployments." namespace:"api"`
		GRPC   grpcutil.Opts `group:"Options controlling the gRPC server"`
		Queues struct {
			RequestQueue        string `short:"q" long:"request_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending requests, e.g. gcppubsub://my-request-queue"`
			ResponseQueue       string `short:"r" long:"response_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending responses, e.g. gcppubsub://my-response-queue"`
			ResponseQueueSuffix string `long:"response_queue_suffix" env:"RESPONSE_QUEUE_SUFFIX" description:"Suffix to apply to the response queue name"`
			PreResponseQueue    string `long:"pre_response_queue" required:"true" description:"URL describing the pub/sub queue to connect to for preloading responses to other servers"`
		} `group:"Options controlling the pub/sub queues"`
		AllowedPlatform map[string][]string `long:"allowed_platform" description:"Allowed values for platform properties"`
	} `command:"api" description:"Start as an API server"`
	Worker struct {
		Dir             string                  `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		NoClean         bool                    `long:"noclean" description:"Don't clean workdirs after actions complete"`
		Name            string                  `short:"n" long:"name" description:"Name of this worker"`
		Browser         string                  `long:"browser" description:"Base URL for browser service (only used to construct informational user messages"`
		Lucidity        string                  `long:"lucidity" description:"URL of Lucidity server to report to"`
		Sandbox         string                  `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		AltSandbox      string                  `long:"alt_sandbox" description:"Location of tool to sandbox build actions with that don't explicitly request it"`
		Timeout         flags.Duration          `long:"timeout" hidden:"true" description:"Deprecated, has no effect."`
		MinDiskSpace    flags.ByteSize          `long:"min_disk_space" default:"1G" description:"Don't accept builds unless at least this much disk space is available"`
		MemoryThreshold float64                 `long:"memory_threshold" default:"100.0" description:"Don't accept builds unless available memory is under this percentage"`
		VersionFile     string                  `long:"version_file" description:"File containing version tag"`
		Costs           map[string]cli.Currency `long:"cost" description:"Per-second costs to associate with each build action."`
		Cache           CacheOpts               `group:"Options controlling caching" namespace:"cache"`
		Storage         StorageOpts             `group:"Options controlling communication with the CAS server"`
		Queues          struct {
			RequestQueue  string         `short:"q" long:"request_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending requests, e.g. gcppubsub://my-request-queue"`
			ResponseQueue string         `short:"r" long:"response_queue" required:"true" description:"URL defining the pub/sub queue to connect to for sending responses, e.g. gcppubsub://my-response-queue"`
			AckExtension  flags.Duration `long:"ack_extension" description:"Period to extend the ack deadline by during execution. Only has any effect on gcppubsub queues."`
		} `group:"Options controlling the pub/sub queues"`
	} `command:"worker" description:"Start as a worker"`
	Dual struct {
		GRPC            grpcutil.Opts           `group:"Options controlling the gRPC server"`
		Dir             string                  `short:"d" long:"dir" default:"plz-out/mettle" description:"Directory to run actions in"`
		NoClean         bool                    `long:"noclean" env:"METTLE_NO_CLEAN" description:"Don't clean workdirs after actions complete"`
		NumWorkers      int                     `short:"n" long:"num_workers" env:"METTLE_NUM_WORKERS" description:"Number of workers to run in parallel"`
		Browser         string                  `long:"browser" description:"Base URL for browser service (only used to construct informational user messages"`
		Lucidity        string                  `long:"lucidity" description:"URL of Lucidity server to report to"`
		Sandbox         string                  `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		AltSandbox      string                  `long:"alt_sandbox" description:"Location of tool to sandbox build actions with that don't explicitly request it"`
		Timeout         flags.Duration          `long:"timeout" hidden:"true" description:"Deprecated, has no effect."`
		MinDiskSpace    flags.ByteSize          `long:"min_disk_space" default:"1G" description:"Don't accept builds unless at least this much disk space is available"`
		MemoryThreshold float64                 `long:"memory_threshold" default:"100.0" description:"Don't accept builds unless available memory is under this percentage"`
		VersionFile     string                  `long:"version_file" description:"File containing version tag"`
		Costs           map[string]cli.Currency `long:"cost" description:"Per-second costs to associate with each build action."`
		Cache           CacheOpts               `group:"Options controlling caching" namespace:"cache"`
		Storage         struct {
			Storage []string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
			TLS     bool     `long:"tls" description:"Use TLS for communication with the storage server"`
		}
		AllowedPlatform map[string][]string `long:"allowed_platform" description:"Allowed values for platform properties"`
	} `command:"dual" description:"Start as both API server and worker. For local testing only."`
	One struct {
		Args struct {
			Actions []cli.Action `positional-arg-name:"action" required:"true" description:"The action digest to run"`
		} `positional-args:"true"`
		Dir         string         `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		Sandbox     string         `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		AltSandbox  string         `long:"alt_sandbox" description:"Location of tool to sandbox build actions with that don't explicitly request it"`
		Timeout     flags.Duration `long:"timeout" hidden:"true" description:"Deprecated, has no effect."`
		ProfileFile string         `long:"profile_file" hidden:"true" description:"Write a CPU profile to this file"`
		MemProfile  string         `long:"mem_profile_file" hidden:"true" description:"Write a memory profile to this file"`
		Cache       CacheOpts      `group:"Options controlling caching" namespace:"cache"`
		Storage     StorageOpts    `group:"Options controlling communication with the CAS server"`
	} `command:"one" description:"Executes a single build action, identified by its action digest."`
	Admin cli.AdminOpts `group:"Options controlling HTTP admin server" namespace:"admin"`
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
	cmd, info := cli.ParseFlagsOrDie("Mettle", &opts, &opts.Logging)
	if cmd != "one" {
		go cli.ServeAdmin(opts.Admin, info)
	}

	if cmd == "dual" {
		const requests = "mem://requests"
		const responses = "mem://responses"
		// Must ensure the topics are created ahead of time.
		common.MustOpenTopic(requests)
		common.MustOpenTopic(responses)
		if opts.Dual.NumWorkers == 0 {
			opts.Dual.NumWorkers = runtime.NumCPU()
		}
		for i := 0; i < opts.Dual.NumWorkers; i++ {
			storage := opts.Dual.Storage.Storage[i%len(opts.Dual.Storage.Storage)]
			go worker.RunForever(opts.InstanceName, requests+"?ackdeadline=10m", responses, fmt.Sprintf("%s-%d", opts.InstanceName, i), storage, opts.Dual.Dir, opts.Dual.Cache.Dir, opts.Dual.Browser, opts.Dual.Sandbox, opts.Dual.AltSandbox, opts.Dual.Lucidity, opts.Dual.GRPC.TokenFile, opts.Dual.Cache.Prefix, opts.Dual.Cache.Part, !opts.Dual.NoClean, opts.Dual.Storage.TLS, int64(opts.Dual.Cache.MaxMem), int64(opts.Dual.MinDiskSpace), opts.Dual.MemoryThreshold, opts.Dual.VersionFile, opts.Dual.Costs, 0)
		}
		api.ServeForever(opts.Dual.GRPC, "", requests, responses, responses, "", false, opts.Dual.AllowedPlatform)
	} else if cmd == "worker" {
		worker.RunForever(opts.InstanceName, opts.Worker.Queues.RequestQueue, opts.Worker.Queues.ResponseQueue, opts.Worker.Name, opts.Worker.Storage.Storage, opts.Worker.Dir, opts.Worker.Cache.Dir, opts.Worker.Browser, opts.Worker.Sandbox, opts.Worker.AltSandbox, opts.Worker.Lucidity, opts.Worker.Storage.TokenFile, opts.Worker.Cache.Prefix, opts.Worker.Cache.Part, !opts.Worker.NoClean, opts.Worker.Storage.TLS, int64(opts.Worker.Cache.MaxMem), int64(opts.Worker.MinDiskSpace), opts.Worker.MemoryThreshold, opts.Worker.VersionFile, opts.Worker.Costs, time.Duration(opts.Worker.Queues.AckExtension))
	} else if cmd == "api" {
		api.ServeForever(opts.API.GRPC, opts.API.Queues.ResponseQueueSuffix, opts.API.Queues.RequestQueue, opts.API.Queues.ResponseQueue+opts.API.Queues.ResponseQueueSuffix, opts.API.Queues.PreResponseQueue, opts.API.API.URL, opts.API.API.TLS, opts.API.AllowedPlatform)
	} else if err := one(); err != nil {
		log.Fatalf("%s", err)
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
	if opts.One.MemProfile != "" {
		f, err := os.Create(opts.One.MemProfile)
		if err != nil {
			log.Fatalf("Failed to open memory profile file: %s", err)
		}
		defer f.Close()
		defer pprof.WriteHeapProfile(f)
	}
	for _, action := range opts.One.Args.Actions {
		if err := worker.RunOne(opts.InstanceName, "mettle-one", opts.One.Storage.Storage, opts.One.Dir, opts.One.Cache.Dir, opts.One.Sandbox, opts.One.AltSandbox, opts.One.Storage.TokenFile, opts.One.Cache.Prefix, opts.One.Cache.Part, false, opts.One.Storage.TLS, action.ToProto()); err != nil {
			return err
		}
	}
	return nil
}
