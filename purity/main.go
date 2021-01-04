// Package main implements a service to provide GC for Elan.
package main

import (
	"os"
	"runtime/pprof"
	"time"

	"github.com/peterebden/go-cli-init/v3"

	flags "github.com/thought-machine/please-servers/cli"
	"github.com/thought-machine/please-servers/purity/gc"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage   string
	Logging flags.LoggingOpts `group:"Options controlling logging output"`
	GC      struct {
		URL          string `short:"u" long:"url" required:"true" description:"URL for the storage server"`
		InstanceName string `short:"i" long:"instance_name" default:"purity-gc" description:"Name of this execution instance"`
		TokenFile    string `long:"token_file" description:"File containing token to authenticate gRPC requests with"`
		TLS          bool   `long:"tls" description:"Use TLS for communicating with the storage server"`
	} `group:"Options controlling GC settings"`
	One struct {
		DryRun            bool         `long:"dry_run" description:"Don't actually clean anything, just log what we'd do"`
		MinAge            cli.Duration `long:"min_age" required:"true" description:"Minimum age of artifacts that will be considered for purification"`
		ReplicationFactor int          `long:"replication_factor" description:"Min number of replicas to expect for a blob"`
	} `command:"one" description:"Run just once and terminate after"`
	Periodic struct {
		Frequency         cli.Duration `long:"frequency" default:"1h" description:"Length of time to wait between updates"`
		MinAge            cli.Duration `long:"min_age" required:"true" description:"Minimum age of artifacts that will be considered for purification"`
		ReplicationFactor int          `long:"replication_factor" description:"Min number of replicas to expect for a blob"`
	} `command:"periodic" description:"Run continually, triggering GCs at a regular interval"`
	Delete struct {
		Args struct {
			Actions []flags.Action `positional-arg-name:"actions" required:"true" description:"Actions to delete"`
		} `positional-args:"true" required:"true"`
	} `command:"delete" description:"Deletes one or more build actions from the server."`
	Clean struct {
		DryRun bool `long:"dry_run" description:"Don't actually clean anything, just log what we'd do"`
	} `command:"clean" description:"Cleans out any build actions with missing inputs or outputs"`
	Replicate struct {
		ReplicationFactor int  `long:"replication_factor" required:"true" description:"Min number of replicas to expect for a blob"`
		DryRun            bool `long:"dry_run" description:"Don't actually do anything, just log what we'd do"`
	} `command:"replicate" description:"Re-replicates any underreplicated blobs"`
	Admin       admin.Opts `group:"Options controlling HTTP admin server" namespace:"admin"`
	ProfileFile string     `long:"profile_file" hidden:"true" description:"Write a CPU profile to this file"`
}{
	Usage: `
Purity is a service to implement GC logic for Elan.

It queries the given servers to identify the set of action results to
retain, finds all blobs transitively referred to by them, and exiles all
others.

The name refers simply GC as a means of "purifying" things and vaguely
retains the "personal characteristics" theme.
`,
}

func main() {
	cmd := cli.ParseFlagsOrDie("Purity", &opts)
	info := cli.MustInitFileLogging(opts.Logging.Verbosity, opts.Logging.FileVerbosity, opts.Logging.LogFile)
	opts.Admin.Logger = cli.MustGetLoggerNamed("github.com.thought-machine.http-admin")
	opts.Admin.LogInfo = info
	if err := run(cmd); err != nil {
		log.Fatalf("Failed: %s", err)
	}
}

func run(cmd string) error {
	if opts.ProfileFile != "" {
		f, err := os.Create(opts.ProfileFile)
		if err != nil {
			log.Fatalf("Failed to open profile file: %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("could not start profiler: %s", err)
		}
		defer f.Close()
		defer pprof.StopCPUProfile()
	}
	if cmd == "one" {
		return gc.Run(opts.GC.URL, opts.GC.InstanceName, opts.GC.TokenFile, opts.GC.TLS, time.Duration(opts.One.MinAge), opts.One.ReplicationFactor, opts.One.DryRun)
	} else if cmd == "periodic" {
		go admin.Serve(opts.Admin)
		gc.RunForever(opts.GC.URL, opts.GC.InstanceName, opts.GC.TokenFile, opts.GC.TLS, time.Duration(opts.Periodic.MinAge), time.Duration(opts.Periodic.Frequency), opts.One.ReplicationFactor)
	} else if cmd == "delete" {
		return gc.Delete(opts.GC.URL, opts.GC.InstanceName, opts.GC.TokenFile, opts.GC.TLS, flags.AllToProto(opts.Delete.Args.Actions))
	} else if cmd == "clean" {
		return gc.Clean(opts.GC.URL, opts.GC.InstanceName, opts.GC.TokenFile, opts.GC.TLS, opts.Clean.DryRun)
	} else if cmd == "replicate" {
		return gc.Replicate(opts.GC.URL, opts.GC.InstanceName, opts.GC.TokenFile, opts.GC.TLS, opts.Replicate.ReplicationFactor, opts.Replicate.DryRun)
	}
	return nil
}
