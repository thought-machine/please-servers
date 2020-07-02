// Package main implements a CAS storage server for the Remote Execution API.
package main

import (
	"time"

	"github.com/peterebden/go-cli-init"
	admin "github.com/thought-machine/http-admin"

	"github.com/thought-machine/please-servers/purity/gc"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	GC struct {
		URL          string       `short:"u" long:"url" required:"true" description:"URL for the storage server"`
		InstanceName string       `short:"i" long:"instance_name" default:"mettle" description:"Name of this execution instance"`
		TokenFile    string       `long:"token_file" description:"File containing token to authenticate gRPC requests with"`
		MinAge       cli.Duration `long:"min_age" required:"true" description:"Minimum age of artifacts that will be considered for purification"`
		TLS          bool         `long:"tls" description:"Use TLS for communicating with the storage server"`
	} `group:"Options controlling GC settings"`
	One struct {
		DryRun bool `long:"dry_run" description:"Don't actually clean anything, just log what we'd do"`
	} `command:"one" description:"Run just once and terminate after"`
	Periodic struct {
		Frequency cli.Duration `long:"frequency" default:"1h" description:"Length of time to wait between updates"`
	} `command:"periodic" description:"Run continually, triggering GCs at a regular interval"`
	Delete struct {
		Args struct {
			Hashes []string `positional-arg-name:"hashes" required:"true" description:"Hashes to delete"`
		} `positional-args:"true" required:"true"`
	} `command:"delete" description:"Deletes one or more build actions from the server."`
	Admin admin.Opts `group:"Options controlling HTTP admin server" namespace:"admin"`
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
	if cmd == "one" {
		if err := gc.Run(opts.GC.URL, opts.GC.InstanceName, opts.GC.TokenFile, opts.GC.TLS, time.Duration(opts.GC.MinAge), opts.One.DryRun); err != nil {
			log.Fatalf("Failed to GC: %s", err)
		}
	} else if cmd == "periodic" {
		opts.Admin.Logger = cli.MustGetLoggerNamed("github.com.thought-machine.http-admin")
		opts.Admin.LogInfo = info
		go admin.Serve(opts.Admin)
		gc.RunForever(opts.GC.URL, opts.GC.InstanceName, opts.GC.TokenFile, opts.GC.TLS, time.Duration(opts.GC.MinAge), time.Duration(opts.Periodic.Frequency))
	} else if cmd == "delete" {
		if err := gc.Delete(opts.GC.URL, opts.GC.InstanceName, opts.GC.TokenFile, opts.GC.TLS, opts.Delete.Args.Hashes); err != nil {
			log.Fatalf("Failed to delete: %s", err)
		}
	}
}
