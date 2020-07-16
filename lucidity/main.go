// Package main implements a server to record the state of a fleet of Mettle workers.
package main

import (
	"time"

	"github.com/peterebden/go-cli-init/v2"
	"github.com/thought-machine/http-admin"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/lucidity/rpc"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	HTTPPort int          `long:"http_port" default:"7773" description:"Port to serve HTTP on"`
	MaxAge   cli.Duration `long:"max_age" description:"Forget results from any workers older than this"`
	IAP      struct {
		Audience string   `long:"audience" description:"Expected audience for the IAP tokens"`
		Users    []string `short:"u" long:"user" env:"LUCIDITY_IAP_USERS" env-delim:"," description:"Users allowed to make mutating actions on the server"`
	} `group:"Options controlling Cloud IAP auth"`
	GRPC  grpcutil.Opts `group:"Options controlling the gRPC server"`
	Admin admin.Opts    `group:"Options controlling HTTP admin server" namespace:"admin"`
}{
	Usage: `
Lucidity is a server that records the state of a fleet of Mettle workers.

Essentially the workers periodically report their current state to it; it exists simply to
make it easy to keep an eye on the state of a distributed fleet of them.

Lucidity carries on with the somewhat overdone naming scheme, referring to having a
quality of being able to think or express something clearly. If we need a video game reference
to go along with Zeal, then it could refer to the GTA Lucidity in FreeSpace 2.
`,
}

func main() {
	cli.ParseFlagsOrDie("Lucidity", &opts)
	info := cli.MustInitFileLogging(opts.Logging.Verbosity, opts.Logging.FileVerbosity, opts.Logging.LogFile)
	opts.Admin.Logger = cli.MustGetLoggerNamed("github.com.thought-machine.http-admin")
	opts.Admin.LogInfo = info
	go admin.Serve(opts.Admin)
	rpc.ServeForever(opts.GRPC, opts.HTTPPort, time.Duration(opts.MaxAge), opts.IAP.Audience, opts.IAP.Users)
}
