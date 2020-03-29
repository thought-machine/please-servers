// Package main implements a CAS storage server for the Remote Execution API.
package main

import (
	"github.com/peterebden/go-cli-init"
	"github.com/thought-machine/http-admin"

	"github.com/thought-machine/please-servers/elan/rpc"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage   string
	Logging struct {
		Verbosity     cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
		FileVerbosity cli.Verbosity `long:"file_verbosity" default:"debug" description:"Verbosity of file logging output"`
		LogFile       string        `long:"log_file" description:"File to additionally log output to"`
	} `group:"Options controlling logging output"`
	Port        int    `short:"p" long:"port" default:"7777" description:"Port to serve on"`
	Storage     string `short:"s" long:"storage" required:"true" description:"URL defining where to store data, eg. gs://bucket-name."`
	TLS         struct {
		KeyFile  string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
		CertFile string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
	} `group:"Options controlling TLS for the gRPC server"`
	Cache struct {
		Port        int          `long:"cache_port" default:"8080" description:"Port to communicate cache data over"`
		MaxSize     cli.ByteSize `long:"cache_max_size" default:"10M" description:"Max size of in-memory cache"`
		MaxItemSize cli.ByteSize `long:"cache_max_item_size" default:"100K" description:"Max size of any single item in the cache"`
		Peers       []string     `long:"cache_peer" description:"URLs of cache peers to connect to. Will be monitored via DNS."`
		SelfIP      string       `long:"cache_self_ip" env:"CACHE_SELF_IP" description:"IP address of the current peer."`
	} `group:"Options controlling in-memory caching of blobs"`
	Admin admin.Opts `group:"Options controlling HTTP admin server" namespace:"admin"`
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
	info := cli.MustInitFileLogging(opts.Logging.Verbosity, opts.Logging.FileVerbosity, opts.Logging.LogFile)
	opts.Admin.Logger = cli.MustGetLoggerNamed("github.com.thought-machine.http-admin")
	opts.Admin.LogInfo = info
	go admin.Serve(opts.Admin)
	log.Notice("Serving on :%d", opts.Port)
	rpc.ServeForever(opts.Port, opts.Cache.Port, opts.Storage, opts.TLS.KeyFile, opts.TLS.CertFile, opts.Cache.SelfIP, opts.Cache.Peers, int64(opts.Cache.MaxSize), int64(opts.Cache.MaxItemSize))
}
