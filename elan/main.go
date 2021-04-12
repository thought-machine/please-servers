// Package main implements a CAS storage server for the Remote Execution API.
package main

import (
	"github.com/peterebden/go-cli-init/v4/flags"

	"github.com/thought-machine/please-servers/cli"
	"github.com/thought-machine/please-servers/elan/rpc"
	"github.com/thought-machine/please-servers/grpcutil"
)

var opts = struct {
	Usage              string
	Logging            cli.LoggingOpts `group:"Options controlling logging output"`
	GRPC               grpcutil.Opts   `group:"Options controlling the gRPC server"`
	Storage            string          `short:"s" long:"storage" required:"true" description:"URL defining where to store data, eg. gs://bucket-name."`
	Parallelism        int             `long:"parallelism" default:"50" description:"Maximum number of in-flight parallel requests to the backend storage layer"`
	DirCacheSize       int64           `long:"dir_cache_size" default:"10240" description:"Number of directory entries to cache for GetTree"`
	KnownBlobCacheSize flags.ByteSize  `long:"known_blob_cache_size" description:"Max size of known blob cache (in approximate bytes)"`
	Admin              cli.AdminOpts   `group:"Options controlling HTTP admin server" namespace:"admin"`
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
	_, info := cli.ParseFlagsOrDie("Elan", &opts, &opts.Logging)
	go cli.ServeAdmin(opts.Admin, info)
	rpc.ServeForever(opts.GRPC, opts.Storage, opts.Parallelism, opts.DirCacheSize, int64(opts.KnownBlobCacheSize))
}
