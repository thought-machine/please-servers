// Package main implements a simple utility to diff two build actions.
package main

import (
	"context"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/peterebden/go-cli-init"

	"github.com/thought-machine/please-servers/grpcutil"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage     string
	Verbosity cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
	Storage   struct {
		InstanceName string `long:"instance" default:"mettle" description:"Instance name"`
		Storage      string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
		TLS          bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	} `group:"Options controlling connection to the CAS server"`
	Before struct {
		Hash string `long:"hash" description:"Hash of the build action"`
		Size int    `long:"size" description:"Size in bytes of the build action"`
	} `group:"Options identifying the 'before' build action" namespace:"before"`
	After struct {
		Hash string `long:"hash" description:"Hash of the build action"`
		Size int    `long:"size" description:"Size in bytes of the build action"`
	} `group:"Options identifying the 'after' build action" namespace:"after"`
}{
	Usage: `
Discern is a simple binary for showing differences between two build actions.
This can be useful for a "what's changed" kind of question.

Note that it does not support every field exhaustively right now - notably we leave
out NodeProperties since we aren't using them (yet?).

It doesn't quite follow our naming scheme (should be "discerning") but it
also isn't a server so #dealwithit
`,
}

func main() {
	cli.ParseFlagsOrDie("Discern", &opts)
	cli.InitLogging(opts.Verbosity)
	client, err := client.NewClient(context.Background(), opts.Storage.InstanceName, client.DialParams{
		Service:            opts.Storage.Storage,
		NoSecurity:         !opts.Storage.TLS,
		TransportCredsOnly: opts.Storage.TLS,
		DialOpts:           grpcutil.DialOptions(""),
	}, client.UseBatchOps(true), client.RetryTransient())
	if err != nil {
		log.Fatalf("Failed to contact CAS server: %s", err)
	}
	before := &pb.Action{}
	after := &pb.Action{}
	mustGetProto(client, opts.Before.Hash, opts.Before.Size, before)
	mustGetProto(client, opts.After.Hash, opts.After.Size, after)
	if before.CommandDigest.Hash == after.CommandDigest.Hash {
		log.Notice("Commands are identical")
	} else {
		log.Warning("Commands differ: %s vs. %s", before.CommandDigest.Hash, after.CommandDigest.Hash)
		beforeCmd := &pb.Command{}
		afterCmd := &pb.Command{}
		mustGetProtoDigest(client, before.CommandDigest, beforeCmd)
		mustGetProtoDigest(client, after.CommandDigest, afterCmd)
		compareCommands(beforeCmd, afterCmd)
	}
	if before.InputRootDigest.Hash == after.InputRootDigest.Hash {
		log.Notice("Input roots are identical")
	} else {
		log.Warning("Input roots differ: %s vs. %s", before.InputRootDigest, after.InputRootDigest)
		compareDirectories(client, before.InputRootDigest, after.InputRootDigest, "")
	}
	if !proto.Equal(before.Timeout, after.Timeout) {
		log.Warning("Timeouts are different: %s / %s", before.Timeout, after.Timeout)
	}
	if before.DoNotCache != after.DoNotCache {
		log.Warning("DoNotCache differs: %v / %v", before.DoNotCache, after.DoNotCache)
	}
}

func mustGetProto(client *client.Client, hash string, size int, msg proto.Message) {
	if err := client.ReadProto(context.Background(), digest.Digest{
		Hash: hash,
		Size: int64(size),
	}, msg); err != nil {
		log.Fatalf("Failed to fetch digest %s: %s", hash, err)
	}
}

func mustGetProtoDigest(client *client.Client, digest *pb.Digest, msg proto.Message) {
	mustGetProto(client, digest.Hash, int(digest.SizeBytes), msg)
}

func compareCommands(b, a *pb.Command) {
	compareRepeatedString("Arguments", b.Arguments, a.Arguments)
	if compareRepeatedString("OutputPaths", b.OutputPaths, a.OutputPaths) {
		// Don't repeat these two if output paths differ (since that is basically a superset)
		compareRepeatedString("OutputFiles", b.OutputFiles, a.OutputFiles)
		compareRepeatedString("OutputDirectories", b.OutputDirectories, a.OutputDirectories)
	}
	// We could do a better test here and match up names but c'est la vie.
	for i, v := range b.EnvironmentVariables {
		if i >= len(a.EnvironmentVariables) {
			log.Warning("Environment variable %s not in 'after' action")
		} else if v2 := a.EnvironmentVariables[i]; v2.Name != v.Name || v2.Value != v.Value {
			log.Warning("Environment variables differ: %s=%s / %s=%s", v.Name, v.Value, v2.Name, v2.Value)
		}
	}
	// TODO(peterebden): check platform properties too
}

func compareRepeatedString(name string, b, a []string) bool {
	ret := false
	for i, s1 := range b {
		if i >= len(a) {
			log.Warning("%s differ; %s not in 'after' action", name, s1)
			ret = true
		} else if s2 := a[i]; s1 != s2 {
			log.Warning("%s differ: %s / %s", s1, s2)
			ret = true
		}
	}
	return ret
}

func compareDirectories(client *client.Client, before, after *pb.Digest, indent string) {
	b := &pb.Directory{}
	a := &pb.Directory{}
	mustGetProtoDigest(client, before, b)
	mustGetProtoDigest(client, after, a)
	for i, f1 := range b.Files {
		if i >= len(a.Files) {
			log.Warning("%s%s %s%s / <missing>", indent, f1.Name, f1.Digest.Hash, exe(f1.IsExecutable))
		} else if f2 := a.Files[i]; !proto.Equal(f1, f2) {
			log.Warning("%s%s %s%s / %s %s%s", indent, f1.Name, f1.Digest.Hash, exe(f1.IsExecutable), f2.Name, f2.Digest.Hash, exe(f2.IsExecutable))
		}
	}
	for i, d1 := range b.Directories {
		if i >= len(a.Directories) {
			log.Warning("%s%s %s / <missing>", indent, d1.Name, d1.Digest.Hash)
		} else if d2 := a.Directories[i]; !proto.Equal(d1, d2) {
			log.Warning("%s%s %s / %s %s", indent, d1.Name, d1.Digest.Hash, d2.Name, d2.Digest.Hash)
			compareDirectories(client, d1.Digest, d2.Digest, indent+"  ")
		}
	}
	for i, s1 := range b.Symlinks {
		if i >= len(a.Symlinks) {
			log.Warning("%s%s -> %s / <missing>", indent, s1.Name, s1.Target)
		} else if s2 := a.Symlinks[i]; !proto.Equal(s1, s2) {
			log.Warning("%s%s -> %s / %s -> %s", indent, s1.Name, s1.Target, s2.Name, s2.Target)
		}
	}
}

func exe(is bool) string {
	if is {
		return " (exe)"
	}
	return ""
}
