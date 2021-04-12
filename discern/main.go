// Package main implements a simple utility to visualise build actions.
package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/dustin/go-humanize"
	"github.com/golang/protobuf/proto"
	"github.com/peterebden/go-cli-init/v4"

	flags "github.com/thought-machine/please-servers/cli"
	"github.com/thought-machine/please-servers/purity/gc"
	"github.com/thought-machine/please-servers/rexclient"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage     string
	Verbosity cli.Verbosity `short:"v" long:"verbosity" default:"notice" description:"Verbosity of output (higher number = more output)"`
	Storage   struct {
		InstanceName string `short:"i" long:"instance" default:"mettle" description:"Instance name"`
		Storage      string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
		TLS          bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	} `group:"Options controlling connection to the CAS server"`
	Diff struct {
		Before flags.Action `short:"b" long:"before" required:"true" description:"'Before' action hash"`
		After  flags.Action `short:"a" long:"after" required:"true" description:"'After' action hash"`
	} `command:"diff" description:"Show differences between two actions"`
	Show struct {
		Args struct {
			Actions []flags.Action `positional-arg-name:"action" required:"true" description:"Hashes of actions to display"`
		} `positional-args:"true"`
	} `command:"show" description:"Show detail about an action or series of them"`
	TopN struct {
		N          int    `short:"n" long:"number" default:"100" description:"Number of actions to display"`
		BrowserURL string `long:"browser_url" description:"Browser base URL to display links to"`
	} `command:"topn" description:"Display information on the top N actions with biggest inputs / outputs"`
	MostUsed struct {
		N       int      `short:"n" long:"number" default:"100" description:"Number of blobs to display"`
		Include []string `short:"i" long:"include" description:"Filename prefixes to include"`
		Exclude []string `short:"e" long:"exclude" description:"Filename prefixes to exclude"`
	} `command:"mostused" description:"Display information on the most-downloaded blobs"`
}{
	Usage: `
Discern is a simple binary for visualising build actions; either showing differences
between two or displaying the inputs to a single one.
This can be useful for a "what's changed" kind of question.

Note that it does not support every field exhaustively right now - notably we leave
out NodeProperties since we aren't using them (yet?).

It doesn't quite follow our naming scheme (should be "discerning") but it
also isn't a server so #dealwithit
`,
}

func main() {
	cmd := cli.ParseFlagsOrDie("Discern", &opts)
	cli.InitLogging(opts.Verbosity)
	if cmd == "topn" {
		if err := topn(); err != nil {
			log.Fatalf("Failed to find action results: %s", err)
		}
		return
	} else if cmd == "mostused" {
		if err := mostused(); err != nil {
			log.Fatalf("Failed to find blob info: %s", err)
		}
		return
	}
	client := rexclient.MustNew(opts.Storage.InstanceName, opts.Storage.Storage, opts.Storage.TLS, "")
	if cmd == "diff" {
		diff(client)
	} else {
		show(client)
	}
}

func diff(client *client.Client) {
	before := &pb.Action{}
	after := &pb.Action{}
	mustGetProto(client, opts.Diff.Before.ToProto(), before)
	mustGetProto(client, opts.Diff.After.ToProto(), after)
	if before.CommandDigest.Hash == after.CommandDigest.Hash {
		log.Notice("Commands are identical")
	} else {
		log.Warning("Commands differ: %s vs. %s", before.CommandDigest.Hash, after.CommandDigest.Hash)
		beforeCmd := &pb.Command{}
		afterCmd := &pb.Command{}
		mustGetProto(client, before.CommandDigest, beforeCmd)
		mustGetProto(client, after.CommandDigest, afterCmd)
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

func mustGetProto(client *client.Client, dg *pb.Digest, msg proto.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.ReadProto(ctx, digest.NewFromProtoUnvalidated(dg), msg); err != nil {
		log.Fatalf("Failed to fetch digest %s: %s", dg.Hash, err)
	}
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
			log.Warning("%s differ: %s / %s", name, s1, s2)
			ret = true
		}
	}
	return ret
}

func compareDirectories(client *client.Client, before, after *pb.Digest, indent string) {
	b := &pb.Directory{}
	a := &pb.Directory{}
	mustGetProto(client, before, b)
	mustGetProto(client, after, a)
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

func show(client *client.Client) {
	for i, a := range opts.Show.Args.Actions {
		if i > 0 {
			fmt.Print("\n\n")
		}
		log.Notice("Action %s/%d:", a.Hash, a.Size)
		action := &pb.Action{}
		command := &pb.Command{}
		mustGetProto(client, a.ToProto(), action)
		mustGetProto(client, action.CommandDigest, command)
		log.Notice("Inputs:")
		showDir(client, action.InputRootDigest, "")
	}
}

func showDir(client *client.Client, dg *pb.Digest, indent string) {
	dir := &pb.Directory{}
	if err := client.ReadProto(context.Background(), digest.NewFromProtoUnvalidated(dg), dir); err != nil {
		log.Error("[%s/%08d] %s: Not found!", dg.Hash, dg.SizeBytes, indent)
		return
	}
	for _, d := range dir.Directories {
		log.Notice("[%s/%08d] %s%s/", d.Digest.Hash, d.Digest.SizeBytes, indent, d.Name)
		showDir(client, d.Digest, indent+"  ")
	}
	req := &pb.FindMissingBlobsRequest{InstanceName: client.InstanceName}
	for _, f := range dir.Files {
		req.BlobDigests = append(req.BlobDigests, f.Digest)
	}
	resp, err := client.FindMissingBlobs(context.Background(), req)
	if err != nil {
		log.Error("%s: Request failed! %s", indent, err)
		return
	}
	m := map[string]bool{}
	for _, r := range resp.MissingBlobDigests {
		m[r.Hash] = true
	}
	for _, f := range dir.Files {
		if m[f.Digest.Hash] {
			log.Error("[%s/%08d] %s%s Not found!", f.Digest.Hash, f.Digest.SizeBytes, indent, f.Name)
		} else {
			log.Notice("[%s/%08d] %s%s", f.Digest.Hash, f.Digest.SizeBytes, indent, f.Name)
		}
	}
	for _, s := range dir.Symlinks {
		log.Notice("[%s/%08d]%s%-50s -> %s", strings.Repeat(" ", 64), 0, indent, s.Name, s.Target)
	}
}

func topn() error {
	actions, err := gc.Sizes(opts.Storage.Storage, opts.Storage.InstanceName, "", opts.Storage.TLS, opts.TopN.N)
	if err != nil {
		return err
	}
	log.Notice("Top %d actions:", opts.TopN.N)
	for i, a := range actions {
		in := humanize.Bytes(uint64(a.InputSize))
		out := humanize.Bytes(uint64(a.OutputSize))
		if opts.TopN.BrowserURL != "" {
			log.Notice("%d: %s (in) %s (out): %s/action/%s/%s/%d/", i, in, out, opts.TopN.BrowserURL, opts.Storage.InstanceName, a.Hash, a.SizeBytes)
		} else {
			log.Notice("%d: %s (in) %s (out): %s/%d", i, in, out, a.Hash, a.SizeBytes)
		}
	}
	return nil
}

func mostused() error {
	allBlobs, err := gc.BlobUsage(opts.Storage.Storage, opts.Storage.InstanceName, "", opts.Storage.TLS)
	if err != nil {
		return err
	}
	blobs := make([]gc.Blob, 0, len(allBlobs))
	for _, blob := range allBlobs {
		if shouldInclude(blob.Filename) {
			blobs = append(blobs, blob)
		}
	}
	sort.Slice(blobs, func(i, j int) bool {
		return blobs[i].SizeBytes*blobs[i].Count > blobs[j].SizeBytes*blobs[j].Count
	})
	if len(blobs) > opts.MostUsed.N {
		blobs = blobs[:opts.MostUsed.N]
	}
	log.Notice("Most used %d blobs:", opts.MostUsed.N)
	var size, total int64
	for _, blob := range blobs {
		log.Notice("%s/%08d: %s (%s, used %d times, total %s)", blob.Hash, blob.SizeBytes, blob.Filename, humanize.Bytes(uint64(blob.SizeBytes)), blob.Count, humanize.Bytes(uint64(blob.SizeBytes*blob.Count)))
		size += blob.SizeBytes
		total += blob.SizeBytes * blob.Count
	}
	log.Notice("Total size %s, total downloads %s", humanize.Bytes(uint64(size)), humanize.Bytes(uint64(total)))
	return nil
}

func shouldInclude(name string) bool {
	for _, excl := range opts.MostUsed.Exclude {
		if strings.HasPrefix(name, excl) {
			return false
		}
	}
	if len(opts.MostUsed.Include) == 0 {
		return true
	}
	for _, incl := range opts.MostUsed.Include {
		if strings.HasPrefix(name, incl) {
			return true
		}
	}
	return false
}
