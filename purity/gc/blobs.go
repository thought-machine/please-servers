package gc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ppb "github.com/thought-machine/please-servers/proto/purity"
	"github.com/thought-machine/please-servers/rexclient"
)

func (c *collector) markReferencedBlobs(ar *ppb.ActionResult) error {
	outputBlobs, err := c.outputBlobs(ar)
	if err != nil {
		return err
	}

	// Mark all the inputs as well. There are some fringe cases that make things awkward
	// and it means things look more sensible in the browser.
	inputBlobs := c.inputBlobs(ar)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	log.Debugf("marking input blobs for %s: %v", ar.Hash, inputBlobs)
	var inputSize int64
	for _, b := range inputBlobs {
		c.referencedBlobs[b.Hash] = struct{}{}
		inputSize += b.SizeBytes
	}
	c.inputSizes[ar.Hash] = int(inputSize)
	log.Debugf("marking output blobs for %s: %v", ar.Hash, outputBlobs)
	var outputSize int64
	for _, b := range outputBlobs {
		c.referencedBlobs[b.Hash] = struct{}{}
		outputSize += b.SizeBytes
	}
	c.outputSizes[ar.Hash] = int(outputSize)
	c.referencedBlobs[ar.Hash] = struct{}{}
	return nil
}

// inputBlobs returns all the inputs for an action. It doesn't return any errors because we don't
// want it to be fatal on failure; otherwise anything missing breaks the whole process.
func (c *collector) inputBlobs(ar *ppb.ActionResult) []*pb.Digest {
	dg := &pb.Digest{Hash: ar.Hash, SizeBytes: ar.SizeBytes}
	action := &pb.Action{}
	blob, present := c.allBlobs[dg.Hash]
	if !present {
		log.Errorf("missing action for %s", dg.Hash)
		atomic.AddInt64(&c.missingInputs, 1)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	if err := c.client.ReadProto(ctx, digest.Digest{
		Hash: dg.Hash,
		Size: blob.SizeBytes,
	}, action); err != nil {
		log.Errorf("Failed to read action %s: %s", dg.Hash, err)
		atomic.AddInt64(&c.missingInputs, 1)
		return nil
	}

	digests, err := c.blobsForAction(action)
	if err != nil {
		log.Errorf("Failed to get blobs for action %s: %s", dg.Hash, err)
		atomic.AddInt64(&c.missingInputs, 1)
		return nil
	}
	return append(digests, dg)
}

func (c *collector) blobsForAction(action *pb.Action) ([]*pb.Digest, error) {
	digests := make([]*pb.Digest, 0, 2)
	if action.InputRootDigest == nil {
		return nil, errors.New("nil input root")
	}
	digests = append(digests, action.InputRootDigest)
	if action.CommandDigest != nil {
		digests = append(digests, action.CommandDigest)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	dirs, err := c.client.GetDirectoryTree(ctx, action.InputRootDigest)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory tree for input root %s: %s", action.InputRootDigest, err)
	}
	for _, dir := range dirs {
		digests = append(digests, c.blobsForDirectory(dir)...)
	}
	return digests, nil
}

func (c *collector) outputBlobs(ar *ppb.ActionResult) ([]*pb.Digest, error) {
	dg := &pb.Digest{Hash: ar.Hash, SizeBytes: ar.SizeBytes}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	result, err := c.client.GetActionResult(ctx, &pb.GetActionResultRequest{
		InstanceName: c.client.InstanceName,
		ActionDigest: dg,
	})
	if err != nil {
		return nil, fmt.Errorf("Couldn't download action result for %s: %s", ar.Hash, err)
	}
	if result.ExitCode != 0 {
		return nil, fmt.Errorf("Found failed action result %s: exit code %d", ar.Hash, result.ExitCode)
	}

	outputBlobs, err := c.blobsForActionResult(result)
	if err != nil {
		return nil, err
	}

	// Check whether all these outputs exist.
	resp, err := c.client.FindMissingBlobs(context.Background(), &pb.FindMissingBlobsRequest{
		InstanceName: c.client.InstanceName,
		BlobDigests:  outputBlobs,
	})
	if err != nil {
		log.Warning("Failed to check blob digests for %s: %s", ar.Hash, err)
	}
	if resp != nil && len(resp.MissingBlobDigests) > 0 {
		digests := make([]string, len(resp.MissingBlobDigests))
		for i, dg := range resp.MissingBlobDigests {
			digests[i] = fmt.Sprintf("%s/%d", dg.Hash, dg.SizeBytes)
		}
		return nil, fmt.Errorf("Action result is missing %d digests: %s", len(resp.MissingBlobDigests), strings.Join(digests, ", "))
	}
	return outputBlobs, nil
}

func (c *collector) blobsForActionResult(ar *pb.ActionResult) ([]*pb.Digest, error) {
	digests := make([]*pb.Digest, 0, len(ar.OutputDirectories))
	for _, d := range ar.OutputDirectories {
		dgs, err := c.blobsForOutputDir(d)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil, err
			}
		}
		digests = append(digests, dgs...)
	}
	for _, f := range ar.OutputFiles {
		digests = append(digests, f.Digest)
	}
	if ar.StdoutDigest != nil {
		digests = append(digests, ar.StdoutDigest)
	}
	if ar.StderrDigest != nil {
		digests = append(digests, ar.StderrDigest)
	}
	return digests, nil
}

func (c *collector) blobsForOutputDir(d *pb.OutputDirectory) ([]*pb.Digest, error) {
	tree := &pb.Tree{}
	if err := c.client.ReadProto(context.Background(), digest.NewFromProtoUnvalidated(d.TreeDigest), tree); err != nil {
		return nil, err
	}

	return append(c.blobsForTree(tree), d.TreeDigest), nil
}

func (c *collector) blobsForTree(tree *pb.Tree) []*pb.Digest {
	// Here we attempt to fix up any outputs that don't also have the input facet.
	// This is an incredibly sucky part of the API; the output doesn't contain some of the blobs
	// that will get used on the input facet, so it's really nonobvious where they come from.
	c.checkDirectories(append(tree.Children, tree.Root))

	digests := c.blobsForDirectory(tree.Root)
	for _, child := range tree.Children {
		digests = append(digests, c.blobsForDirectory(child)...)
	}
	return digests
}

// checkDirectory checks that the directory protos from a Tree still exist in the CAS and uploads it if not.
func (c *collector) checkDirectories(dirs []*pb.Directory) {
	entries := make([]*uploadinfo.Entry, len(dirs))
	for i, d := range dirs {
		e, _ := uploadinfo.EntryFromProto(d)
		entries[i] = e
	}
	if !c.dryRun {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		if _, _, err := c.client.UploadIfMissing(ctx, entries...); err != nil {
			log.Warning("Failed to upload missing directory protos: %s", err)
		}
	}
}

func (c *collector) blobsForDirectory(d *pb.Directory) []*pb.Digest {
	// If the dir has any node properties, save a copy of one that doesn't.
	cp := d
	if d.NodeProperties != nil {
		cp = proto.Clone(d).(*pb.Directory)
		cp.NodeProperties = nil
	}
	ue, _ := uploadinfo.EntryFromProto(cp)
	digests := []*pb.Digest{ue.Digest.ToProto()}
	for _, f := range d.Files {
		digests = append(digests, f.Digest)
	}
	for _, d := range d.Directories {
		digests = append(digests, d.Digest)
	}
	// If this directory has a pack associated, mark that too.
	if pack := rexclient.PackDigest(d); pack.Hash != "" {
		digests = append(digests, pack.ToProto())
	}
	return digests
}
