// Package gc implements the garbage collection logic for Purity.
package gc

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/dustin/go-humanize"
	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/hashicorp/go-multierror"
	"github.com/peterebden/go-cli-init/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/thought-machine/please-servers/grpcutil"
	ppb "github.com/thought-machine/please-servers/proto/purity"
)

var log = cli.MustGetLogger()

// RunForever runs indefinitely, periodically hitting the remote server and possibly GC'ing it.
func RunForever(url, instanceName, tokenFile string, tls bool, minAge, frequency time.Duration) {
	for range time.NewTicker(frequency).C {
		if err := Run(url, instanceName, tokenFile, tls, minAge, false); err != nil {
			log.Error("Failed to GC: %s", err)
		}
	}
}

// Run runs once against the remote servers and triggers a GC if needed.
func Run(url, instanceName, tokenFile string, tls bool, minAge time.Duration, dryRun bool) error {
	gc, err := newCollector(url, instanceName, tokenFile, tls, dryRun, minAge)
	if err != nil {
		return err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return err
	} else if err := gc.MarkReferencedBlobs(); err != nil {
		return err
	} else if err := gc.RemoveBlobs(); err != nil {
		return err
	}
	log.Notice("Complete!")
	return nil
}

// Delete deletes a series of build actions from the remote server.
func Delete(url, instanceName, tokenFile string, tls bool, hashes []string) error {
	gc, err := newCollector(url, instanceName, tokenFile, tls, false, 0)
	if err != nil {
		return err
	}
	return gc.RemoveSpecificBlobs(hashes)
}

// Clean cleans any build actions referencing missing blobs from the server.
func Clean(url, instanceName, tokenFile string, tls, dryRun bool) error {
	gc, err := newCollector(url, instanceName, tokenFile, tls, dryRun, 1000000*time.Hour)
	if err != nil {
		return err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return err
	} else if err := gc.MarkReferencedBlobs(); err != nil {
		return err
	}
	return gc.RemoveBrokenBlobs()
}

// Sizes returns the sizes of the top N actions.
func Sizes(url, instanceName, tokenFile string, tls bool, n int) ([]Action, error) {
	gc, err := newCollector(url, instanceName, tokenFile, tls, true, 1000000*time.Hour)
	if err != nil {
		return nil, err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return nil, err
	} else if err := gc.MarkReferencedBlobs(); err != nil {
		return nil, err
	}
	return gc.Sizes(n), nil
}

// An Action is a convenience type returned from Sizes.
type Action struct {
	pb.Digest
	InputSize, OutputSize int
}

type collector struct {
	client          *client.Client
	gcclient        ppb.GCClient
	actionResults   []*ppb.ActionResult
	allBlobs        map[string]int64
	referencedBlobs map[string]struct{}
	brokenResults   map[string]struct{}
	inputSizes      map[string]int
	outputSizes     map[string]int
	mutex           sync.Mutex
	ageThreshold    int64
	missingInputs   int64
	dryRun          bool
}

func newCollector(url, instanceName, tokenFile string, tls, dryRun bool, minAge time.Duration) (*collector, error) {
	log.Notice("Dialling remote...")
	client, err := client.NewClient(context.Background(), instanceName, client.DialParams{
		Service:            url,
		NoSecurity:         !tls,
		TransportCredsOnly: tls,
		DialOpts:           append(grpcutil.DialOptions(tokenFile), grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor())),
	}, client.UseBatchOps(true), client.RetryTransient())
	if err != nil {
		return nil, err
	}
	return &collector{
		client:          client,
		gcclient:        ppb.NewGCClient(client.Connection),
		dryRun:          dryRun,
		allBlobs:        map[string]int64{},
		referencedBlobs: map[string]struct{}{},
		brokenResults:   map[string]struct{}{},
		inputSizes:      map[string]int{},
		outputSizes:     map[string]int{},
		ageThreshold:    time.Now().Add(-minAge).Unix(),
	}, nil
}

func (c *collector) LoadAllBlobs() error {
	log.Notice("Receiving current list of items...")
	ch := newProgressBar("Enumerating blobs", 256)
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond) // obviously yuck but helps the progress bar get out of the way
		log.Notice("Received %d action results and %d blobs", len(c.actionResults), len(c.allBlobs))
	}()
	var g multierror.Group
	var mutex sync.Mutex
	for i := 0; i < 16; i++ {
		i := i
		g.Go(func() error {
			for j := 0; j < 16; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				defer cancel()
				resp, err := c.gcclient.List(ctx, &ppb.ListRequest{
					Prefix: hex.EncodeToString([]byte{byte(i*16 + j)}),
				})
				if err != nil {
					return err
				}
				mutex.Lock()
				c.actionResults = append(c.actionResults, resp.ActionResults...)
				for _, b := range resp.Blobs {
					c.allBlobs[b.Hash] = b.SizeBytes
				}
				mutex.Unlock()
				ch <- 1
			}
			return nil
		})
	}
	return g.Wait().ErrorOrNil()
}

func (c *collector) MarkReferencedBlobs() error {
	// Get a little bit of parallelism here, but not too much.
	const parallelism = 16
	log.Notice("Finding referenced blobs...")
	ch := newProgressBar("Checking action results", len(c.actionResults))
	var live int64
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
		log.Notice("Found %d live action results and %d referenced blobs", live, len(c.referencedBlobs))
		if c.missingInputs > 0 {
			log.Warning("Missing inputs for %d action results", c.missingInputs)
		}
	}()
	var wg sync.WaitGroup
	wg.Add(parallelism)
	step := len(c.actionResults) / parallelism
	for i := 0; i < parallelism; i++ {
		go func(ars []*ppb.ActionResult) {
			for _, ar := range ars {
				if !c.shouldDelete(ar) {
					if err := c.markReferencedBlobs(ar); err != nil {
						// Not fatal otherwise one bad action result will stop the whole show.
						log.Warning("Failed to find referenced blobs for %s: %s", ar.Hash, err)
						c.markBroken(ar.Hash)
					}
					atomic.AddInt64(&live, 1)
				}
				ch <- 1
			}
			wg.Done()
		}(c.actionResults[step*i : step*(i+1)])
	}
	wg.Wait()
	return nil
}

func (c *collector) markReferencedBlobs(ar *ppb.ActionResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, grpcutil.GCKey, "true")
	dg := &pb.Digest{Hash: ar.Hash, SizeBytes: ar.SizeBytes}
	result, err := c.client.GetActionResult(ctx, &pb.GetActionResultRequest{
		InstanceName: c.client.InstanceName,
		ActionDigest: dg,
	})
	if err != nil {
		return fmt.Errorf("Couldn't download action result for %s: %s", ar.Hash, err)
	}
	size := ar.SizeBytes
	digests := []*pb.Digest{}
	for _, d := range result.OutputDirectories {
		sz, dgs, err := c.markTree(d)
		if err != nil {
			log.Warning("Couldn't download output tree for %s: %s", ar.Hash, err)
		}
		size += sz
		digests = append(digests, dgs...)
	}
	for _, f := range result.OutputFiles {
		digests = append(digests, f.Digest)
	}
	// Check whether all these outputs exist.
	resp, err := c.client.FindMissingBlobs(context.Background(), &pb.FindMissingBlobsRequest{
		InstanceName: c.client.InstanceName,
		BlobDigests:  digests,
	})
	if err != nil {
		log.Warning("Failed to check blob digests for %s: %s", ar.Hash, err)
	}
	// Mark all the inputs as well. There are some fringe cases that make things awkward
	// and it means things look more sensible in the browser.
	inputSize, dirs := c.inputDirs(dg)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.referencedBlobs[ar.Hash] = struct{}{}
	for _, f := range result.OutputFiles {
		c.referencedBlobs[f.Digest.Hash] = struct{}{}
		size += f.Digest.SizeBytes
	}
	for _, d := range dirs {
		c.markDirectory(d)
	}
	if result.StdoutDigest != nil {
		c.referencedBlobs[result.StdoutDigest.Hash] = struct{}{}
	}
	if result.StderrDigest != nil {
		c.referencedBlobs[result.StderrDigest.Hash] = struct{}{}
	}
	c.inputSizes[ar.Hash] = int(inputSize)
	c.outputSizes[ar.Hash] = int(size)
	if len(resp.MissingBlobDigests) > 0 {
		return fmt.Errorf("Action result %s is missing %d digests", ar.Hash, len(resp.MissingBlobDigests))
	}
	return nil
}

// inputDirs returns all the inputs for an action. It doesn't return any errors because we don't
// want it to be fatal on failure; otherwise anything missing breaks the whole process.
func (c *collector) inputDirs(dg *pb.Digest) (int64, []*pb.Directory) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	action := &pb.Action{}
	var size int64
	digestSize, present := c.allBlobs[dg.Hash]
	if !present {
		log.Debug("missing action for %s", dg.Hash)
		atomic.AddInt64(&c.missingInputs, 1)
		return size, nil
	}
	if err := c.client.ReadProto(ctx, digest.Digest{
		Hash: dg.Hash,
		Size: digestSize,
	}, action); err != nil {
		log.Debug("Failed to read action %s: %s", dg.Hash, err)
		atomic.AddInt64(&c.missingInputs, 1)
		return size, nil
	}
	size += dg.SizeBytes
	if action.InputRootDigest == nil {
		log.Debug("nil input root for %s", dg.Hash)
		atomic.AddInt64(&c.missingInputs, 1)
		return size, nil
	}
	size += action.InputRootDigest.SizeBytes
	dirs, err := c.client.GetDirectoryTree(ctx, action.InputRootDigest)
	if err != nil {
		log.Debug("Failed to read directory tree for %s (input root %s): %s", dg.Hash, action.InputRootDigest, err)
		atomic.AddInt64(&c.missingInputs, 1)
		return size, nil
	}
	for _, dir := range dirs {
		for _, d := range dir.Directories {
			size += d.Digest.SizeBytes
		}
		for _, f := range dir.Files {
			size += f.Digest.SizeBytes
		}
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.referencedBlobs[action.CommandDigest.Hash] = struct{}{}
	c.referencedBlobs[dg.Hash] = struct{}{}
	c.referencedBlobs[action.InputRootDigest.Hash] = struct{}{}
	return size, dirs
}

// markBroken marks an action result as missing some relevant files.
func (c *collector) markBroken(hash string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.brokenResults[hash] = struct{}{}
}

func (c *collector) markTree(d *pb.OutputDirectory) (int64, []*pb.Digest, error) {
	tree := &pb.Tree{}
	if err := c.client.ReadProto(context.Background(), digest.NewFromProtoUnvalidated(d.TreeDigest), tree); err != nil {
		return 0, nil, err
	}
	// Here we attempt to fix up any outputs that don't also have the input facet.
	// This is an incredibly sucky part of the API; the output doesn't contain some of the blobs
	// that will get used on the input facet, so it's really nonobvious where they come from.
	c.checkDirectories(append(tree.Children, tree.Root))

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.referencedBlobs[d.TreeDigest.Hash] = struct{}{}
	size, digests := c.markDirectory(tree.Root)
	for _, child := range tree.Children {
		s2, d2 := c.markDirectory(child)
		size += s2
		digests = append(digests, d2...)
	}
	return size, digests, nil
}

func (c *collector) markDirectory(d *pb.Directory) (int64, []*pb.Digest) {
	var size int64
	digests := []*pb.Digest{}
	for _, f := range d.Files {
		c.referencedBlobs[f.Digest.Hash] = struct{}{}
		size += f.Digest.SizeBytes
		digests = append(digests, f.Digest)
	}
	for _, d := range d.Directories {
		c.referencedBlobs[d.Digest.Hash] = struct{}{}
		size += d.Digest.SizeBytes
		digests = append(digests, d.Digest)
	}
	return size, digests
}

// checkDirectory checks that the directory protos from a Tree still exist in the CAS and uploads it if not.
func (c *collector) checkDirectories(dirs []*pb.Directory) {
	chunkers := make([]*chunker.Chunker, len(dirs))
	for i, d := range dirs {
		chomk, _ := chunker.NewFromProto(d, int(c.client.ChunkMaxSize))
		chunkers[i] = chomk
	}
	if !c.dryRun {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		if err := c.client.UploadIfMissing(ctx, chunkers...); err != nil {
			log.Warning("Failed to upload missing directory protos: %s", err)
		}
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, chomk := range chunkers {
		c.referencedBlobs[chomk.Digest().Hash] = struct{}{}
	}
}

func (c *collector) RemoveBlobs() error {
	log.Notice("Determining blobs to remove...")
	blobs := map[string][]*ppb.Blob{}
	ars := map[string][]*ppb.Blob{}
	numArs := 0
	numBlobs := 0
	var totalSize int64
	for _, ar := range c.actionResults {
		if c.shouldDelete(ar) {
			key := ar.Hash[:2]
			ars[key] = append(ars[key], &ppb.Blob{Hash: ar.Hash, SizeBytes: ar.SizeBytes})
			totalSize += ar.SizeBytes
			numArs++
		}
	}
	for hash, size := range c.allBlobs {
		if _, present := c.referencedBlobs[hash]; !present {
			key := hash[:2]
			blobs[key] = append(blobs[key], &ppb.Blob{Hash: hash, SizeBytes: size})
			totalSize += size
			numBlobs++
		}
	}
	if c.dryRun {
		log.Notice("Would delete %d action results and %d blobs, total size %s", numArs, numBlobs, humanize.Bytes(uint64(totalSize)))
		return nil
	}
	log.Notice("Deleting %d action results and %d blobs, total size %s", numArs, numBlobs, humanize.Bytes(uint64(totalSize)))
	ch := newProgressBar("Deleting blobs", 256)
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
	}()
	var g multierror.Group
	for i := 0; i < 16; i++ {
		i := i
		g.Go(func() error {
			for j := 0; j < 16; j++ {
				key := hex.EncodeToString([]byte{byte(i*16 + j)})
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				defer cancel()
				_, err := c.gcclient.Delete(ctx, &ppb.DeleteRequest{
					Prefix:        key,
					Blobs:         blobs[key],
					ActionResults: ars[key],
				})
				if err != nil {
					return err
				}
				ch <- 1
			}
			return nil
		})
	}
	return g.Wait().ErrorOrNil()
}

func (c *collector) shouldDelete(ar *ppb.ActionResult) bool {
	return ar.LastAccessed < c.ageThreshold || len(ar.Hash) != 64
}

func (c *collector) RemoveSpecificBlobs(hashes []string) error {
	if c.dryRun {
		log.Notice("Would remove %d actions", len(hashes))
		return nil
	} else if len(hashes) == 0 {
		log.Notice("Nothing to do")
		return nil
	}
	ch := newProgressBar("Deleting actions", len(hashes))
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
		log.Notice("Deleted %d action results", len(hashes))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, hash := range hashes {
		if _, err := c.gcclient.Delete(ctx, &ppb.DeleteRequest{
			Prefix:        hash[:2],
			ActionResults: []*ppb.Blob{&ppb.Blob{Hash: hash}},
		}); err != nil {
			return err
		}
		ch <- 1
	}
	return nil
}

// RemoveBrokenBlobs removes any blobs previously marked as broken.
func (c *collector) RemoveBrokenBlobs() error {
	hashes := make([]string, 0, len(c.brokenResults))
	for h := range c.brokenResults {
		hashes = append(hashes, h)
	}
	return c.RemoveSpecificBlobs(hashes)
}

// Sizes returns the sizes of the top n biggest actions.
func (c *collector) Sizes(n int) []Action {
	ret := make([]Action, len(c.actionResults))
	for i, ar := range c.actionResults {
		ret[i] = Action{
			Digest: pb.Digest{
				Hash:      ar.Hash,
				SizeBytes: c.allBlobs[ar.Hash],
			},
			InputSize:  c.inputSizes[ar.Hash],
			OutputSize: c.outputSizes[ar.Hash],
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].InputSize+ret[i].OutputSize > ret[j].InputSize+ret[j].OutputSize
	})
	if len(ret) > n {
		return ret[:n]
	}
	return ret
}
