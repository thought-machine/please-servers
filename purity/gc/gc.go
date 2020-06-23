// Package gc implements the garbage collection logic for Purity.
package gc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/hashicorp/go-multierror"

	"github.com/thought-machine/please-servers/grpcutil"
	ppb "github.com/thought-machine/please-servers/proto/purity"
)

// RunForever runs indefinitely, periodically hitting the remote server and possibly GC'ing it.
func RunForever(proxy string, urls []string, tokenFile string, tls bool, minAge, frequency time.Duration, proportion float64, dryRun bool) {
	for range time.NewTicker(frequency).C {
		if err := Run(proxy, urls, tokenFile, tls, minAge, proportion, dryRun); err != nil {
			log.Error("Failed to GC: %s", err)
		}
	}
}

// Run runs once against the remote servers and triggers a GC if needed.
func Run(proxy, urls []string, instanceName, tokenFile string, tls bool, minAge time.Duration, proportion float64, dryRun bool) error {
	gc, err := newCollector(proxy, urls, instanceName, tokenFile, tls, dryRun, minAge)
	if err != nil {
		return err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return err
	} else if err := MarkReferencedBlobs(); err != nil {
		return err
	} else if err := gc.RemoveBlobs(); err != nil {
		return err
	}
	log.Notice("Complete!")
	return nil
}

type collector struct {
	client          *client.Client
	clients         []ppb.GCClient
	actionResults   []*ppb.ActionResult
	allBlobs        []*ppb.Blob
	referencedBlobs map[string]struct{}
	mutex           sync.Mutex
	ageThreshold    int64
	dryRun          bool
}

func newCollector(proxy, urls []string, instanceName, tokenFile string, tls, dryRun bool, minAge time.Duration) (*collector, error) {
	log.Notice("Dialling remotes...")
	client, err := client.NewClient(context.Background(), instanceName, client.DialParams{
		Service:            proxy,
		NoSecurity:         !tls,
		TransportCredsOnly: tls,
		DialOpts:           grpcutil.DialOptions(tokenFile),
	}, client.UseBatchOps(true), client.RetryTransient())
	if err != nil {
		return nil, err
	}
	c := &collector{
		client:          client,
		clients:         make([]ppb.GCClient, len(urls)),
		dryRun:          dryRun,
		referencedBlobs: map[string]struct{}{},
		ageThreshold:    time.Now().Add(-minAge).Unix(),
	}
	for i, url := range urls {
		conn, err := grpcutil.Dial(url, tls, "", tokenFile)
		if err != nil {
			return nil, err
		}
		c.clients[i] = ppb.NewGCClient(conn)
	}
	return c, nil
}

func (c *collector) LoadAllBlobs() error {
	log.Notice("Receiving current list of items...")
	var g multierror.Group
	for _, client := range c.clients {
		client := client
		g.Go(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			resp, err := client.List(ctx, &ppb.ListRequest{})
			if err != nil {
				return fmt.Errorf("Failed to list blobs: %s", err)
			}
			c.mutex.Lock()
			defer c.mutex.Unlock()
			c.actionResults = append(c.actionResults, resp.ActionResults...)
			c.allBlobs = append(c.allBlobs, resp.Blobs)
		})
	}
	return g.Wait()
}

func (c *collector) MarkReferencedBlobs() error {
	// Get a little bit of parallelism here, but not too much.
	const parallelism = 4
	log.Notice("Finding referenced blobs...")
	var wg sync.WaitGroup
	wg.Add(parallelism)
	step := len(c.actionResults) / parallelism
	var done int64
	for i := 0; i < parallelism; i++ {
		go func(ars []*ppb.ActionResult) {
			for _, ar := range ars {
				if c.shouldDelete(ar) {
					if err := c.markReferencedBlobs(ar); err != nil {
						// Not fatal otherwise one bad action result will stop the whole show.
						log.Warning("Failed to find referenced blobs for %s: %s", ar.Hash, err)
					}
				}
				if atomic.AddInt64(&done, 1)%100 == 0 {
					log.Notice("Checked %d of %d action results", done, len(c.actionResults))
				}
			}
			wg.Done()
		}(c.actionResults[step*i : step*(i+1)])
	}
	wg.Wait()
}

func (c *collector) markReferencedBlobs(ar *ppb.ActionResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	result, err := c.client.GetActionResult(ctx, &pb.GetActionResultRequest{
		InstanceName: c.client.InstanceName,
		ActionDigest: &pb.Digest{Hash: ar.Hash, SizeBytes: ar.SizeBytes},
	})
	if err != nil {
		return fmt.Errorf("Couldn't download action result for %s: %s", ar.Hash, err)
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, f := range ar.OutputFiles {
		c.referencedBlobs[f.Digest.Hash] = struct{}{}
	}
	for _, d := range ar.OutputDirectories {
		c.markDirectory(d.Root)
		for _, child := range d.Children {
			c.markDirectory(child)
		}
	}
	// N.B. we do not mark the original action or its sources, those are irrelevant to us
	//      (unless they are also referenced as the output of something else).
	return nil
}

func (c *collector) markDirectory(d *pb.Directory) {
	for _, f := range d.Files {
		c.referencedBlobs[f.Digest.Hash] = struct{}{}
	}
	for _, d := range d.Directories {
		c.referencedBlobs[d.Digest.Hash] = struct{}{}
	}
}

func (c *collector) RemoveBlobs() error {
	log.Notice("Determining blobs to remove...")
	req := &ppb.DeleteRequest{}
	var size int64
	for _, ar := range c.actionResults {
		if c.shouldDelete(ar) {
			req.ActionResults = append(req.ActionResults, &ppb.Blob{Hash: ar.Hash, SizeBytes: ar.SizeBytes})
			size += ar.SizeBytes
		}
	}
	for _, blob := range c.allBlobs {
		if _, present := c.referencedBlobs[blob.Hash]; !present {
			req.Blobs = append(req.Blobs, blob)
			size += blob.SizeBytes
		}
	}
	if c.dryRun {
		log.Notice("Would delete %d action results and %d blobs, total size %d bytes", len(req.ActionResults), len(req.Blobs), size)
	}
	log.Notice("Deleting %d action results and %d blobs, total size %d bytes", len(req.ActionResults), len(req.Blobs), size)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var g multierror.Group
	for _, client := range c.clients {
		client := client
		g.Go(func() error {
			_, err := client.Delete(ctx, req)
			return err
		})
	}
	return g.Wait()
}

func (c *collector) shouldDelete(ar *ppb.ActionResult) bool {
	return ar.LastAccessed < c.ageThreshold
}
