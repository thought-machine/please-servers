package worker

import (
	"context"
	"os"
	"path"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/peterebden/go-copyfile"
	"github.com/karrick/godirwalk"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	sdkdigest "github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"

	rpb "github.com/thought-machine/please-servers/proto/record"
)

// A Cache implements a filesystem-based blob cache.
// Since all the blobs are keyed by hash we don't have to worry about invalidation.
// In normal use the server never writes it; we prefill a selected artifact list offline.
type Cache struct {
	root string
	copier copyfile.Copier
}

// NewCache returns a new cache instance.
func NewCache(root string) *Cache {
	return &Cache{root: root}
}

// Retrieve copies a blob from the cache to the given location.
// It returns true if retrieved.
func (c *Cache) Retrieve(key, dest string, mode os.FileMode) bool {
	if err := os.Link(c.path(key), dest); err != nil {
		log.Warning("Failed to link %s -> %s: %s", c.path(key), dest, err)
	} else {
		return true
	}
	if err := c.copier.LinkMode(c.path(key), dest, mode); err != nil {
		if !os.IsNotExist(err) {
			log.Warning("Failed to retrieve %s from cache: %s", key, err)
		}
		return false
	}
	return true
}

// StoreAll reads the given file and stores all the blobs it finds into the cache.
func (c *Cache) StoreAll(instanceName string, targets []string, storage string, secureStorage bool) error {
	log.Notice("Dialling remote %s...", storage)
	client, err := client.NewClient(context.Background(), instanceName, client.DialParams{
		Service:            storage,
		NoSecurity:         !secureStorage,
		TransportCredsOnly: secureStorage,
	}, client.RetryTransient())
	if err != nil {
		return err
	}
	log.Notice("Querying outputs for %s...", strings.Join(targets, " "))
	rclient := rpb.NewRecorderClient(client.CASConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Minute)
	defer cancel()
	resp, err := rclient.Query(ctx, &rpb.QueryRequest{
		InstanceName: instanceName,
		Queries:      targets,
	})
	if err != nil {
		return err
	}
	keep := map[string]int64{}
	for _, digest := range resp.Digests {
		digests, err := c.allOutputs(client, digest)
		if err != nil {
			log.Error("Error downloading outputs for %s: %s", digest.Hash, err)
			continue
		}
		for _, digest := range digests {
			keep[digest.Hash] = digest.Size
		}
	}
	log.Notice("Removing extraneous artifacts...")
	exists := map[string]bool{}
	if _, err := os.Stat(c.root); err != nil {
		log.Warning("Cannot stat %s, will not check for existing artifacts: %s", c.root, err)
	} else {
		removed := 0
		if err := godirwalk.Walk(c.root, &godirwalk.Options{Callback: func(pathname string, entry *godirwalk.Dirent) error {
			if !entry.IsDir() {
				if _, present := keep[path.Base(pathname)]; !present {
					removed++
					return os.Remove(pathname)
				}
				exists[path.Base(pathname)] = true
			}
			return nil
		}}); err != nil {
			return err
		}
		log.Notice("Removed %d extraneous entries", removed)
	}
	fetch := map[string]int64{}
	for hash, size := range keep {
		if !exists[hash] {
			fetch[hash] = size
		}
	}
	var total int64
	i := 0
	for hash, size := range fetch {
		i++
		total += size
		if exists[hash] {
			log.Debug("Not re-downloading %s...", hash)
			continue
		}
		if i % 10 == 0 {
			log.Notice("Downloading artifact %d of %d...", i, len(fetch))
		}
		if err := c.storeOne(client, hash, size); err != nil {
			log.Error("Error downloading %s: %s", hash, err)
		}
	}
	log.Notice("Downloads completed, total size: %s", humanize.Bytes(uint64(total)))
	return nil
}

// MustStoreAll is like StoreAll but dies on errors.
func (c *Cache) MustStoreAll(instanceName string, targets []string, storage string, secureStorage bool) {
	if err := c.StoreAll(instanceName, targets, storage, secureStorage); err != nil {
		log.Fatalf("%s", err)
	}
}

func (c *Cache) allOutputs(client *client.Client, digest *rpb.Digest) ([]sdkdigest.Digest, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Minute)
	defer cancel()
	ar, err := client.CheckActionCache(ctx, &pb.Digest{Hash: digest.Hash, SizeBytes: digest.SizeBytes})
	if err != nil {
		return nil, err
	}
	outs, err := client.FlattenActionOutputs(ctx, ar)
	ret := make([]sdkdigest.Digest, len(outs))
	for _, out := range outs {
		ret = append(ret, out.Digest)
	}
	return ret, err
}

func (c *Cache) storeOne(client *client.Client, hash string, size int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Minute)
	defer cancel()
	digest, err := sdkdigest.New(hash, size)
	if err != nil {
		return err
	}
	out := c.path(hash)
	tmp := out + ".tmp"
	if err := os.MkdirAll(path.Dir(out), os.ModeDir|0755); err != nil {
		return err
	} else if _, err := client.ReadBlobToFile(ctx, digest, tmp); err != nil {
		return err
	}
	return os.Rename(tmp, out)
}

// path returns the file path for a cache item
func (c *Cache) path(key string) string {
	// Prepend an intermediate directory of a couple of chars to make it a bit more explorable
	return path.Join(c.root, string([]byte{key[0], key[1]}), key)
}
