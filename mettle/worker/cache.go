package worker

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/peterebden/go-copyfile"
	"github.com/karrick/godirwalk"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tree"
	sdkdigest "github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"

	"github.com/thought-machine/please-servers/grpcutil"
	rpb "github.com/thought-machine/please-servers/proto/record"
)

// A Cache implements a filesystem-based blob cache.
// Since all the blobs are keyed by hash we don't have to worry about invalidation.
// In normal use the server never writes it; we prefill a selected artifact list offline.
type Cache struct {
	root, src string
	copier copyfile.Copier
	copyfunc func(string, string, os.FileMode) error
}

// NewCache returns a new cache instance.
func NewCache(root, src string, copy bool) *Cache {
	c := &Cache{root: root}
	if copy {
		c.copyfunc = c.copier.CopyMode
	}  else {
		c.copyfunc = c.copier.LinkMode
	}
	return c
}

// Retrieve copies a blob from the cache to the given location.
// It returns true if retrieved.
func (c *Cache) Retrieve(key, dest string, mode os.FileMode) bool {
	src := c.path(c.root, key)
	if err := c.copyfunc(src, dest, mode); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		log.Warning("Failed to retrieve %s from cache: %s", key, err)
		return false
	} else if c.src == "" {
		return false
	}
	// We can try retrieving from our source directory.
	if err := c.copyfunc(c.path(c.src, key), src, mode); err != nil {
		if !os.IsNotExist(err) {
			log.Warning("Failed to retrieve %s from cache source: %s", key, err)
		}
		return false
	} else if err := c.copyfunc(src, dest, mode); err != nil {
		log.Warning("Failed to retrieve %s from cache source: %s", key, err)
		return false
	}
	return true
}

// StoreAll reads the given file and stores all the blobs it finds into the cache.
func (c *Cache) StoreAll(instanceName string, targets []string, storage string, secureStorage bool, tokenFile string) error {
	log.Notice("Dialling remote %s...", storage)
	client, err := client.NewClient(context.Background(), instanceName, client.DialParams{
		Service:            storage,
		NoSecurity:         !secureStorage,
		TransportCredsOnly: secureStorage,
		DialOpts:           grpcutil.DialOptions(tokenFile),
	}, client.RetryTransient())
	if err != nil {
		return err
	}
	log.Notice("Querying outputs for %s...", strings.Join(targets, " "))
	rclient := rpb.NewRecorderClient(client.CASConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Hour)
	defer cancel()
	resp, err := rclient.Query(ctx, &rpb.QueryRequest{
		InstanceName: instanceName,
		Queries:      targets,
	})
	if err != nil {
		return err
	}
	keep := map[string]int64{}
	exes := map[string]bool{}  // Tracks if any digests are executable as output from any action.
	for _, digest := range resp.Digests {
		outs, err := c.allOutputs(client, digest)
		if err != nil {
			log.Error("Error downloading outputs for %s: %s", digest.Hash, err)
			continue
		}
		for _, out := range outs {
			keep[out.Digest.Hash] = out.Digest.Size
			exes[out.Digest.Hash] = exes[out.Digest.Hash] || out.IsExecutable
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
				base := path.Base(pathname)
				if _, present := keep[base]; !present {
					removed++
					return os.Remove(pathname)
				} else if exes[base] {
					// Ensure this file is executable if needed.
					if err := os.Chmod(pathname, fileMode(true)); err != nil {
						return err
					}
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
		if err := c.storeOne(client, hash, size, exes[hash]); err != nil {
			log.Error("Error downloading %s: %s", hash, err)
		}
	}
	log.Notice("Downloads completed, total size: %s", humanize.Bytes(uint64(total)))
	return nil
}

// MustStoreAll is like StoreAll but dies on errors.
func (c *Cache) MustStoreAll(instanceName string, targets []string, storage string, secureStorage bool, tokenFile string) {
	if err := c.StoreAll(instanceName, targets, storage, secureStorage, tokenFile); err != nil {
		log.Fatalf("%s", err)
	}
}

// CopyTo copies the entire contents of the cache dir to a destination.
func (c *Cache) CopyTo(destination string) error {
	log.Notice("Copying cached content from %s to %s...", c.root, destination)
	return godirwalk.Walk(c.root, &godirwalk.Options{Callback: func(pathname string, entry *godirwalk.Dirent) error {
		dest := path.Join(destination, strings.TrimLeft(strings.TrimPrefix(pathname, c.root), "/"))
		if entry.IsDir() {
			if entry.Name() == "lost+found" {  // Bit of a hack to skip unreadable directories
				return filepath.SkipDir
			}
			return os.MkdirAll(dest, 0755 | os.ModeDir)
		} else if err := c.copyfunc(pathname, dest, 0755); err != nil {
			log.Warning("Failed to copy cache file: %s", err)  // Don't fail on this.
		}
		return nil
	}})
}

func (c *Cache) allOutputs(client *client.Client, digest *rpb.Digest) (map[string]*tree.Output, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Minute)
	defer cancel()
	ar, err := client.CheckActionCache(ctx, &pb.Digest{Hash: digest.Hash, SizeBytes: digest.SizeBytes})
	if err != nil {
		return nil, err
	}
	return client.FlattenActionOutputs(ctx, ar)
}

func (c *Cache) storeOne(client *client.Client, hash string, size int64, isExecutable bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Minute)
	defer cancel()
	digest, err := sdkdigest.New(hash, size)
	if err != nil {
		return err
	}
	out := c.path(c.root, hash)
	tmp := out + ".tmp"
	if err := os.MkdirAll(path.Dir(out), os.ModeDir|0755); err != nil {
		return err
	} else if _, err := client.ReadBlobToFile(ctx, digest, tmp); err != nil {
		return err
	} else if err := os.Chmod(tmp, fileMode(isExecutable)); err != nil {
		return err
	}
	return os.Rename(tmp, out)
}

// path returns the file path for a cache item
func (c *Cache) path(root, key string) string {
	// Prepend an intermediate directory of a couple of chars to make it a bit more explorable
	return path.Join(root, string([]byte{key[0], key[1]}), key)
}
