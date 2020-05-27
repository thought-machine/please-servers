package worker

import (
	"context"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/peterebden/go-copyfile"
	"github.com/karrick/godirwalk"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tree"

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
	c := &Cache{root: root, src: src}
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
	log.Debug("checking cache for %s", src)
	if err := c.copyfunc(src, dest, mode); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		log.Warning("Failed to retrieve %s from cache: %s", key, err)
		return false
	} else if c.src == "" {
		return false
	}
	log.Debug("checking cache src %s", c.path(c.src, key))
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
func (c *Cache) StoreAll(instanceName string, targets []string, storage string, secureStorage bool, tokenFile string, minDiskSpace int64) error {
	w, err := initialiseWorker(instanceName, "mem://requests", "mem://responses", "cache", storage, c.root, "", "", "", "", "", tokenFile, false, secureStorage, true, 1 * time.Hour, 100, minDiskSpace)
	if err != nil {
		return err
	}
	log.Notice("Querying outputs for %s...", strings.Join(targets, " "))
	rclient := rpb.NewRecorderClient(w.client.CASConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Hour)
	defer cancel()
	resp, err := rclient.Query(ctx, &rpb.QueryRequest{
		InstanceName: instanceName,
		Queries:      targets,
	})
	if err != nil {
		return err
	}
	var mutex sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(resp.Digests))
	keep := map[string]int64{}
	exes := map[string]bool{}  // Tracks if any digests are executable as output from any action.
	log.Notice("Resolving outputs for %d actions...", len(resp.Digests))
	for _, digest := range resp.Digests {
		go func(digest *rpb.Digest) {
			defer wg.Done()
			if len(digest.Hash) != 64 {
				log.Errorf("Invalid hash: [%s]", digest.Hash)
				return
			}
			w.limiter <- struct{}{}
			defer func() { <-w.limiter }()
			outs, err := c.allOutputs(w.client, digest)
			if err != nil {
				log.Error("Error downloading outputs for %s: %s", digest.Hash, err)
				return
			}
			mutex.Lock()
			defer mutex.Unlock()
			for _, out := range outs {
				keep[out.Digest.Hash] = out.Digest.Size
				exes[out.Digest.Hash] = exes[out.Digest.Hash] || out.IsExecutable
			}
		}(digest)
	}
	wg.Wait()
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
	log.Notice("Reticulating splines...")
	fetch := map[string]*pb.FileNode{}
	var total int64
	for hash, size := range keep {
		if len(hash) != 64 {
			log.Errorf("Invalid hash: [%s]", hash)
			continue
		}
		if !exists[hash] {
			dest := c.path(c.root, hash)
			fetch[dest] = &pb.FileNode{
				Name:         hash,
				Digest:       &pb.Digest{Hash: hash, SizeBytes: size},
				IsExecutable: exes[hash],
			}
			if err := os.MkdirAll(path.Dir(dest), os.ModeDir | 0755); err != nil {
				log.Error("Failed to create directory: %s", err)
				return err
			}
			total += size
		}
	}
	log.Notice("Downloading %d files...", len(fetch))
	if err := w.downloadAllFiles(fetch); err != nil {
		log.Error("Failed to fetch some files: %s", err)
		return err
	}
	log.Notice("Downloads completed, total size: %s", humanize.Bytes(uint64(total)))
	return nil
}

// MustStoreAll is like StoreAll but dies on errors.
func (c *Cache) MustStoreAll(instanceName string, targets []string, storage string, secureStorage bool, tokenFile string, minDiskSpace int64) {
	if err := c.StoreAll(instanceName, targets, storage, secureStorage, tokenFile, minDiskSpace); err != nil {
		log.Fatalf("%s", err)
	}
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

// path returns the file path for a cache item
func (c *Cache) path(root, key string) string {
	// Prepend an intermediate directory of a couple of chars to make it a bit more explorable
	return path.Join(root, string([]byte{key[0], key[1]}), key)
}
