package worker

import (
	"os"
	"path"
	"strings"

	"github.com/peterebden/go-copyfile"
)

// A cache implements a filesystem-based blob cache.
// Since all the blobs are keyed by hash we don't have to worry about invalidation.
// In normal use the server never writes it; we prefill a selected artifact list offline.
type cache struct {
	root     string
	copier   copyfile.Copier
	prefixes []string
}

// newCache returns a new cache instance.
func newCache(root string, prefixes []string) *cache {
	return &cache{
		root:     root,
		prefixes: prefixes,
	}
}

// Retrieve copies a blob from the cache to the given location.
// It returns true if retrieved.
func (c *cache) Retrieve(key, dest string, mode os.FileMode) bool {
	src := c.path(key)
	if err := c.copier.LinkMode(src, dest, mode); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		log.Warning("Failed to retrieve %s from cache: %s", key, err)
		return false
	}
	return false
}

// Store stores a blob into the cache if it matches any of the prefixes.
func (c *cache) Store(dir, src, key string) {
	if c.shouldStore(dir, src) {
		c.store(src, key)
	}
}

// StoreAny stores any one of the given files if they match any of the prefixes.
func (c *cache) StoreAny(dir string, srcs []string, key string) {
	for _, src := range srcs {
		if c.shouldStore(dir, src) {
			c.store(src, key)
			return
		}
	}
}

// shouldStore returns true if the cache should store a file by this name.
func (c *cache) shouldStore(dir, src string) bool {
	src = strings.TrimLeft(strings.TrimPrefix(src, dir), "/")
	for _, p := range c.prefixes {
		if strings.HasPrefix(src, p) {
			return true
		}
	}
	return false
}

// store stores a single file into the cache.
func (c *cache) store(src, key string) {
	log.Debug("Storing blob %s (from %s) in local cache", key, src)
	dest := c.path(key)
	if _, err := os.Stat(dest); err == nil {
		log.Debug("Artifact %s already exists in cache, not storing", key)
	} else if err := os.MkdirAll(path.Dir(dest), os.ModeDir|0755); err != nil {
		log.Warning("Failed to store %s in cache: %s", src, err)
	} else if err := c.copier.LinkMode(src, dest, 0555); err != nil {
		log.Warning("Failed to store %s in cache: %s", src, err)
	}
}

// path returns the file path for a cache item
func (c *cache) path(key string) string {
	// Prepend an intermediate directory of a couple of chars to make it a bit more explorable
	return path.Join(c.root, string([]byte{key[0], key[1]}), key)
}
