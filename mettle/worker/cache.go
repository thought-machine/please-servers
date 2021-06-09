package worker

import (
	"io"
	"os"
	"path"
	"path/filepath"
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
	parts    map[string]struct{}
}

// newCache returns a new cache instance.
func newCache(root string, prefixes, parts []string) *cache {
	c := &cache{
		root:     root,
		prefixes: prefixes,
		parts:    map[string]struct{}{},
	}
	for _, part := range parts {
		c.parts[part] = struct{}{}
	}
	return c
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

// RetrieveStream returns a file handle from the cache if it exists (or nil if not).
func (c *cache) RetrieveStream(key string) io.ReadCloser {
	f, err := os.Open(c.path(key))
	if err != nil {
		if !os.IsNotExist(err) {
			log.Warning("Failed to retrieve %s from cache: %s", key, err)
		}
		return nil
	}
	return f
}

// Store stores a blob into the cache if it matches any of the prefixes.
func (c *cache) Store(dir, src, key string) {
	if c.shouldStore(dir, src) {
		c.store(src, key)
	}
}

// StoreAny stores any one of the given files if they match any of the prefixes.
func (c *cache) StoreAny(dir string, files []fileNode, key string) {
	for _, file := range files {
		if c.shouldStore(dir, file.Name) {
			c.store(file.Name, key)
			return
		}
	}
}

// shouldStore returns true if the cache should store a file by this name.
func (c *cache) shouldStore(dir, src string) bool {
	if c == nil {
		return false
	}
	src = strings.TrimLeft(strings.TrimPrefix(src, dir), "/")
	for _, p := range c.prefixes {
		if strings.HasPrefix(src, p) {
			return true
		}
	}
	if len(c.parts) > 0 {
		for _, part := range strings.Split(src, string(filepath.Separator)) {
			if _, present := c.parts[part]; present {
				return true
			}
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
