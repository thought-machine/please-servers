package rpc

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/dgraph-io/ristretto"
)

// A fileCache manages a set of blobs in a directory.
// It uses Ristretto to manage the set of blobs to keep which minimises the amount of logic needed here.
type fileCache struct {
	cache *ristretto.Cache
	root  string
}

// newFileCache constructs a new cache.
// It loads the existing set of artifacts currently in the cache, which may take some time if a significant
// number are already present.
func newFileCache(root string, maxSize int64) (*fileCache, error) {
	fc := &fileCache{root: root}
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: maxSize / 10,
		MaxCost:     maxSize,
		BufferItems: 64,
		OnEvict:     fc.OnEvict,
	})
	if err != nil {
		return nil, err
	}
	log.Notice("Initialising file cache...")
	defer func() { log.Notice("Initialised file cache") }()
	if err := os.MkdirAll(root, 0775 | os.ModeDir); err != nil {
		return nil, fmt.Errorf("Failed to create cache directory: %s", err)
	}
	fc.cache = c
	return fc, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		} else if key, err := filepath.Rel(root, path); err != nil {
			log.Error("Failed to make path relative: %s", err)
		} else if c.Set(key, path, info.Size()) {
			log.Debug("Added existing item %s to cache", key)
		} else {
			// Warning because this shouldn't happen (unless you're initialising with a smaller cache size than before)
			log.Warning("Discarded pre-existing item %s", key)
		}
		return nil
	})
}

// Get returns an item from the cache. It returns nil if the item is not contained.
func (fc *fileCache) Get(key string) io.ReadCloser {
	filename, present := fc.cache.Get(key)
	if !present {
		return nil
	}
	f, err := os.Open(filename.(string))
	if err != nil {
		log.Warning("Failed to open item we thought was in the cache: %s", err)
		fc.cache.Del(key)
		return nil
	}
	return f
}

// GetAll is like Get but reads all the bytes of the item.
func (fc *fileCache) GetAll(key string) []byte {
	filename, present := fc.cache.Get(key)
	if !present {
		return nil
	}
	b, err := ioutil.ReadFile(filename.(string))
	if err != nil {
		log.Warning("Failed to read an item from the cache: %s", err)
		os.Remove(filename.(string))
		fc.cache.Del(key)
		return nil
	}
	return b
}

// Set sets the given item into the cache. It returns an io.WriteCloser to write to, which is never nil
// (although this makes no promise that future calls to Get will or won't retrieve this item).
func (fc *fileCache) Set(key string, size int64) io.WriteCloser {
	filename := path.Join(fc.root, key)
	if !fc.cache.Set(key, filename, size) {
		log.Debug("Cache rejected write for %s [%d]", key, size)
		return discardCloser{}
	}
	if err := os.MkdirAll(path.Dir(filename), 0775 | os.ModeDir); err != nil {
		log.Warning("Failed to create cache directory: %s", err)
		fc.cache.Del(key)
		return discardCloser{}
	}
	f, err := os.Create(filename)
	if err != nil {
		log.Warning("Failed to create cache file: %s", err)
		fc.cache.Del(key)
		return discardCloser{}
	}
	log.Debug("Cache accepted write for %s [%d]", key, size)
	return f
}

// SetAll sets the entirety of a given blob into the cache.
func (fc *fileCache) SetAll(key string, size int64, contents []byte) {
	w := fc.Set(key, size)
	defer w.Close()
	if _, err := io.Copy(w, bytes.NewReader(contents)); err != nil {
		log.Warning("Failed to write file into cache: %s", err)
		fc.cache.Del(key)
	}
}

func (fc *fileCache) OnEvict(key, conflict uint64, value interface{}, cost int64) {
	if err := os.Remove(value.(string)); err != nil {
		log.Warning("Failed to remove evicted file %s from cache: %s", value, err)
	}
}

// A discardCloser is like ioutil's discard but also implements a no-op Close so it's a WriteCloser too.
type discardCloser struct {}

func (discardCloser) Write(p []byte) (int, error) { return len(p), nil }
func (discardCloser) Close() error { return nil }
