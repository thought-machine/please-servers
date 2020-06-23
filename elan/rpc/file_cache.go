package rpc

import (
	"context"
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
	if err := os.MkdirAll(root, 0775|os.ModeDir); err != nil {
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
// To abort the write to the file, cancel the passed context.
func (fc *fileCache) Set(ctx context.Context, key string, size int64) io.WriteCloser {
	filename := path.Join(fc.root, key)
	if !fc.cache.Set(key, filename, size) {
		log.Debug("Cache rejected write for %s [%d]", key, size)
		return discardCloser{}
	}
	if err := os.MkdirAll(path.Dir(filename), 0775|os.ModeDir); err != nil {
		log.Warning("Failed to create cache directory: %s", err)
		fc.cache.Del(key)
		return discardCloser{}
	}
	f, err := newAtomicFile(ctx, filename)
	if err != nil {
		log.Warning("Failed to create cache file: %s", err)
		fc.cache.Del(key)
		return discardCloser{}
	}
	log.Debug("Cache accepted write for %s [%d]", key, size)
	return f
}

func (fc *fileCache) OnEvict(key, conflict uint64, value interface{}, cost int64) {
	if s, ok := value.(string); ok {
		if err := os.Remove(s); err != nil {
			log.Warning("Failed to remove evicted file %s from cache: %s", value, err)
		}
	}
}

// Remove removes an item from this cache.
func (fc *fileCache) Remove(key string) {
	if err := os.RemoveAll(path.Join(fc.root, key)); err != nil {
		log.Warning("Failed to remove item from cache: %s", err)
	}
}

// A discardCloser is like ioutil's discard but also implements a no-op Close so it's a WriteCloser too.
type discardCloser struct{}

func (discardCloser) Write(p []byte) (int, error) { return len(p), nil }
func (discardCloser) Close() error                { return nil }

// An atomicFile wraps a file to do atomic writing; it writes to a temp file and moves on close.
// If its context is cancelled it aborts the write.
type atomicFile struct {
	ctx  context.Context
	f    *os.File
	name string
}

func newAtomicFile(ctx context.Context, filename string) (*atomicFile, error) {
	af := &atomicFile{
		ctx:  ctx,
		name: filename,
	}
	dir, file := path.Split(filename)
	f, err := ioutil.TempFile(dir, file+".tmp")
	if err != nil {
		return nil, err
	}
	af.f = f
	return af, nil
}

func (af *atomicFile) Write(buf []byte) (int, error) {
	return af.f.Write(buf)
}

func (af *atomicFile) Close() error {
	tmpfile := af.f.Name()
	if err := af.f.Close(); err != nil {
		log.Warning("Error closing cache file: %s", err)
		os.Remove(tmpfile)
		return err
	} else if err := af.ctx.Err(); err != nil { // Don't log this since it's a cancellation
		os.Remove(tmpfile)
		return err
	} else if err := os.Rename(tmpfile, af.name); err != nil {
		os.Remove(tmpfile)
		return err
	}
	return nil
}
