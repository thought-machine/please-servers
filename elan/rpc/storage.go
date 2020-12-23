package rpc

import (
	"context"
	"io"
	"io/ioutil"

	"github.com/klauspost/compress/zstd"
	"gocloud.dev/blob"
)

// A bucket is a minimal interface that we need to talk to our backend storage.
// It is very similar to a subset of blob.Bucket but slightly different (the reader and
// writer returned are more generic to facilitate us putting a generic interface around it).
type bucket interface {
	NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blob.ReaderOptions) (io.ReadCloser, error)
	NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error)
	WriteAll(ctx context.Context, key string, data []byte) error
	Exists(ctx context.Context, key string) (bool, error)
	List(opts *blob.ListOptions) *blob.ListIterator
	Delete(ctx context.Context, key string, hard bool) error
}

func mustOpenStorage(url string) bucket {
	bucket, err := blob.OpenBucket(context.Background(), url)
	if err != nil {
		log.Fatalf("Failed to open storage %s: %v", url, err)
	}
	return &adapter{bucket: bucket}
}

type adapter struct {
	bucket *blob.Bucket
}

func (a *adapter) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blob.ReaderOptions) (io.ReadCloser, error) {
	return a.bucket.NewRangeReader(ctx, key, offset, length, opts)
}

func (a *adapter) NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error) {
	return a.bucket.NewWriter(ctx, key, opts)
}

func (a *adapter) WriteAll(ctx context.Context, key string, data []byte) error {
	return a.bucket.WriteAll(ctx, key, data, nil)
}

func (a *adapter) Exists(ctx context.Context, key string) (bool, error) {
	return a.bucket.Exists(ctx, key)
}

func (a *adapter) List(opts *blob.ListOptions) *blob.ListIterator {
	return a.bucket.List(opts)
}

func (a *adapter) Delete(ctx context.Context, key string, hard bool) error {
	return a.bucket.Delete(ctx, key) // this is always a 'hard' delete
}

// compressedReader returns a reader wrapped in a decompressor or compressor as needed.
func (s *server) compressedReader(r io.ReadCloser, needCompression, isCompressed bool) (io.ReadCloser, bool, error) {
	if needCompression == isCompressed {
		return r, false, nil // stream back bytes directly
	} else if isCompressed {
		zr := s.decompressorPool.Get().(*zstd.Decoder)
		defer s.decompressorPool.Put(zr)
		zr.Reset(r)
		return &doubleCloser{c: r, r: ioutil.NopCloser(zr)}, false, nil
	}
	return r, true, nil
}

// A doubleCloser wraps an io.ReadCloser and an io.Closer and closes both on Close().
type doubleCloser struct {
	c io.Closer
	r io.ReadCloser
}

func (c *doubleCloser) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *doubleCloser) Close() error {
	if err := c.r.Close(); err != nil {
		c.c.Close()
		return err
	}
	return c.c.Close()
}
