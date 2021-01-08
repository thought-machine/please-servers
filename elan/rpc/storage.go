package rpc

import (
	"context"
	"io"
	"io/ioutil"
	"sync"

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
	ReadAll(ctx context.Context, key string) ([]byte, error)
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

func (a *adapter) ReadAll(ctx context.Context, key string) ([]byte, error) {
	return a.bucket.ReadAll(ctx, key)
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
func (s *server) compressedReader(r io.ReadCloser, needCompression, isCompressed bool, offset int64) (io.ReadCloser, bool, error) {
	if isCompressed && offset != 0 {
		// Offsets refer into the uncompressed blob, we have to handle that ourselves.
		r = s.decompressReader(r)
		_, err := io.CopyN(ioutil.Discard, r, offset)
		return r, true, err
	} else if needCompression == isCompressed {
		return r, false, nil // stream back bytes directly
	} else if isCompressed {
		return s.decompressReader(r), false, nil
	}
	return r, true, nil
}

// decompressReader wraps a reader in zstd decompression.
func (s *server) decompressReader(r io.ReadCloser) io.ReadCloser {
	zr := s.decompressorPool.Get().(*zstd.Decoder)
	zr.Reset(r)
	return &zstdCloser{c: r, r: zr, p: s.decompressorPool}
}

// A zstdCloser takes all reads from a reader but closes a different closer, and re-adds
// the decoder to a pool after.
// This is because we don't want to close the zstd readers (we reset them instead) but we do
// want to close the underlying reader into the storage bucket, and make sure they are
// returned to the pool.
type zstdCloser struct {
	c io.Closer
	r *zstd.Decoder
	p *sync.Pool
}

func (c *zstdCloser) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *zstdCloser) Close() error {
	err := c.c.Close()
	c.p.Put(c.r)
	return err
}
