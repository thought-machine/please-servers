package rpc

import (
	"context"
	"io"

	"gocloud.dev/blob"

	"github.com/thought-machine/please-servers/elan/mux"
)

// A bucket is a minimal interface that we need to talk to our backend storage.
// It is very similar to a subset of blob.Bucket but slightly different (the
// reader and writer returned are more generic to facilitate our multiplexer).
type bucket interface {
	NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blob.ReaderOptions) (io.ReadCloser, error)
	NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error)
	Exists(ctx context.Context, key string) (bool, error)
	List(opts *blob.ListOptions) *blob.ListIterator
	Delete(ctx context.Context, key string) error
}

func mustOpenStorage(url string) *blob.Bucket {
	bucket, err := blob.OpenBucket(context.Background(), url)
	if err != nil {
		log.Fatalf("Failed to open storage %s: %v", url, err)
	}
	return bucket
}

func mustOpenStorages(primary, secondary string) bucket {
	if secondary == "" {
		return &adapter{bucket: mustOpenStorage(primary)}
	}
	return mux.New(mustOpenStorage(primary), mustOpenStorage(secondary))
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

func (a *adapter) Exists(ctx context.Context, key string) (bool, error) {
	return a.bucket.Exists(ctx, key)
}

func (a *adapter) List(opts *blob.ListOptions) *blob.ListIterator {
	return a.bucket.List(opts)
}

func (a *adapter) Delete(ctx context.Context, key string) error {
	return a.bucket.Delete(ctx, key)
}
