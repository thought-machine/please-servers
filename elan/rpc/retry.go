package rpc

import (
	"context"
	"fmt"
	"io"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryBucket implements a retry logic on top of all bucket methods that can
// return an error. Those methods are retried only on internal errors that are
// not caused by a context cancellation.
type retryBucket struct {
	bucket
	retries int
}

func (b retryBucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blob.ReaderOptions) (io.ReadCloser, error) {
	var (
		rc  io.ReadCloser
		err error
	)
	err = b.retry(ctx, func() error {
		rc, err = b.bucket.NewRangeReader(ctx, key, offset, length, opts)
		return err
	})
	return rc, err
}

func (b retryBucket) NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error) {
	var (
		wc  io.WriteCloser
		err error
	)
	err = b.retry(ctx, func() error {
		wc, err = b.bucket.NewWriter(ctx, key, opts)
		return err
	})
	return wc, err
}

func (b retryBucket) WriteAll(ctx context.Context, key string, data []byte) error {
	return b.retry(ctx, func() error {
		return b.bucket.WriteAll(ctx, key, data)
	})
}

func (b retryBucket) ReadAll(ctx context.Context, key string) ([]byte, error) {
	var (
		data []byte
		err  error
	)
	err = b.retry(ctx, func() error {
		data, err = b.bucket.ReadAll(ctx, key)
		return err
	})
	return data, err
}

func (b retryBucket) Exists(ctx context.Context, key string) (bool, error) {
	var (
		exists bool
		err    error
	)
	err = b.retry(ctx, func() error {
		exists, err = b.bucket.Exists(ctx, key)
		return err
	})
	return exists, err
}

func (b retryBucket) Delete(ctx context.Context, key string, hard bool) error {
	return b.retry(ctx, func() error {
		return b.bucket.Delete(ctx, key, hard)
	})
}

func (b retryBucket) retry(ctx context.Context, f func() error) error {
	var (
		err = f()
		r   int
	)
	for r = 0; r < b.retries && err != nil && ctx.Err() == nil && b.isErrorRetryable(err); r++ {
		err = f()
	}
	if err != nil && r > 0 {
		return fmt.Errorf("%w (retries: %d)", err, r)
	}
	return err
}

func (b retryBucket) isErrorRetryable(err error) bool {
	return gcerrors.Code(err) == gcerrors.Internal || status.Code(err) == codes.Internal
}
