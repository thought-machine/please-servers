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
	return retryWithReturnValue(ctx, b.retries, func() (io.ReadCloser, error) {
		return b.bucket.NewRangeReader(ctx, key, offset, length, opts)
	})
}

func (b retryBucket) NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error) {
	return retryWithReturnValue(ctx, b.retries, func() (io.WriteCloser, error) {
		return b.bucket.NewWriter(ctx, key, opts)
	})
}

func (b retryBucket) WriteAll(ctx context.Context, key string, data []byte) error {
	return retry(ctx, b.retries, func() error {
		return b.bucket.WriteAll(ctx, key, data)
	})
}

func (b retryBucket) ReadAll(ctx context.Context, key string) ([]byte, error) {
	return retryWithReturnValue(ctx, b.retries, func() ([]byte, error) {
		return  b.bucket.ReadAll(ctx, key)
	})
}

func (b retryBucket) Exists(ctx context.Context, key string) (bool, error) {
	return retryWithReturnValue(ctx, b.retries, func() (bool, error) {
		return  b.bucket.Exists(ctx, key)
	})
}

func (b retryBucket) Delete(ctx context.Context, key string, hard bool) error {
	return retry(ctx, b.retries, func() error {
		return b.bucket.Delete(ctx, key, hard)
	})
}

func retryWithReturnValue[V any](ctx context.Context, retries int, f func() (V, error)) (V, error) {
	var (
		ret V
		err    error
	)
	err = retry(ctx, retries, func() error {
		ret, err = f()
		return err
	})
	return ret, err
}

func retry(ctx context.Context, retries int, f func() error) error {
	var (
		err = f()
		r   int
	)
	for r = 0; r < retries && err != nil && ctx.Err() == nil && isErrorRetryable(err); r++ {
		err = f()
	}
	if err != nil && r > 0 {
		return fmt.Errorf("%w (retries: %d)", err, r)
	}
	return err
}

func isErrorRetryable(err error) bool {
	return (gcerrors.Code(err) == gcerrors.Internal || status.Code(err) == codes.Internal) ||
		(gcerrors.Code(err) == gcerrors.Canceled || status.Code(err) == codes.Canceled) ||
		(gcerrors.Code(err) == gcerrors.DeadlineExceeded || status.Code(err) == codes.DeadlineExceeded) ||
		(gcerrors.Code(err) == gcerrors.ResourceExhausted || status.Code(err) == codes.ResourceExhausted) ||
		(gcerrors.Code(err) == gcerrors.Unknown || status.Code(err) == codes.Unknown)
}
