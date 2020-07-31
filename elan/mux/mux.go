// Package mux provides a multiplexer over two gocloud buckets.
package mux

import (
	"context"
	"io"

	"github.com/peterebden/go-cli-init/v2"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

var log = cli.MustGetLogger()

// New returns a new mux.
func New(primary, secondary *blob.Bucket) *Mux {
	return &Mux{
		primary:   primary,
		secondary: secondary,
	}
}

type Mux struct {
	primary, secondary *blob.Bucket
}

func (m *Mux) Exists(ctx context.Context, key string) (bool, error) {
	if exists, err := m.primary.Exists(ctx, key); exists || err != nil {
		return exists, err
	}
	return m.secondary.Exists(ctx, key)
}

func (m *Mux) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blob.ReaderOptions) (io.ReadCloser, error) {
	if r, err := m.primary.NewRangeReader(ctx, key, offset, length, opts); err == nil {
		return r, nil
	}
	// Try to read from the secondary. If successful, we will store the write in the primary
	// on completion (but can only do that if we start at the beginning)
	r, err := m.secondary.NewRangeReader(ctx, key, offset, length, opts)
	if err != nil || offset != 0 {
		return r, err
	}
	w, err := m.primary.NewWriter(ctx, key, nil)
	if err != nil {
		// This isn't fatal, we can still satisfy the read off the secondary.
		log.Warning("Failed to duplicate blob to primary: %s", err)
		return r, nil
	}
	return &reader{r: r, w: w}, nil
}

func (m *Mux) NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error) {
	// We only write directly to the primary.
	return m.primary.NewWriter(ctx, key, opts)
}

func (m *Mux) List(opts *blob.ListOptions) *blob.ListIterator {
	// We only list the primary.
	return m.primary.List(opts)
}

func (m *Mux) Delete(ctx context.Context, key string, hard bool) error {
	if hard {
		err1 := m.primary.Delete(ctx, key)
		err2 := m.secondary.Delete(ctx, key)
		if err1 != nil && gcerrors.Code(err1) != gcerrors.NotFound {
			return err1
		} else if err2 != nil && gcerrors.Code(err2) != gcerrors.NotFound {
			return err2
		}
		return nil
	}
	// We interpret a soft delete as a move to the secondary.
	if err := m.copyBlob(ctx, key); err != nil {
		return err
	}
	return m.primary.Delete(ctx, key)
}

func (m *Mux) copyBlob(ctx context.Context, key string) error {
	r, err := m.primary.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := m.secondary.NewWriter(ctx, key, nil)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// A reader wraps an existing reader with writes to another destination.
// It's akin to an io.TeeReader but implements Close() and doesn't fail on write errors
// (which are best-effort).
type reader struct {
	r io.ReadCloser
	w io.WriteCloser
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if n > 0 {
		if _, err := r.w.Write(p[:n]); err != nil {
			log.Warning("Failed to duplicate write to primary file: %s", err)
		}
	}
	return n, err
}

func (r *reader) Close() error {
	if err := r.w.Close(); err != nil {
		log.Warning("Failed to finalise write to primary: %s", err)
	}
	return r.r.Close()
}
