package rpc

import (
	"context"
	"sync"
	"time"
)

// defaultWriteTimeout is the maximum time a reader will wait for an in-progress
// write to complete before proceeding anyway.
const defaultWriteTimeout = 10 * time.Minute

// inflightWrites tracks blob writes that are currently in progress so that
// concurrent readers can block until the write completes rather than getting
// a spurious NotFound.
type inflightWrites struct {
	mu struct {
		sync.Mutex
		blobs map[string]context.Context
	}
}

func newInflightWrites() *inflightWrites {
	w := &inflightWrites{}
	w.mu.blobs = make(map[string]context.Context)
	return w
}

// startWrite registers a blob write in progress. The returned cancel function
// must be called when the write completes (success or failure) — use defer.
// The write context inherits from the caller so parent cancellation propagates.
// A default timeout of defaultWriteTimeout is applied as a safety net.
func (w *inflightWrites) startWrite(ctx context.Context, hash string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, defaultWriteTimeout)
	w.mu.Lock()
	w.mu.blobs[hash] = ctx
	w.mu.Unlock()
	return ctx, func() {
		w.mu.Lock()
		delete(w.mu.blobs, hash)
		w.mu.Unlock()
		cancel()
	}
}

// waitForWrite blocks until any in-progress write for the given hash completes.
// If no write is in progress, it returns immediately.
func (w *inflightWrites) waitForWrite(hash string) {
	w.mu.Lock()
	ctx, ok := w.mu.blobs[hash]
	w.mu.Unlock()
	if ok {
		<-ctx.Done()
	}
}
