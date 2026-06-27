package rpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitForWrite_NoInflight(t *testing.T) {
	w := newInflightWrites()
	// Should return immediately when nothing is in flight.
	doneCtx, doneFunc := context.WithCancel(t.Context())
	go func() {
		w.waitForWrite("abc123")
		doneFunc()
	}()
	select {
	case <-doneCtx.Done():
	case <-time.After(120 * time.Second):
		t.Fatal("waitForWrite blocked when no write was in progress")
	}
}

func TestWaitForWrite_BlocksUntilDone(t *testing.T) {
	w := newInflightWrites()
	_, finish := w.startWrite(t.Context(), "abc123")

	var order []string
	var mu sync.Mutex
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	doneCtx, doneFunc := context.WithCancel(t.Context())
	go func() {
		w.waitForWrite("abc123")
		record("read")
		doneFunc()
	}()

	// Give the reader goroutine time to block.
	time.Sleep(50 * time.Millisecond)
	record("write")
	finish()

	select {
	case <-doneCtx.Done():
	case <-t.Context().Done():
		t.Fatal("reader never unblocked")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"write", "read"}, order)
}

func TestWaitForWrite_DifferentDigests(t *testing.T) {
	w := newInflightWrites()
	_, finish := w.startWrite(t.Context(), "abc123")
	defer finish()

	// A different digest should not block.
	doneCtx, doneFunc := context.WithCancel(t.Context())
	go func() {
		w.waitForWrite("def456")
		doneFunc()
	}()
	select {
	case <-doneCtx.Done():
	case <-t.Context().Done():
		t.Fatal("waitForWrite blocked on a different digest")
	}
}

func TestWaitForWrite_ParentCancellation(t *testing.T) {
	w := newInflightWrites()
	ctx, cancel := context.WithCancel(t.Context())
	_, finish := w.startWrite(ctx, "abc123")
	defer finish()

	doneCtx, doneFunc := context.WithCancel(t.Context())
	go func() {
		w.waitForWrite("abc123")
		doneFunc()
	}()

	// Cancel the parent context — reader should unblock.
	cancel()
	select {
	case <-doneCtx.Done():
	case <-t.Context().Done():
		t.Fatal("reader did not unblock after parent context cancellation")
	}
}

func TestWaitForWrite_MultipleReaders(t *testing.T) {
	w := newInflightWrites()
	_, finish := w.startWrite(t.Context(), "abc123")

	const numReaders = 10
	doneCtx, doneFunc := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Add(numReaders)
	for range numReaders {
		go func() {
			w.waitForWrite("abc123")
			wg.Done()
		}()
	}

	// All readers should be blocked. Finish the write.
	time.Sleep(50 * time.Millisecond)
	finish()

	go func() {
		wg.Wait()
		doneFunc()
	}()
	select {
	case <-doneCtx.Done():
	case <-t.Context().Done():
		t.Fatal("not all readers unblocked")
	}
}
