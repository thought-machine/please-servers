package api

import (
	"sync"
)

// A bufferedChannel acts like a Go channel but with an (effectively) unlimited buffer.
type bufferedChannel[T any] struct {
	ch     chan T
	buf    []T
	lock   sync.Mutex
	closed bool
}

func newBufferedChannel[T any]() *bufferedChannel[T] {
	return &bufferedChannel[T]{
		ch: make(chan T, 10), // Always apply a bit of buffer here
	}
}

// Send adds a message to this channel. It will honour the ordering of previous messages.
func (ch *bufferedChannel[T]) Send(t T) {
	ch.lock.Lock()
	defer ch.lock.Unlock()
	if len(ch.buf) > 0 {
		ch.buf = append(ch.buf, t)
		return
	}
	// Either put it on the channel or buffer it if that can't accept it
	select {
	case ch.ch <- t:
	default:
		ch.buf = append(ch.buf, t)
	}
}

// Close closes this channel.
func (ch *bufferedChannel[T]) Close() {
	ch.lock.Lock()
	defer ch.lock.Unlock()
	ch.closed = true
}

// Receive receives a new message from the channel. It blocks until one is available.
func (ch *bufferedChannel[T]) Receive() (T, bool) {
	select {
	case t, ok := <-ch.ch:
		return t, ok
	default:
	}
	ch.lock.Lock()
	if len(ch.buf) > 0 {
		t := ch.buf[0]
		ch.buf = ch.buf[1:]
		ch.lock.Unlock()
		return t, true
	}
	if ch.closed {
		close(ch.ch)
		ch.lock.Unlock()
		var t T
		return t, false
	}
	ch.lock.Unlock()
	t, ok := <-ch.ch
	return t, ok
}
