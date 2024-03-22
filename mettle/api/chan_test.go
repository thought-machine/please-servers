package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferedChannel(t *testing.T) {
	ch := newBufferedChannel[int]()
	const n = 100
	go func() {
		for i := range n {
			ch.Send(i)
		}
		ch.Close()
	}()
	for i := range n {
		j, ok := ch.Receive()
		assert.Equal(t, i, j)
		assert.True(t, ok)
	}
	_, ok := ch.Receive()
	assert.False(t, ok)
}
