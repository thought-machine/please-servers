package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/thought-machine/please-servers/proto/lucidity"
)

func TestValidVersion(t *testing.T) {
	// Requiring 70% here facilitates the test below; I aimed for it to be 40% but there is a bit of rounding weirdness
	// when the number of workers is small.
	s := newServer(0.7)

	// Helper function. Returns true if this update would trigger disabling the worker.
	update := func(name, version string) bool {
		r, err := s.Update(context.Background(), &pb.UpdateRequest{
			Name:    name,
			Version: version,
		})
		assert.NoError(t, err)
		return r.ShouldDisable
	}

	assert.False(t, update("worker-1", "1.0")) // At this point this worker is 100% of the fleet
	assert.False(t, update("worker-2", "2.0")) // This one is 50% of the fleet so still OK
	assert.False(t, update("worker-3", "2.0")) // 2 and 3 are now 66% so still OK
	assert.True(t, update("worker-1", "1.0"))  // worker-1 is now only 33% so it gets turned off.
	assert.False(t, update("worker-2", "2.0")) // worker-2 is still OK as it was before though
	assert.False(t, update("worker-1", "2.0")) // worker-1 has now updated and is alive again.
	assert.True(t, update("worker-3", "3.0"))  // worker-3 has now updated beyond the others and it disables.
}
