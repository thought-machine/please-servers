package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckPath(t *testing.T) {
	assert.NoError(t, CheckPath("test"))
	assert.NoError(t, CheckPath("test/test2"))
	assert.NoError(t, CheckPath("test/./test2")) // Bit dodgy but not actually a problem
	assert.Error(t, CheckPath("/test"))
	assert.Error(t, CheckPath("/.."))
	assert.Error(t, CheckPath(".."))
	assert.Error(t, CheckPath("../"))
	assert.Error(t, CheckPath("../test"))
	assert.Error(t, CheckPath("test/../test"))
}

func TestLimitBatchSize(t *testing.T) {
	assert.Equal(t, "gcppubsub://projects/mettle/subscriptions/mettle-requests?max_recv_batch_size=1", limitBatchSize("gcppubsub://projects/mettle/subscriptions/mettle-requests", "1"))
}
