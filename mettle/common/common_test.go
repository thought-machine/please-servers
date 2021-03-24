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
