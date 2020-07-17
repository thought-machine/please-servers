package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseProper(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381/122")
	assert.NoError(t, err)
	assert.Equal(t, "ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381", a.Hash)
	assert.Equal(t, 122, a.Size)
}

func TestParseTrailingSlash(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381/122/")
	assert.NoError(t, err)
	assert.Equal(t, "ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381", a.Hash)
	assert.Equal(t, 122, a.Size)
}

func TestParseShort(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381")
	assert.NoError(t, err)
	assert.Equal(t, "ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381", a.Hash)
	assert.Equal(t, 147, a.Size)
}

func TestParseFail(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("thirty-five ham and cheese sandwiches")
	assert.Error(t, err)
}

func TestParseWrongLength(t *testing.T) {
	var a Action
	err := a.UnmarshalFlag("ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf138")
	assert.Error(t, err)
}
