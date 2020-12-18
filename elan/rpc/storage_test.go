package rpc

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVerifyingReaderSuccess(t *testing.T) {
	const content = "catjam is the GIF of the year for me"
	const hash = "4df03f66bfa40c0dd9f7a96ce4799595c8d41a73d83f2a007f4bf0b6b1b1c10f"

	r := newVerifyingReader(strings.NewReader(content), hash)
	b := make([]byte, len(content)*2)
	n, err := r.Read(b)
	assert.NoError(t, err)
	assert.Equal(t, len(content), n)

	n, err = r.Read(b)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, n)
}

func TestVerifyingReaderFailure(t *testing.T) {
	const content = "catjam is the GIF of the year for me"
	const hash = "8aacd0326253200f202faaeaeb6174dce3f26a2524a8c42bdae1d45223c58ed5"

	r := newVerifyingReader(strings.NewReader(content), hash)
	b := make([]byte, len(content)*2)
	n, err := r.Read(b)
	assert.NoError(t, err)
	assert.Equal(t, len(content), n)

	n, err = r.Read(b)
	assert.Error(t, err)
	assert.NotEqual(t, io.EOF, err)
	assert.Equal(t, 0, n)

}
