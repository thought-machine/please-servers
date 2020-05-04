package trie

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplicatedSequentialSuccess(t *testing.T) {
	called := 0
	trie := testTrie(t)
	r := NewReplicator(trie, 2)
	// This should succeed on the second call.
	assert.NoError(t, r.Sequential("0000", func(s *Server) error {
		called++
		if s == trie.Get("0000") {
			return status.Errorf(codes.Unavailable, "Server down")
		}
		return nil
	}))
	assert.Equal(t, 2, called)
}

func TestReplicatedSequentialFailure(t *testing.T) {
	called := 0
	r := NewReplicator(testTrie(t), 2)
	// This should fail on all calls.
	assert.Error(t, r.Sequential("0000", func(s *Server) error {
		called++
		return status.Errorf(codes.Unavailable, "Server down")
	}))
	assert.Equal(t, 2, called)
}

func TestReplicatedSequentialNotRetryable(t *testing.T) {
	called := 0
	r := NewReplicator(testTrie(t), 4)
	// This should fail on the first call but not retry.
	assert.Error(t, r.Sequential("0000", func(s *Server) error {
		called++
		return status.Errorf(codes.InvalidArgument, "Your call is bad and you should feel bad")
	}))
	assert.Equal(t, 1, called)
}

func TestReplicatedParallelSuccess(t *testing.T) {
	called := 0
	trie := testTrie(t)
	r := NewReplicator(trie, 2)
	// This should succeed on the second server, and hence overall
	assert.NoError(t, r.Parallel("0000", func(s *Server) error {
		called++
		if s == trie.Get("0000") {
			return status.Errorf(codes.Unavailable, "Server down")
		}
		return nil
	}))
	assert.Equal(t, 2, called)
}

func TestReplicatedParallelFailure(t *testing.T) {
	called := 0
	r := NewReplicator(testTrie(t), 3)
	// This should fail since all writes fail
	assert.Error(t, r.Parallel("0000", func(s *Server) error {
		called++
		return status.Errorf(codes.Unavailable, "Server down")
	}))
	assert.Equal(t, 3, called)
}

func testTrie(t *testing.T) *Trie {
	trie := New(callback)
	assert.NoError(t, trie.AddAll(map[string]string{
		"00-3f": "127.0.0.1:443",
		"40-7f": "127.0.0.1:443",
		"80-af": "127.0.0.1:443",
		"b0-ff": "127.0.0.1:443",
	}))
	return trie
}
