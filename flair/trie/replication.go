package trie

import (
	"fmt"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/hashicorp/go-multierror"
	"github.com/peterebden/go-cli-init"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = cli.MustGetLogger()

// A Replicator implements replication for our RPCs.
type Replicator struct {
	Trie      *Trie
	Replicas  int
	increment int
}

// NewReplicator returns a new Replicator instance.
func NewReplicator(t *Trie, replicas int) *Replicator {
	return &Replicator{
		Trie:      t,
		Replicas:  replicas,
		increment: 16 / replicas,
	}
}

// A ReplicatedFunc is a function that is passed to the replicator that gets called potentially more than once
// against different servers.
// If the returned appear appears to be retryable we will try on a new replica (e.g. we would retry Unavailable
// errors where the server is down, we wouldn't retry InvalidArgument where it seems it would be pointless).
type ReplicatedFunc func(*Server) error

// A ReplicatedAckFunc is like a ReplicatedFunc but allows the caller to specify whether the
// call should retry on the next replica by returning true, or not by returning false.
type ReplicatedAckFunc func(*Server) (bool, error)

// Sequential runs the function sequentially from the primary, attempting each replica in sequence until either one is
// successful or they all fail.
// It returns an error if all replicas fail, which is the error of the primary replica (with appropriate status code etc).
func (r *Replicator) Sequential(key string, f ReplicatedFunc) error {
	return r.SequentialAck(key, func(s *Server) (bool, error) {
		return false, f(s)
	})
}

// SequentialAck is like Sequential but allows the caller to specify whether the call should
// continue to the next replica even on a non-error response.
// This facilitates the BatchReadBlobs endpoint that basically never returns an 'actual' error
// because they're all inline.
func (r *Replicator) SequentialAck(key string, f ReplicatedAckFunc) error {
	var e error
	offset := 0
	for i := 0; i < r.Replicas; i++ {
		shouldContinue, err := f(r.Trie.GetOffset(key, offset))
		if !r.shouldRetry(err) && !shouldContinue {
			return err
		}
		if err == nil {
			log.Debug("Caller requested to continue on next replica for %s", key)
		} else if i < r.Replicas-1 {
			log.Debug("Error reading from replica for %s: %s. Will retry on next replica.", key, err)
		} else {
			log.Debug("Error reading from replica for %s: %s.", key, err)
		}
		if e == nil {
			e = err
		}
		offset += r.increment
	}
	if e != nil {
		log.Info("Reads from all replicas failed: %s", e)
	}
	return e
}

// SequentialDigest is like Sequential but takes a digest instead of the raw hash.
func (r *Replicator) SequentialDigest(digest *pb.Digest, f ReplicatedFunc) error {
	if digest == nil {
		return fmt.Errorf("Missing digest")
	} else if len(digest.Hash) != 64 {
		return fmt.Errorf("Invalid digest: [%s]", digest.Hash)
	}
	return r.Sequential(digest.Hash, f)
}

// Parallel replicates the given function to all replicas at once.
// It returns an error if all replicas fail, hence it is possible for some replicas not to receive data.
func (r *Replicator) Parallel(key string, f ReplicatedFunc) error {
	var g multierror.Group
	offset := 0
	for i := 0; i < r.Replicas; i++ {
		o := offset
		g.Go(func() error {
			return f(r.Trie.GetOffset(key, o))
		})
		offset += r.increment
	}
	if err := g.Wait(); err != nil {
		if len(err.Errors) < r.Replicas {
			log.Debug("Writes to some replicas for %s failed: %s", key, err)
			return nil
		}
		log.Info("Writes to all replicas for %s failed: %s", key, err)
		return err
	}
	return nil
}

// ParallelDigest is like Parallel but takes a digest instead of the raw hash.
func (r *Replicator) ParallelDigest(digest *pb.Digest, f ReplicatedFunc) error {
	if digest == nil {
		return fmt.Errorf("Missing digest")
	} else if len(digest.Hash) != 64 {
		return fmt.Errorf("Invalid digest: [%s]", digest.Hash)
	}
	return r.Parallel(digest.Hash, f)
}

// All sends a request to all replicas for a particular key simultaneously;
// it is like Parallel but waits for all replicas to complete.
func (r *Replicator) All(key string, f ReplicatedFunc) error {
	var g multierror.Group
	offset := 0
	for i := 0; i < r.Replicas; i++ {
		o := offset
		g.Go(func() error {
			return f(r.Trie.GetOffset(key, o))
		})
		offset += r.increment
	}
	return g.Wait().ErrorOrNil()
}

// shouldRetry returns true if the given error is retryable.
func (r *Replicator) shouldRetry(err error) bool {
	switch status.Code(err) {
	case codes.Unknown:
		return true // Unclear, might as well try again
	case codes.DeadlineExceeded:
		return true // Depends where the deadline is, but sure.
	case codes.NotFound:
		return true // This replica doesn't have the file, but another one might.
	case codes.ResourceExhausted:
		return true // Hopefully the replica will be more energetic
	case codes.Aborted:
		return true // Debatable since might need a higher-level retry, but we can give it a go.
	case codes.Unavailable:
		return true // Clearly retryable
	default:
		return false // Everything else.
	}
}
