package trie

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/hashicorp/go-multierror"
	"github.com/peterebden/go-cli-init/v4/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var log = logging.MustGetLogger()

var serverDead = status.Errorf(codes.DeadlineExceeded, "Server is down")

// failureThreshold is the number of timeouts we tolerate on a server before marking it
// out of service.
const failureThreshold = 5

// recheckFrequency is the rate at which we re-check dead servers to see if they become alive again.
const recheckFrequency = 1 * time.Minute

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
	var me *multierror.Error
	success := false
	offset := 0
	for i := 0; i < r.Replicas; i++ {
		shouldContinue, err := r.callAck(f, r.Trie.GetOffset(key, offset))
		if err == nil {
			if !shouldContinue {
				return nil  // No need to do any more.
			}
			log.Debug("Caller requested to continue on next replica for %s", key)
			success = true // we're always successful from here on
			offset += r.increment
			continue
		} else if !r.isContinuable(err) && !shouldContinue {
			return err
		}
		me = multierror.Append(me, err)
		if i < r.Replicas-1 {
			log.Debug("Error reading from replica for %s: %s. Will retry on next replica.", key, err)
		} else {
			log.Debug("Error reading from replica for %s: %s.", key, err)
		}
		offset += r.increment
	}
	if success {
		return nil
	} else if me != nil {
		log.Info("Reads from all replicas failed: %s", me)
	}
	return me.ErrorOrNil()
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
			return r.call(f, r.Trie.GetOffset(key, o))
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

// callAck calls a replicated function on a single replica, handling liveness on the server.
func (r *Replicator) callAck(f ReplicatedAckFunc, s *Server) (bool, error) {
	if s.Failed >= failureThreshold {
		log.Debug("Skipping replica %s-%s, it is marked down", s.Start, s.End)
		return true, serverDead
	}
	shouldContinue, err := f(s)
	if r.isServer(err) {
		if failed := atomic.AddInt64(&s.Failed, 1); failed == failureThreshold {
			log.Error("Error on server %s-%s, taking it out of service: %s", s.Start, s.End, err)
			go r.recheck(s)
		} else {
			log.Warning("Error on server %s-%s, failures: %d: %s", s.Start, s.End, failed, err)
		}
	} else if err == nil {
		s.Failed = 0 // Must be OK since it responded to this RPC successfully
	}
	return shouldContinue, err
}

// call is like callAck but doesn't offer the option of acknowledgement.
func (r *Replicator) call(f ReplicatedFunc, s *Server) error {
	_, err := r.callAck(func(s *Server) (bool, error) {
		return false, f(s)
	}, s)
	return err
}

// Healthcheck returns an error if any first-level ranges of the trie are unhealthy
// (i.e. do not have any live servers in them)
func (r *Replicator) Healthcheck() error {
	for i := 0; i < 16; i++ {
		if err := r.healthcheckRange(string(toHex(i)) + "000"); err != nil {
			return err
		}
	}
	return nil
}

func (r *Replicator) healthcheckRange(key string) error {
	for j := 0; j < r.Replicas; j++ {
		s := r.Trie.GetOffset(key, j)
		if s.Failed < failureThreshold {
			return nil // This server is alive, so this range is alive.
		}
	}
	return fmt.Errorf("All replicas for range %s are down", key)
}

// recheck continually rechecks a server to see if it's become alive again.
func (r *Replicator) recheck(s *Server) {
	t := time.NewTicker(recheckFrequency)
	defer t.Stop()
	for range t.C {
		if r.recheckOnce(s) {
			break
		}
	}
}

func (r *Replicator) recheckOnce(s *Server) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.Health.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		log.Warning("Server %s-%s is still unhealthy: %s", s.Start, s.End, err)
		return false
	}
	log.Notice("Server %s-%s is now healthy again", s.Start, s.End)
	s.Failed = 0
	return true
}

// isContinuable returns true if the given error is retryable.
func (r *Replicator) isContinuable(err error) bool {
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

// isServer returns true if the given error is a server error and we should mark it down.
func (r *Replicator) isServer(err error) bool {
	switch status.Code(err) {
	case codes.Unknown, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Unavailable:
		return true
	default:
		return false
	}
}
