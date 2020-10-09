package rpc

import (
	"context"
	"sync"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/hashicorp/go-multierror"
)

// getTree returns a channel to iterate over all the directories recursively under the given one.
// The responses are not necessarily returned in any particular order.
func (s *server) getTree(digest *pb.Digest) ([]*pb.Directory, error) {
	dir := &pb.Directory{}
	if digest.SizeBytes == 0 && digest.Hash == emptyHash {
		return []*pb.Directory{dir}, nil
	}
	if dir, present := s.dirCache.Get(digest.Hash); present {
		d := dir.([]*pb.Directory)
		dirCacheHits.Add(float64(len(d)))
		return d, nil
	}
	dirCacheMisses.Inc()
	if err := s.readBlobIntoMessage(context.Background(), "cas", digest, dir); err != nil {
		return nil, err
	}
	ret := []*pb.Directory{dir}
	var g multierror.Group
	var mutex sync.Mutex
	for _, child := range dir.Directories {
		child := child
		g.Go(func() error {
			dirs, err := s.getTree(child.Digest)
			mutex.Lock()
			defer mutex.Unlock()
			ret = append(ret, dirs...)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return ret, err
	}
	s.dirCache.Set(digest.Hash, ret, int64(len(ret)))
	return ret, nil
}
