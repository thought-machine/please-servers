package rpc

import (
	"context"
	"sync"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

type dirResponse struct {
	Dir *pb.Directory
	Err error
}

// getTree returns a channel to iterate over all the directories recursively under the given one.
// The responses are not necessarily returned in any particular order.
func (s *server) getTree(digest *pb.Digest) chan *dirResponse {
	var wg sync.WaitGroup
	wg.Add(1)
	ch := make(chan *dirResponse, 10)
	go s.fetchDir(digest, &wg, ch)
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func (s *server) fetchDir(digest *pb.Digest, wg *sync.WaitGroup, ch chan *dirResponse) {
	dir := &pb.Directory{}
	err := s.readBlobIntoMessage(context.Background(), "cas", digest, dir)
	ch <- &dirResponse{Dir: dir, Err: err}
	wg.Add(len(dir.Directories))
	for _, child := range dir.Directories {
		go s.fetchDir(child.Digest, wg, ch)
	}
	wg.Done()
}
