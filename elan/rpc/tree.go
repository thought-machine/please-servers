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

// A treePool implements the downloading of directories in parallel for the GetTree RPC.
// It limits parallelism globally so we don't generate an arbitrarily large number of goroutines
// all trying to hit up the bucket at once.
type treePool struct {
	s *server
}

func newPool(s *server) *treePool {
	return &treePool{
		s: s,
	}
}

// GetTree returns a channel to iterate over all the directories recursively under the given one.
// The responses are not necessarily returned in any particular order.
func (p *treePool) GetTree(digest *pb.Digest) chan *dirResponse {
	var wg sync.WaitGroup
	wg.Add(1)
	ch := make(chan *dirResponse, 10)
	go p.fetchDir(digest, &wg, ch)
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func (p *treePool) fetchDir(digest *pb.Digest, wg *sync.WaitGroup, ch chan *dirResponse) {
	dir := &pb.Directory{}
	err := p.s.readBlobIntoMessage(context.Background(), "cas", digest, dir)
	ch <- &dirResponse{Dir: dir, Err: err}
	wg.Add(len(dir.Directories))
	for _, child := range dir.Directories {
		go p.fetchDir(child.Digest, wg, ch)
	}
	wg.Done()
}
