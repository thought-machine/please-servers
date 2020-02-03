package rpc

import (
	"context"
	"reflect"
	"sync"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

var (
	sizeofFileNode      = int(reflect.TypeOf(pb.FileNode{}).Size())
	sizeofDirectoryNode = int(reflect.TypeOf(pb.DirectoryNode{}).Size())
	sizeofSymlinkNode   = int(reflect.TypeOf(pb.SymlinkNode{}).Size())
	sizeofDirectory     = int(reflect.TypeOf(pb.Directory{}).Size())
)

type dirResponse struct {
	Dir  *pb.Directory
	Err  error
	Size int
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
	size := len(dir.Files)*sizeofFileNode + len(dir.Directories)*sizeofDirectoryNode + len(dir.Symlinks)*sizeofSymlinkNode + sizeofDirectory
	ch <- &dirResponse{Dir: dir, Err: err, Size: size}
	wg.Add(len(dir.Directories))
	for _, child := range dir.Directories {
		go s.fetchDir(child.Digest, wg, ch)
	}
	wg.Done()
}
