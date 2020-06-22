package rpc

import (
	"context"
	"io"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/go-multierror"
	"gocloud.dev/blob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	ppb "github.com/thought-machine/please-servers/proto/purity"
)

func (s *server) Info(ctx context.Context, req *ppb.InfoRequest) (*ppb.InfoResponse, error) {
	if !strings.HasPrefix(s.storage, "file://") {
		return nil, status.Errorf(codes.Unimplemented, "This server does not support GC")
	}
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(strings.TrimPrefix(s.storage, "file://"), &statfs); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to determine available space: %s", err)
	}
	return &ppb.InfoResponse{
		FreeBytes:  int64(statfs.Bsize) * int64(statfs.Bavail),
		TotalBytes: int64(statfs.Bsize) * int64(statfs.Blocks),
	}, nil
}

func (s *server) List(ctx context.Context, req *ppb.ListRequest) (*ppb.ListResponse, error) {
	var g multierror.Group
	resp := &ppb.ListResponse{}
	g.Go(func() error {
		ar, err := s.list(ctx, "ac")
		resp.ActionResults = ar
		return err
	})
	g.Go(func() error {
		ar, err := s.list(ctx, "cas")
		for _, a := range ar {
			resp.Blobs = append(resp.Blobs, &ppb.Blob{Hash: a.Hash, SizeBytes: a.SizeBytes})
		}
		return err
	})
	return resp, g.Wait().ErrorOrNil()
}

func (s *server) list(ctx context.Context, prefix string) ([]*ppb.ActionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	iter := s.bucket.List(&blob.ListOptions{Prefix: prefix})
	ret := []*ppb.ActionResult{}
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return ret, err
		} else if obj.IsDir {
			continue
		}
		ret = append(ret, &ppb.ActionResult{
			Hash:         path.Base(obj.Key),
			SizeBytes:    obj.Size,
			LastAccessed: obj.ModTime.Unix(),
		})
	}
	return ret, nil
}

func (s *server) Delete(ctx context.Context, req *ppb.DeleteRequest) (*ppb.DeleteResponse, error) {
	log.Notice("Delete request for %d action results, %d blobs", len(req.ActionResults), len(req.Blobs))
	var g multierror.Group
	g.Go(func() error { return s.deleteAll(ctx, "ac", req.ActionResults) })
	g.Go(func() error { return s.deleteAll(ctx, "cas", req.Blobs) })
	return &ppb.DeleteResponse{}, g.Wait().ErrorOrNil()
}

func (s *server) deleteAll(ctx context.Context, prefix string, blobs []*ppb.Blob) error {
	var e error
	for _, blob := range blobs {
		if err := s.bucket.Delete(ctx, s.key(prefix, &pb.Digest{Hash: blob.Hash, SizeBytes: blob.SizeBytes})); err != nil {
			e = err
		}
	}
	return e
}
