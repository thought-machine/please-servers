package rpc

import (
	"context"
	"io"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"gocloud.dev/blob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	ppb "github.com/thought-machine/please-servers/proto/purity"
)

func (s *server) List(ctx context.Context, req *ppb.ListRequest) (*ppb.ListResponse, error) {
	if len(req.Prefix) != 2 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid prefix provided: "+req.Prefix)
	}
	var g multierror.Group
	resp := &ppb.ListResponse{}
	g.Go(func() error {
		ar, err := s.list(ctx, "ac", req.Prefix)
		resp.ActionResults = ar
		return err
	})
	g.Go(func() error {
		ar, err := s.list(ctx, "cas", req.Prefix)
		for _, a := range ar {
			resp.Blobs = append(resp.Blobs, &ppb.Blob{Hash: a.Hash, SizeBytes: a.SizeBytes, Replicas: 1})
		}
		return err
	})
	g.Go(func() error {
		ar, err := s.list(ctx, "zstd_cas", req.Prefix)
		for _, a := range ar {
			resp.Blobs = append(resp.Blobs, &ppb.Blob{Hash: a.Hash, SizeBytes: a.SizeBytes, Replicas: 1})
		}
		return err
	})
	return resp, g.Wait().ErrorOrNil()
}

func (s *server) list(ctx context.Context, prefix, prefix2 string) ([]*ppb.ActionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	iter := s.bucket.List(&blob.ListOptions{Prefix: prefix + "/" + prefix2 + "/"})
	ret := []*ppb.ActionResult{}
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return ret, err
		} else if obj.IsDir {
			continue
		} else if hash := path.Base(obj.Key); !strings.HasPrefix(hash, "tmp") {
			ret = append(ret, &ppb.ActionResult{
				Hash:         hash,
				SizeBytes:    obj.Size, // Note that this might not be accurate for compressed blobs. For GC it is unlikely to matter deeply.
				LastAccessed: obj.ModTime.Unix(),
				Replicas:     1,
			})
		}
	}
	return ret, nil
}

func (s *server) Delete(ctx context.Context, req *ppb.DeleteRequest) (*ppb.DeleteResponse, error) {
	log.Notice("Delete request for %d action results, %d blobs", len(req.ActionResults), len(req.Blobs))
	// Note that we don't differentiate between compressed and uncompressed blobs in the request
	// (the fact they're stored differently is an implementation detail). This is a _little_
	// inefficient since we have to check all of them twice, but c'est la vie.
	var g multierror.Group
	g.Go(func() error { return s.deleteAll(ctx, "ac", req.ActionResults, req.Hard) })
	g.Go(func() error { return s.deleteAll(ctx, "cas", req.Blobs, req.Hard) })
	g.Go(func() error { return s.deleteAll(ctx, "zstd_cas", req.Blobs, req.Hard) })
	return &ppb.DeleteResponse{}, g.Wait().ErrorOrNil()
}

func (s *server) deleteAll(ctx context.Context, prefix string, blobs []*ppb.Blob, hard bool) error {
	var me *multierror.Error
	for _, blob := range blobs {
		key := s.key(prefix, &pb.Digest{Hash: blob.Hash, SizeBytes: blob.SizeBytes})
		if exists, _ := s.bucket.Exists(ctx, key); exists {
			if err := s.bucket.Delete(ctx, key, hard); err != nil {
				me = multierror.Append(me, err)
			}
			if s.knownBlobCache != nil {
				s.knownBlobCache.Del(key)
			}
		}
	}
	return me.ErrorOrNil()
}
