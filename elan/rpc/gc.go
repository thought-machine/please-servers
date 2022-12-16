package rpc

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"gocloud.dev/blob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
			resp.Blobs = append(resp.Blobs, &ppb.Blob{Hash: a.Hash, SizeBytes: a.SizeBytes, Replicas: 1, CachePrefix: a.CachePrefix})
		}
		return err
	})
	g.Go(func() error {
		ar, err := s.list(ctx, "zstd_cas", req.Prefix)
		for _, a := range ar {
			resp.Blobs = append(resp.Blobs, &ppb.Blob{Hash: a.Hash, SizeBytes: a.SizeBytes, Replicas: 1, CachePrefix: a.CachePrefix})
		}
		return err
	})
	return resp, g.Wait().ErrorOrNil()
}

func (s *server) list(ctx context.Context, prefix1, prefix2 string) ([]*ppb.ActionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	prefix := prefix1 + "/" + prefix2 + "/"
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
		} else if hash := path.Base(obj.Key); !strings.HasPrefix(hash, "tmp") {
			ret = append(ret, &ppb.ActionResult{
				Hash:         hash,
				SizeBytes:    obj.Size, // Note that this might not be accurate for compressed blobs. For GC it is unlikely to matter deeply.
				LastAccessed: obj.ModTime.Unix(),
				Replicas:     1,
				CachePrefix:  prefix,
			})
		}
	}
	return ret, nil
}

func (s *server) Delete(ctx context.Context, req *ppb.DeleteRequest) (*ppb.DeleteResponse, error) {
	log.Notice("Delete request for %d action results, %d blobs", len(req.ActionResults), len(req.Blobs))
	var g multierror.Group
	g.Go(func() error { return s.deleteAll(ctx, req.ActionResults, req.Hard) })
	g.Go(func() error { return s.deleteAll(ctx, req.Blobs, req.Hard) })
	return &ppb.DeleteResponse{}, g.Wait().ErrorOrNil()
}

func (s *server) deleteAll(ctx context.Context, blobs []*ppb.Blob, hard bool) error {
	var me *multierror.Error
	for _, blob := range blobs {
		key := blob.CachePrefix + blob.Hash
		if exists, err := s.bucket.Exists(ctx, key); exists {
			if err := s.bucket.Delete(ctx, key, hard); err != nil {
				log.Error("Error deleting blob: %v", err)
				me = multierror.Append(me, fmt.Errorf("Error deleting blob: %w", err))
			}
			if s.knownBlobCache != nil {
				s.knownBlobCache.Del(key)
			}
		} else if err != nil {
			log.Warning("Error reading blob: %v", err)
		} else {
			log.Warning("blob not found: %s", key)
		}
	}
	return me.ErrorOrNil()
}
