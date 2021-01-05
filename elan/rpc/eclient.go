package rpc

import (
	"context"
	"io"
	"os"
	"time"

	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"
	"github.com/klauspost/compress/zstd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
)

const compressionThreshold = 1024

// This is the implementation backed by Elan in-process.
type elanClient struct{
	s *server
	timeout time.Duration
}

func (e *elanClient) ReadBlob(dg *pb.Digest) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	return e.s.readAllBlob(ctx, "cas", dg, false, false)
}

func (e *elanClient) WriteBlob(b []byte) (*pb.Digest, error) {
	dg := digest.NewFromBlob(b).ToProto()
	compressed := false
	if len(b) >= compressionThreshold {
		compressed = true
		b = e.s.compressor.EncodeAll(b, make([]byte, 0, len(b)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	return dg, e.s.bucket.WriteAll(ctx, e.s.compressedKey("cas", dg, compressed), b)
}

func (e *elanClient) UpdateActionResult(req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	return e.s.UpdateActionResult(ctx, req)
}

func (e *elanClient) UploadIfMissing(entries []*uploadinfo.Entry) error {
	m := make(map[string]*uploadinfo.Entry, len(entries))
	digests := make([]*pb.Digest, len(entries))
	for i, e := range entries {
		m[e.Digest.Hash] = e
		digests[i] = e.Digest.ToProto()
	}
	resp, err := e.s.FindMissingBlobs(context.Background(), &pb.FindMissingBlobsRequest{
		BlobDigests: digests,
	})
	if err != nil {
		return err
	}
	var g errgroup.Group
	for _, dg := range resp.MissingBlobDigests {
		entry := m[dg.Hash]
		g.Go(func() error {
			return e.uploadOne(entry)
		})
	}
	return g.Wait()
}

func (e *elanClient) uploadOne(entry *uploadinfo.Entry) error {
	e.s.limiter <- struct{}{}
	defer func() { <-e.s.limiter }()
	compressed := entry.Compressor != pb.Compressor_IDENTITY
	key := e.s.compressedKey("cas", entry.Digest.ToProto(), compressed)
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	if len(entry.Contents) > 0 {
		return e.s.bucket.WriteAll(ctx, key, entry.Contents)
	}
	// OK it's in a file, stream it across.
	f, err := os.Open(entry.Path)
	if err != nil {
		return err
	}
	defer f.Close()
	wr, err := e.s.bucket.NewWriter(ctx, key, &blob.WriterOptions{BufferSize: e.s.bufferSize(entry.Digest.ToProto())})
	if err != nil {
		return err
	}
	var w io.Writer = wr
	if compressed {
		zw := e.s.compressorPool.Get().(*zstd.Encoder)
		defer e.s.compressorPool.Put(zw)
		zw.Reset(w)
		w = zw
		defer zw.Close()
	}
	if _, err := io.Copy(w, f); err != nil {
		return err
	}
	if err := wr.Close(); err != nil {
		return err
	}
	e.s.markKnownBlob(key)
	return nil
}
