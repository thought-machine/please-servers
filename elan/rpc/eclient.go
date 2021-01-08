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
	if dg.Hash == digest.Empty.Hash {
		return dg, nil
	}
	compressed := false
	if len(b) >= compressionThreshold {
		compressed = true
		b = e.s.compressor.EncodeAll(b, make([]byte, 0, len(b)))
	}
	key := e.s.compressedKey("cas", dg, compressed)
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	if e.s.blobExists(ctx, key) {
		return dg, nil
	}
	return dg, e.s.bucket.WriteAll(ctx, key, b)
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
		if compressed {
			entry.Contents = e.s.compressor.EncodeAll(entry.Contents, make([]byte, 0, entry.Digest.Size))
		}
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
	defer wr.Close()
	var w io.WriteCloser = wr
	if compressed {
		zw := e.s.compressorPool.Get().(*zstd.Encoder)
		defer e.s.compressorPool.Put(zw)
		zw.Reset(w)
		w = zw
	}
	if _, err := io.Copy(w, f); err != nil {
		cancel()
		return err
	}
	if err := w.Close(); err != nil {
		cancel()
		return err
	}
	e.s.markKnownBlob(key)
	return nil
}

func (e *elanClient) BatchDownload(digests []digest.Digest, compressors []pb.Compressor_Value) (map[digest.Digest][]byte, error) {
	m := make(map[digest.Digest][]byte, len(digests))
	for i, dg := range digests {
		d, err := e.downloadOne(dg.ToProto(), compressors[i] != pb.Compressor_IDENTITY)
		if err != nil {
			return nil, err
		}
		m[dg] = d
	}
	return m, nil
}

func (e *elanClient) downloadOne(dg *pb.Digest, compressed bool) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	return e.s.readAllBlob(ctx, "cas", dg, true, false)
}

func (e *elanClient) ReadToFile(dg digest.Digest, filename string, compressed bool) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	r, _, err := e.s.readCompressed(ctx, "cas", dg.ToProto(), false, 0, -1)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.Copy(f, r)
	return err
}

func (e *elanClient) GetDirectoryTree(dg *pb.Digest) ([]*pb.Directory, error) {
	return e.s.getTree(dg)
}
