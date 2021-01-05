package rpc

import (
	"context"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
)

// This is the implementation backed by the SDK client. It's pretty simple since it was what we
// were using before and the interface mostly mimics that.
type remoteClient struct{
	c *client.Client
}

func (r *remoteClient) ReadBlob(dg *pb.Digest) ([]byte, error) {
	return r.c.ReadBlob(context.Background(), digest.NewFromProtoUnvalidated(dg))
}

func (r *remoteClient) WriteBlob(b []byte) (*pb.Digest, error) {
	dg, err := r.c.WriteBlob(context.Background(), b)
	return dg.ToProto(), err
}

func (r *remoteClient) UpdateActionResult(req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	return r.c.UpdateActionResult(context.Background(), req)
}

func (r *remoteClient) UploadIfMissing(entries []*uploadinfo.Entry) error {
	_, _, err := r.c.UploadIfMissing(context.Background(), entries...)
	return err
}

func (r *remoteClient) BatchDownload(digests []digest.Digest, compressors []pb.Compressor_Value) (map[digest.Digest][]byte, error) {
	return r.c.BatchDownloadCompressedBlobs(context.Background(), digests, compressors)
}

func (r *remoteClient) ReadToFile(dg digest.Digest, filename string, compressed bool) error {
	if !compressed {
		_, err := r.c.ReadBlobToFileUncompressed(context.Background(), dg, filename)
		return err
	}
	_, err := r.c.ReadBlobToFile(context.Background(), dg, filename)
	return err
}
