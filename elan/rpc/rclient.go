package rpc

import (
	"bytes"
	"context"
	"io"
	"fmt"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	hpb "google.golang.org/grpc/health/grpc_health_v1"
)

// This is the implementation backed by the SDK client. It's pretty simple since it was what we
// were using before and the interface mostly mimics that.
type remoteClient struct {
	c      *client.Client
	health hpb.HealthClient
}

func (r *remoteClient) Healthcheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if resp, err := r.health.Check(ctx, &hpb.HealthCheckRequest{}); err != nil {
		return err
	} else if resp.Status != hpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("Server not in healthy state: %s", resp.Status)
	}
	return nil
}

func (r *remoteClient) StreamBlob(dg *pb.Digest) (io.ReadCloser, error) {
	// TODO(peterebden): this kind of sucks. Unfortunately the SDK doesn't provide any useful
	//                   function to do a similar thing :(
	b, err := r.ReadBlob(dg)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), nil
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

func (r *remoteClient) GetDirectoryTree(dg *pb.Digest) ([]*pb.Directory, error) {
	return r.c.GetDirectoryTree(context.Background(), dg)
}
