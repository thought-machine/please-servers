package rpc

import (
	"context"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

const compressionThreshold = 1024

type elanClient struct{
	s *server
	timeout time.Duration
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
