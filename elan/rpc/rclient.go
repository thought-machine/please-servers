package rpc

import (
	"context"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

type remoteClient struct{
	c *client.Client
}

func (r *remoteClient) WriteBlob(b []byte) (*pb.Digest, error) {
	dg, err := r.c.WriteBlob(context.Background(), b)
	return dg.ToProto(), err
}
