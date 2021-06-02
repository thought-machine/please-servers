package rexclient

import (
	"testing"

	sdkdigest "github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/assert"
)

func TestPackDigest(t *testing.T) {
	assert.Equal(t, PackDigest(&pb.Directory{
		NodeProperties: &pb.NodeProperties{
			Properties: []*pb.NodeProperty{
				{
					Name:  "bob",
					Value: "6e104986dd5b8b3b51d755276d77cc3b6034a89f8e856c3518d21ad9233be9a2/123",
				},
				{
					Name:  PackName,
					Value: "b10bd3130c1c0c13552e64356445290992346ad995cc5c1388a1e6150bc21c07/156",
				},
			},
		},
	}), sdkdigest.Digest{
		Hash: "b10bd3130c1c0c13552e64356445290992346ad995cc5c1388a1e6150bc21c07",
		Size: 156,
	})
}

func TestPackDigestMissing(t *testing.T) {
	assert.Equal(t, PackDigest(&pb.Directory{
		NodeProperties: &pb.NodeProperties{
			Properties: []*pb.NodeProperty{
				{
					Name:  "bob",
					Value: "6e104986dd5b8b3b51d755276d77cc3b6034a89f8e856c3518d21ad9233be9a2/123",
				},
			},
		},
	}), sdkdigest.Digest{})
}

func TestPackDigestNotParseable(t *testing.T) {
	assert.Equal(t, PackDigest(&pb.Directory{
		NodeProperties: &pb.NodeProperties{
			Properties: []*pb.NodeProperty{
				{
					Name:  "bob",
					Value: "6e104986dd5b8b3b51d755276d77cc3b6034a89f8e856c3518d21ad9233be9a2/123",
				},
				{
					Name:  PackName,
					Value: "b10bd3130c1c0c13552e64356445290992346ad995cc5c1388a1e6150bc21c07",
				},
			},
		},
	}), sdkdigest.Digest{})
}

func TestPackDigestNoMessage(t *testing.T) {
	assert.Equal(t, PackDigest(&pb.Directory{}), sdkdigest.Digest{})
}
