package rexclient

import (
	"context"
	"strconv"
	"strings"

	sdkdigest "github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/metadata"
)

// PackName is the node property name that we apply to a 'pack', a compressed tarball representing
// everything under a directory which we can use to short-circuit the download of a large tree.
const PackName = "mettle.pack"

// packKey is the metadata key we use to control tree searches.
const packKey = "mettle.stop-at-pack"

// PackDigest returns the digest of a pack associated with the given directory, or an empty
// digest if there isn't one.
func PackDigest(dir *pb.Directory) sdkdigest.Digest {
	if dir.NodeProperties == nil {
		return sdkdigest.Digest{}
	}
	for _, prop := range dir.NodeProperties.Properties {
		if prop.Name == PackName {
			// Need to do a bit of parsing here
			if idx := strings.IndexByte(prop.Value, '/'); idx != -1 {
				size, err := strconv.Atoi(prop.Value[idx+1:])
				if err != nil {
					log.Warning("Can't parse size from pack %s: %s", prop.Value, err)
					continue
				}
				return sdkdigest.Digest{
					Hash: prop.Value[:idx],
					Size: int64(size),
				}
			} else {
				log.Warning("Invalid pack format: %s", prop.Value)
			}
		}
	}
	return sdkdigest.Digest{}
}

// ShouldStopAtPack indicates if the given context contains metadata indicating that the client
// will consume packs and we should stop processing a tree if we find one.
func ShouldStopAtPack(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return len(md.Get(packKey)) > 0
	}
	return false
}

// StopAtPack adds metadata to the outgoing context indicating that the client will consume
// packs and the server should stop processing a tree when it reaches one.
func StopAtPack(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, packKey, "stop")
}
