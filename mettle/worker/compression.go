package worker

import (
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// compressor returns the compressor to use for a file with the given name.
func compressor(filename string) pb.Compressor_Value {
	if strings.HasSuffix(filename, ".zip") || strings.HasSuffix(filename, ".pex") ||
		strings.HasSuffix(filename, ".jar") || strings.HasSuffix(filename, ".gz") ||
		strings.HasSuffix(filename, ".bz2") || strings.HasSuffix(filename, ".xz") {
		return pb.Compressor_IDENTITY
	}
	return pb.Compressor_ZSTD
}

// A fileMetadataCache is a no-op implementation of the SDK's filemetadata.Cache interface.
// The main change versus the built-in no-op cache is setting the compressor on the metadata.
type fileMetadataCache struct{}

func (c fileMetadataCache) Get(path string) *filemetadata.Metadata {
	m := filemetadata.Compute(path)
	m.Compressor = compressor(path)
	return m
}

func (c fileMetadataCache) Delete(string) error {
	return nil
}

func (c fileMetadataCache) Update(string, *filemetadata.Metadata) error {
	return nil
}

func (c fileMetadataCache) Reset() {}

func (c fileMetadataCache) GetCacheHits() uint64 {
	return 0
}

func (c fileMetadataCache) GetCacheMisses() uint64 {
	return 0
}
