package rexclient

import (
	"bytes"
	"net/http"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
)

// DetectArchiveUploads is an UploadCompressionPredicate that attempts to detect whether
// uploads are already archives, either based on their initial few bytes or known file extensions.
// It is a best-effort classifier that may not recognise every possible type.
func DetectArchiveUploads(ue *uploadinfo.Entry) bool {
	if len(ue.Contents) > 0 {
		switch http.DetectContentType(ue.Contents) {
		case "application/x-rar-compressed", "application/x-gzip", "application/zip", "video/webm", "image/gif", "image/webp", "image/png", "image/jpeg":
			return false
		case "application/octet-stream":
			// xz and bzip2 aren't detected by the net/http package but are fairly common in the wild
			if bytes.HasPrefix(ue.Contents, []byte{0xFD, '7', 'z', 'X', 'Z', 0x00}) || bytes.HasPrefix(ue.Contents, []byte{0x42, 0x5A, 0x68}) {
				return false
			}
		}
		return true
	}
	for _, suffix := range knownIncompressibleSuffixes {
		if strings.HasSuffix(ue.Path, suffix) {
			return false
		}
	}
	return true
}

// File suffixes that we expect not to be further compressible
var knownIncompressibleSuffixes = []string{
	".zip", ".pex", ".jar", ".gz", ".bz2", ".xz", ".rar", ".whl", ".lzma",
	".webm", ".jpg", ".jpeg", ".gif", ".png", ".mp4", ".mkv",
}
