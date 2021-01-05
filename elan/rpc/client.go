package rpc

import (
	"strings"
	"time"

	"github.com/thought-machine/please-servers/rexclient"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
)

// New creates a new Client based on the given URL.
// If a scheme is given (gs:// etc) it assumes it is a bucket URL and uses that (with some
// default settings etc). The tls and tokenFile arguments are ignored in this case.
// If no scheme is given it assumes a gRPC URL and creates a remote client communicating using the API.
func New(url string, tls bool, tokenFile string) (Client, error) {
	// We can't use url.Parse here because it tends to put too much into the scheme (e.g. example.org:8080 -> scheme:example.org)
	if strings.Contains(url, "://") {
		return &elanClient{
			s: createServer(url, 8, 10240, 10*1024*1024),
			timeout: 1 * time.Minute,
		}, nil
	}
	client, err := rexclient.New("mettle", url, tls, tokenFile)
	return &remoteClient{c: client}, err
}

// Client is a genericised interface over a limited set of actions on the CAS / AC.
type Client interface{
	ReadBlob(*pb.Digest) ([]byte, error)
	WriteBlob([]byte) (*pb.Digest, error)
	UpdateActionResult(*pb.UpdateActionResultRequest) (*pb.ActionResult, error)
	UploadIfMissing([]*uploadinfo.Entry) error
}
