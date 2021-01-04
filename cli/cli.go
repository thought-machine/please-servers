// Package cli implements some simple shared CLI flag types.
package cli

import (
	"fmt"
	"regexp"
	"strconv"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/peterebden/go-cli-init/v3"
)

var log = cli.MustGetLogger()

var actionRe = regexp.MustCompile("([0-9a-fA-F]+)/([0-9]+)/?")
var shortActionRe = regexp.MustCompile("([0-9a-fA-F]+)")

// An Action represents a combined hash / size pair written like
// ff17a4efe382e245491d6a9f1ac6bf3adce454f7e4a5559a3579c3856edf1381/122
// This is a bit more concise than passing them with flags.
type Action struct {
	Hash string
	Size int
}

func (a *Action) UnmarshalFlag(in string) error {
	matches := actionRe.FindStringSubmatch(in)
	if matches != nil {
		return a.fromComponents(matches[1], matches[2])
	}
	matches = shortActionRe.FindStringSubmatch(in)
	if matches != nil {
		return a.fromComponents(matches[1], "")
	}
	return fmt.Errorf("Unknown action format: %s", in)
}

func (a *Action) fromComponents(hash, size string) error {
	if len(hash) != 64 {
		return fmt.Errorf("Invalid hash %s; has length %d, should be 64", hash, len(hash))
	}
	a.Hash = hash
	if size == "" {
		// This looks a bit arbitrary but for whatever reason most of our actions (which is
		// generally what you pass in here) are 147 bytes long.
		log.Warning("Missing size in hash; arbitrarily guessing 147...")
		a.Size = 147
	} else {
		a.Size, _ = strconv.Atoi(size)
	}
	return nil
}

// ToProto converts this Action to the proto digest.
func (a *Action) ToProto() *pb.Digest {
	return &pb.Digest{
		Hash:      a.Hash,
		SizeBytes: int64(a.Size),
	}
}

// AllToProto converts a slice of actions to protos.
func AllToProto(actions []Action) []*pb.Digest {
	ret := make([]*pb.Digest, len(actions))
	for i, a := range actions {
		ret[i] = a.ToProto()
	}
	return ret
}
