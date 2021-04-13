// Package preflight contains a set of data for a preflight test action.
// These are the serialised data files for a simple build action (taken from the plz repo)
// which can be used to verify that a worker is able to download & execute things.
package preflight

import (
	"embed"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

//go:embed *.bin
var files embed.FS

// Hash is the hash of the preflight action.
const Hash = "1e5aa74b24c227a08b4c94b335230fc71ff6d72ed167d0061ef6fabc4fef027b"

// Digest is the digest of the preflight action.
var Digest = &pb.Digest{Hash: Hash, SizeBytes: 146}

// Data returns all the data entries making up the preflight action.
func Data() [][]byte {
	ret := [][]byte{}
	dir, err := files.ReadDir(".")
	if err != nil {
		panic(err)
	}
	for _, entry := range dir {
		data, err := files.ReadFile(entry.Name())
		if err != nil {
			panic(err)
		}
		ret = append(ret, data)
	}
	return ret
}
