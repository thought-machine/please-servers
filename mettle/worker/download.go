package worker

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	sdkdigest "github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
)

// downloadDirectory downloads & writes out a single Directory proto and all its children.
// TODO(peterebden): can we replace some of this with GetTree, or otherwise share with src/remote?
func (w *worker) downloadDirectory(root string, digest *pb.Digest) error {
	dir := &pb.Directory{}
	if err := w.readProto(digest, dir); err != nil {
		return fmt.Errorf("Failed to download directory metadata for %s: %s", root, err)
	}
	if err := os.MkdirAll(root, os.ModeDir|0775); err != nil {
		return err
	}
	for _, file := range dir.Files {
		if err := makeDirIfNeeded(root, file.Name); err != nil {
			return err
		}
		filename := path.Join(root, file.Name)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if _, err := w.client.ReadBlobToFile(ctx, sdkdigest.NewFromProtoUnvalidated(file.Digest), filename); err != nil {
			return fmt.Errorf("Failed to download file: %s", err)
		} else if file.IsExecutable {
			if err := os.Chmod(filename, 0755); err != nil {
				return fmt.Errorf("Failed to chmod file: %s", err)
			}
		}
	}
	for _, dir := range dir.Directories {
		if err := makeDirIfNeeded(root, dir.Name); err != nil {
			return err
		}
		if err := w.downloadDirectory(path.Join(root, dir.Name), dir.Digest); err != nil {
			return err
		}
	}
	for _, sym := range dir.Symlinks {
		if err := makeDirIfNeeded(root, sym.Name); err != nil {
			return err
		}
		if err := os.Symlink(sym.Target, path.Join(root, sym.Name)); err != nil {
			return err
		}
	}
	return nil
}

// readProto reads a protobuf from the remote CAS.
// TODO(peterebden): replace with w.client.ReadProto once merged upstream.
func (w *worker) readProto(digest *pb.Digest, msg proto.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	bytes, err := w.client.ReadBlob(ctx, sdkdigest.NewFromProtoUnvalidated(digest))
	if err != nil {
		return err
	}
	return proto.Unmarshal(bytes, msg)
}

// makeDirIfNeeded makes a new subdir if the given name specifies a subdir (i.e. contains a path separator)
func makeDirIfNeeded(root, name string) error {
	if strings.ContainsRune(name, '/') {
		return os.MkdirAll(path.Join(root, path.Dir(name)), os.ModeDir|0755)
	}
	return nil
}
