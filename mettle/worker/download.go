package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	sdkdigest "github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

// maxBlobBatchSize is the maximum size of a single blob batch we'll ever request.
const maxBlobBatchSize = 4012000 // 4000 Kelly-Bootle standard units

// downloadParallelism is the maximum number of parallel downloads we'll allow.
const downloadParallelism = 4

// ioParallelism is the maximum number of parallel disk writes we'll allow.
const ioParallelism = 10

// downloadDirectory downloads & writes out a single Directory proto and all its children.
func (w *worker) downloadDirectory(root string, digest *pb.Digest) error {
	ts1 := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	dirs, err := w.client.GetDirectoryTree(ctx, digest)
	if err != nil {
		return err
	}
	ts2 := time.Now()
	m := make(map[string]*pb.Directory, len(dirs))
	for _, dir := range dirs {
		m[digestProto(dir).Hash] = dir
	}
	files := map[string]*pb.FileNode{}
	if err := w.createDirectory(m, files, root, digest); err != nil {
		return err
	}
	ts3 := time.Now()
	err = w.downloadAllFiles(files)
	w.metadataFetch = ts2.Sub(ts1)
	w.dirCreation = ts3.Sub(ts2)
	w.fileDownload = time.Since(ts3)
	return err
}

// createDirectory creates a directory & all its children
func (w *worker) createDirectory(dirs map[string]*pb.Directory, files map[string]*pb.FileNode, root string, digest *pb.Digest) error {
	dir, present := dirs[digest.Hash]
	if !present {
		return fmt.Errorf("Missing directory %s", digest.Hash)
	}
	log.Debug("creating dir %s [%s]", root, digest.Hash)
	if len(dir.Files) > 0 {
		log.Debug("Files: ")
		for _, file := range dir.Files {
			log.Debug("  %s [%s/%d]", file.Name, file.Digest.Hash, file.Digest.SizeBytes)
		}
	}
	if err := os.MkdirAll(root, os.ModeDir|0775); err != nil {
		return err
	}
	for _, file := range dir.Files {
		if err := makeDirIfNeeded(root, file.Name); err != nil {
			return err
		}
		files[path.Join(root, file.Name)] = file
	}
	for _, dir := range dir.Directories {
		if err := w.createDirectory(dirs, files, path.Join(root, dir.Name), dir.Digest); err != nil {
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

// downloadAllFiles downloads all the files for a single build action.
func (w *worker) downloadAllFiles(files map[string]*pb.FileNode) error {
	var g errgroup.Group

	filenames := []string{}
	var totalSize int64
	for filename, file := range files {
		if w.fileCache != nil && w.fileCache.Retrieve(file.Digest.Hash, filename, fileMode(file.IsExecutable)) {
			cacheHits.Inc()
			w.cachedBytes += file.Digest.SizeBytes
			continue
		}
		if file.Digest.SizeBytes > maxBlobBatchSize {
			// This blob is big enough that it must always be done on its own.
			totalSize += file.Digest.SizeBytes
			w.downloadedBytes += file.Digest.SizeBytes
			fn := filename
			f := file
			g.Go(func() error { return w.downloadFile(fn, f) })
			continue
		}
		// Check cache for this blob (we never cache blobs that are big enough not to be batchable)
		if blob, present := w.cache.Get(file.Digest.Hash); present {
			cacheHits.Inc()
			w.cachedBytes += file.Digest.SizeBytes
			fn := filename
			f := file
			g.Go(func() error {
				return w.writeFile(fn, blob.([]byte), fileMode(f.IsExecutable))
			})
			continue
		}
		cacheMisses.Inc()
		if totalSize+file.Digest.SizeBytes > maxBlobBatchSize {
			// This blob on its own is OK but will exceed the total.
			// Download what we have so far then deal with it.
			fs := filenames[:]
			g.Go(func() error { return w.downloadFiles(fs, files) })
			filenames = []string{}
			totalSize = 0
		}
		filenames = append(filenames, filename)
		totalSize += file.Digest.SizeBytes
		w.downloadedBytes += file.Digest.SizeBytes
	}
	// If we have anything left over, handle them now.
	if len(filenames) != 0 {
		g.Go(func() error { return w.downloadFiles(filenames, files) })
	}
	return g.Wait()
}

// downloadFiles downloads a set of files to disk in a batch.
// The total size must be lower than whatever limits are considered relevant.
func (w *worker) downloadFiles(filenames []string, files map[string]*pb.FileNode) error {
	w.limiter <- struct{}{}
	defer func() { <-w.limiter }()

	digests := make([]*pb.Digest, len(filenames))
	digestToFilename := make(map[string]string, len(filenames))
	for i, f := range filenames {
		digests[i] = files[f].Digest
		digestToFilename[files[f].Digest.Hash] = f
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	resp, err := w.client.BatchReadBlobs(ctx, &pb.BatchReadBlobsRequest{
		InstanceName: w.client.InstanceName,
		Digests:      digests,
	})
	if err != nil {
		return err
	}
	for _, r := range resp.Responses {
		if r.Status.Code != int32(codes.OK) {
			return fmt.Errorf("%s", r.Status.Message)
		} else if filename, present := digestToFilename[r.Digest.Hash]; !present {
			return fmt.Errorf("Unknown digest %s in response", r.Digest.Hash)
		} else if err := w.writeFile(filename, r.Data, fileMode(files[filename].IsExecutable)); err != nil {
			return err
		}
		w.cache.Set(r.Digest.Hash, r.Data, int64(len(r.Data)))
	}
	return nil
}

// downloadFile downloads a single file to disk.
func (w *worker) downloadFile(filename string, file *pb.FileNode) error {
	w.limiter <- struct{}{}
	defer func() { <-w.limiter }()

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	if _, err := w.client.ReadBlobToFile(ctx, sdkdigest.NewFromProtoUnvalidated(file.Digest), filename); err != nil {
		return fmt.Errorf("Failed to download file: %s", err)
	} else if file.IsExecutable {
		if err := os.Chmod(filename, 0755); err != nil {
			return fmt.Errorf("Failed to chmod file: %s", err)
		}
	}
	return nil
}

// writeFile writes a blob to disk.
func (w *worker) writeFile(filename string, data []byte, mode os.FileMode) error {
	w.iolimiter <- struct{}{}
	defer func() { <-w.iolimiter }()
	return ioutil.WriteFile(filename, data, mode)
}

// makeDirIfNeeded makes a new subdir if the given name specifies a subdir (i.e. contains a path separator)
func makeDirIfNeeded(root, name string) error {
	if strings.ContainsRune(name, '/') {
		return os.MkdirAll(path.Join(root, path.Dir(name)), os.ModeDir|0755)
	}
	return nil
}

// digestProto returns a digest for a proto message.
func digestProto(msg proto.Message) *pb.Digest {
	blob, _ := proto.Marshal(msg)
	h := sha256.Sum256(blob)
	return &pb.Digest{Hash: hex.EncodeToString(h[:]), SizeBytes: int64(len(blob))}
}

func fileMode(isExecutable bool) os.FileMode {
	if isExecutable {
		return 0755
	}
	return 0644
}
