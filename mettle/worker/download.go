package worker

import (
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
	grpcstatus "google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/rexclient"
)

// maxBlobBatchSize is the maximum size of a single blob batch we'll ever request.
const maxBlobBatchSize = 4012000 // 4000 Kelly-Bootle standard units

// downloadParallelism is the maximum number of parallel downloads we'll allow.
const downloadParallelism = 4

// ioParallelism is the maximum number of parallel disk writes we'll allow.
const ioParallelism = 10

// emptyHash is the sha256 hash of the empty file.
// Technically checking the size is sufficient but we add this as well for general sanity in case something
// else lost the size for some reason (it will be more obvious to debug that mismatch than mysteriously empty files).
const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// downloadDirectory downloads & writes out a single Directory proto and all its children.
func (w *worker) downloadDirectory(digest *pb.Digest) error {
	ts1 := time.Now()
	dirs, err := w.client.GetDirectoryTree(digest)
	if err != nil {
		return err
	}
	ts2 := time.Now()
	m := make(map[string]*pb.Directory, len(dirs))
	for _, dir := range dirs {
		m[digestProto(dir).Hash] = dir
	}
	files := map[string]*pb.FileNode{}
	if err := w.createDirectory(m, files, w.dir, digest); err != nil {
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
	if err := os.MkdirAll(root, os.ModeDir|0775); err != nil {
		return err
	}
	if digest.Hash == emptyHash {
		return nil // Nothing to be done.
	}
	dir, present := dirs[digest.Hash]
	if !present {
		return fmt.Errorf("Missing directory %s", digest.Hash)
	}
	for _, file := range dir.Files {
		if err := common.CheckPath(file.Name); err != nil {
			return err
		} else if err := makeDirIfNeeded(root, file.Name); err != nil {
			return err
		}
		files[path.Join(root, file.Name)] = file
	}
	for _, dir := range dir.Directories {
		if err := common.CheckPath(dir.Name); err != nil {
			return err
		} else if err := w.createDirectory(dirs, files, path.Join(root, dir.Name), dir.Digest); err != nil {
			return err
		}
	}
	for _, sym := range dir.Symlinks {
		if err := common.CheckPath(sym.Name); err != nil {
			return err
		} else if err := makeDirIfNeeded(root, sym.Name); err != nil {
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
		// Optimise out any empty files. The empty blob is surprisingly popular and obviously we always know what
		// it will contain, so save the RPCs.
		if file.Digest.SizeBytes == 0 && file.Digest.Hash == emptyHash {
			fn := filename
			f := file
			g.Go(func() error {
				return w.writeFile(fn, nil, f)
			})
			continue
		}
		if w.fileCache != nil && w.fileCache.Retrieve(file.Digest.Hash, filename, fileMode(file.IsExecutable)) {
			cacheHits.Inc()
			w.cachedBytes += file.Digest.SizeBytes
			continue
		}
		if file.Digest.SizeBytes > maxBlobBatchSize {
			// This blob is big enough that it must always be done on its own.
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
				return w.writeFile(fn, blob.([]byte), f)
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

	log.Debug("Downloading batch of %d files...", len(filenames))
	digests := make([]sdkdigest.Digest, 0, len(filenames))
	compressors := make([]pb.Compressor_Value, 0, len(filenames))
	digestToFilenames := make(map[string][]string, len(filenames))
	for _, f := range filenames {
		d := files[f].Digest
		filenames, present := digestToFilenames[d.Hash]
		if !present {
			digests = append(digests, sdkdigest.NewFromProtoUnvalidated(d))
			compressors = append(compressors, w.compressor(f, d.SizeBytes))
		}
		digestToFilenames[d.Hash] = append(filenames, f)
	}
	responses, err := w.client.BatchDownload(digests, compressors)
	if err != nil {
		return err
	}
	if len(responses) != len(digests) {
		return grpcstatus.Errorf(codes.InvalidArgument, "Unexpected response, requested %d blobs, got %d", len(digests), len(responses))
	}
	for dg, data := range responses {
		if filenames, present := digestToFilenames[dg.Hash]; !present {
			return grpcstatus.Errorf(codes.InvalidArgument, "Unknown digest %s in response", dg.Hash)
			// The below isn't *quite* right since it assumes file modes are consistent across all files with a matching
			// digest, which isn't actually what the protocol says....
		} else if err := w.writeFiles(filenames, data, files[filenames[0]]); err != nil {
			return err
		}
		w.cache.Set(dg.Hash, data, int64(len(data)))
	}
	return nil
}

// downloadFile downloads a single file to disk.
func (w *worker) downloadFile(filename string, file *pb.FileNode) error {
	w.limiter <- struct{}{}
	defer func() { <-w.limiter }()

	log.Debug("Downloading file of %d bytes...", file.Digest.SizeBytes)
	if err := w.client.ReadToFile(sdkdigest.NewFromProtoUnvalidated(file.Digest), filename, w.compressor(filename, file.Digest.SizeBytes) != pb.Compressor_ZSTD); err != nil {
		return grpcstatus.Errorf(grpcstatus.Code(err), "Failed to download file: %s", err)
	} else if err := os.Chmod(filename, fileMode(file.IsExecutable)); err != nil {
		return fmt.Errorf("Failed to chmod file: %s", err)
	}
	w.fileCache.Store(w.dir, filename, file.Digest.Hash)
	return nil
}

// writeFile writes a blob to disk.
func (w *worker) writeFile(filename string, data []byte, f *pb.FileNode) error {
	w.iolimiter <- struct{}{}
	defer func() { <-w.iolimiter }()
	if err := ioutil.WriteFile(filename, data, fileMode(f.IsExecutable)); err != nil {
		return err
	}
	w.fileCache.Store(w.dir, filename, f.Digest.Hash)
	return nil
}

// writeFiles writes a blob to a series of files.
func (w *worker) writeFiles(filenames []string, data []byte, f *pb.FileNode) error {
	mode := fileMode(f.IsExecutable)
	w.iolimiter <- struct{}{}
	defer func() { <-w.iolimiter }()
	// We could potentially be slightly smarter here by writing only the first file and linking others, although
	// first attempts resulted in some odd "file exists" errors.
	for _, fn := range filenames {
		if err := ioutil.WriteFile(fn, data, mode); err != nil {
			return err
		}
	}
	w.fileCache.StoreAny(w.dir, filenames, f.Digest.Hash)
	return nil
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
		return 0555
	}
	return 0444
}

// compressor returns the compressor to use for a given filename
func (w *worker) compressor(filename string, size int64) pb.Compressor_Value {
	if size >= rexclient.CompressionThreshold && shouldCompress(filename) {
		return pb.Compressor_ZSTD
	}
	return pb.Compressor_IDENTITY
}

// shouldCompress returns true if the given filename should be compressed.
func shouldCompress(filename string) bool {
	return !(strings.HasSuffix(filename, ".zip") || strings.HasSuffix(filename, ".pex") ||
		strings.HasSuffix(filename, ".jar") || strings.HasSuffix(filename, ".gz") ||
		strings.HasSuffix(filename, ".bz2") || strings.HasSuffix(filename, ".xz"))
}
