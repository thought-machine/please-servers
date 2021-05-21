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

// A fileNode contains the minimal fields we need to write a pb.FileNode.
type fileNode struct {
	Name         string
	IsExecutable bool
}

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
	files := map[sdkdigest.Digest][]fileNode{}
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
func (w *worker) createDirectory(dirs map[string]*pb.Directory, files map[sdkdigest.Digest][]fileNode, root string, digest *pb.Digest) error {
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
		dg := sdkdigest.NewFromProtoUnvalidated(file.Digest)
		files[dg] = append(files[dg], fileNode{
			Name:         path.Join(root, file.Name),
			IsExecutable: file.IsExecutable},
		)
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
func (w *worker) downloadAllFiles(files map[sdkdigest.Digest][]fileNode) error {
	var g errgroup.Group

	fileNodes := map[sdkdigest.Digest][]fileNode{}
	var totalSize int64
	for dg, filenames := range files {
		// Optimise out any empty files. The empty blob is surprisingly popular and obviously we always know what
		// it will contain, so save the RPCs.
		if dg.Size == 0 && dg.Hash == emptyHash {
			fns := filenames
			dg := dg
			g.Go(func() error {
				return w.writeFiles(fns, nil, dg)
			})
			continue
		}
		if w.fileCache != nil && w.fileCache.Retrieve(dg.Hash, filenames[0].Name, fileMode(filenames[0].IsExecutable)) {
			cacheHits.Inc()
			w.cachedBytes += dg.Size
			if err := w.linkAll(filenames); err != nil {
				return err
			}
			continue
		}
		if dg.Size > maxBlobBatchSize {
			// This blob is big enough that it must always be done on its own.
			w.downloadedBytes += dg.Size
			fns := filenames
			dg := dg
			g.Go(func() error { return w.downloadFile(fns, dg) })
			continue
		}
		// Check cache for this blob (we never cache blobs that are big enough not to be batchable)
		if blob, present := w.cache.Get(dg.Hash); present {
			cacheHits.Inc()
			w.cachedBytes += dg.Size
			fns := filenames
			dg := dg
			g.Go(func() error {
				return w.writeFiles(fns, blob.([]byte), dg)
			})
			continue
		}
		cacheMisses.Inc()
		if totalSize+dg.Size > maxBlobBatchSize {
			// This blob on its own is OK but will exceed the total.
			// Download what we have so far then deal with it.
			fns := fileNodes
			g.Go(func() error { return w.downloadFiles(fns) })
			fileNodes = map[sdkdigest.Digest][]fileNode{}
			totalSize = 0
		}
		fileNodes[dg] = filenames
		totalSize += dg.Size
		w.downloadedBytes += dg.Size
	}
	// If we have anything left over, handle them now.
	if len(fileNodes) != 0 {
		g.Go(func() error { return w.downloadFiles(fileNodes) })
	}
	return g.Wait()
}

// downloadFiles downloads a set of files to disk in a batch.
// The total size must be lower than whatever limits are considered relevant.
func (w *worker) downloadFiles(files map[sdkdigest.Digest][]fileNode) error {
	w.limiter <- struct{}{}
	defer func() { <-w.limiter }()

	if w.redis != nil {
		remaining, err := w.downloadRedis(files)
		if err != nil {
			return err
		} else if len(remaining) == 0 {
			return nil // we fetched everything from Redis \o/
		}
		files = remaining
	}

	log.Debug("Downloading batch of %d files...", len(files))
	digests := make([]sdkdigest.Digest, 0, len(files))
	compressors := make([]pb.Compressor_Value, 0, len(files))
	for dg, filenames := range files {
		digests = append(digests, dg)
		compressors = append(compressors, w.compressor(filenames, dg.Size))
	}
	responses, err := w.client.BatchDownload(digests, compressors)
	if err != nil {
		return err
	}
	if len(responses) != len(digests) {
		return grpcstatus.Errorf(codes.InvalidArgument, "Unexpected response, requested %d blobs, got %d", len(digests), len(responses))
	}
	for dg, data := range responses {
		if fileNodes, present := files[dg]; !present {
			return grpcstatus.Errorf(codes.InvalidArgument, "Unknown digest %s in response", dg.Hash)
		} else if err := w.writeFiles(fileNodes, data, dg); err != nil {
			return err
		}
		w.cache.Set(dg.Hash, data, int64(len(data)))
	}
	if w.redis != nil {
		// We can do this async, we don't need to wait for it to complete.
		go w.uploadRedis(responses)
	}
	return nil
}

// downloadRedis downloads a batch of files from Redis to disk.
// It returns the set of files still to download.
func (w *worker) downloadRedis(files map[sdkdigest.Digest][]fileNode) (map[sdkdigest.Digest][]fileNode, error) {
	log.Debug("Checking Redis for batch of %d files...", len(files))
	keys := make([]string, 0, len(files))
	dgs := make([]sdkdigest.Digest, 0, len(files))
	for k := range files {
		keys = append(keys, k.Hash)
		dgs = append(dgs, k)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	resp, err := w.redis.MGet(ctx, keys...).Result()
	if err != nil {
		log.Warning("Failed to retrieve blobs from Redis: %s", err)
		return files, nil
	}
	if len(resp) != len(keys) {
		log.Warning("Length mismatch in Redis response; got %d, expected %d", len(resp), len(keys))
		// If the lengths don't match we don't know which is which so it's useless to us.
		// This shouldn't happen but we don't want to assume it never does and blindly index...
		return files, nil
	}
	remaining := make(map[sdkdigest.Digest][]fileNode, len(files))
	for i, blob := range resp {
		dg := dgs[i]
		file := files[dg]
		if blob == nil { // Redis didn't have it
			remaining[dg] = file
			continue
		}
		b, ok := blob.([]byte)
		if !ok {
			log.Warning("Failed to cast Redis response to []byte")
			remaining[dg] = file
			continue
		}
		if err := w.writeFiles(file, b, dg); err != nil {
			return nil, err
		}
		w.redisBytes += dg.Size
	}
	return remaining, nil
}

// uploadRedis uploads a set of blobs to Redis.
func (w *worker) uploadRedis(blobs map[sdkdigest.Digest][]byte) {
	m := make(map[string]interface{}, len(blobs))
	for k, v := range blobs {
		m[k.Hash] = v
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	if _, err := w.redis.MSet(ctx, m).Result(); err != nil {
		log.Warning("Failed to set blobs in Redis: %s", err)
	}
}

// downloadFile downloads a single file to disk.
func (w *worker) downloadFile(files []fileNode, dg sdkdigest.Digest) error {
	w.limiter <- struct{}{}
	defer func() { <-w.limiter }()

	log.Debug("Downloading file of %d bytes...", dg.Size)
	filename := files[0].Name
	if err := w.client.ReadToFile(dg, filename, w.compressor(files, dg.Size) != pb.Compressor_ZSTD); err != nil {
		return grpcstatus.Errorf(grpcstatus.Code(err), "Failed to download file: %s", err)
	} else if err := os.Chmod(filename, fileMode(files[0].IsExecutable)); err != nil {
		return fmt.Errorf("Failed to chmod file: %s", err)
	}
	w.fileCache.Store(w.dir, filename, dg.Hash)
	return w.linkAll(files)
}

// linkAll hardlinks all the given files in the list (assuming the first has already been written)
func (w *worker) linkAll(files []fileNode) error {
	if len(files) == 1 {
		return nil
	}
	done := map[string]struct{}{
		files[0].Name: {},
	}
	for _, f := range files[1:] {
		// This should technically not be necessary (REAPI says all child names must be unique)
		// but we have observed it happen occasionally, so let's defend against it for now.
		if _, present := done[f.Name]; !present {
			if err := os.Link(files[0].Name, f.Name); err != nil {
				return err
			}
			done[f.Name] = struct{}{}
		}
	}
	return nil
}

// writeFiles writes a blob to a series of files.
func (w *worker) writeFiles(files []fileNode, data []byte, dg sdkdigest.Digest) error {
	w.iolimiter <- struct{}{}
	defer func() { <-w.iolimiter }()
	if err := ioutil.WriteFile(files[0].Name, data, fileMode(files[0].IsExecutable)); err != nil {
		return err
	} else if err := w.linkAll(files); err != nil {
		return err
	}
	w.fileCache.StoreAny(w.dir, files, dg.Hash)
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

// compressor returns the compressor to use for downloading the digest for a given set of files.
func (w *worker) compressor(fileNodes []fileNode, size int64) pb.Compressor_Value {
	// Using the first filename only here is a little suboptimal but we assume it won't
	// typically matter (since it is only an optimisation to choose whether to request
	// compression or not). We could potentially find another strategy in future if we
	// decide this is significantly suboptimal.
	return w.oneCompressor(fileNodes[0].Name, size)
}

// oneCompressor returns the compressor to use for a given filename
func (w *worker) oneCompressor(filename string, size int64) pb.Compressor_Value {
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
