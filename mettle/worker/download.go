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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/grpcutil"
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

// compressionKey is the context key we use to indicate whether we'll apply compression to an RPC.
type compressionKey struct{}

// downloadDirectory downloads & writes out a single Directory proto and all its children.
func (w *worker) downloadDirectory(digest *pb.Digest) error {
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
	dir, present := dirs[digest.Hash]
	if !present {
		return fmt.Errorf("Missing directory %s", digest.Hash)
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
	digests := make([]*pb.Digest, 0, len(filenames))
	digestToFilenames := make(map[string][]string, len(filenames))
	for _, f := range filenames {
		d := files[f].Digest
		filenames, present := digestToFilenames[d.Hash]
		if !present {
			digests = append(digests, d)
		}
		digestToFilenames[d.Hash] = append(filenames, f)
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
			return grpcstatus.Errorf(codes.Code(r.Status.Code), "Error fetching %s: %s", r.Digest.Hash, r.Status.Message)
		} else if filenames, present := digestToFilenames[r.Digest.Hash]; !present {
			return grpcstatus.Errorf(codes.InvalidArgument, "Unknown digest %s in response", r.Digest.Hash)
			// The below isn't *quite* right since it assumes file modes are consistent across all files with a matching
			// digest, which isn't actually what the protocol says....
		} else if err := w.writeFiles(filenames, r.Data, files[filenames[0]]); err != nil {
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

	log.Debug("Downloading file of %d bytes...", file.Digest.SizeBytes)
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	ctx = context.WithValue(ctx, compressionKey{}, shouldCompress(filename))
	if _, err := w.client.ReadBlobToFile(ctx, sdkdigest.NewFromProtoUnvalidated(file.Digest), filename); err != nil {
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

// shouldCompress returns true if the given filename should be compressed.
func shouldCompress(filename string) bool {
	return !(strings.HasSuffix(filename, ".zip") || strings.HasSuffix(filename, ".pex") ||
		strings.HasSuffix(filename, ".jar") || strings.HasSuffix(filename, ".gz"))
}

// shouldCompressAll returns true if all of the given filenames should be compressed.
func shouldCompressAll(filenames []string) bool {
	for _, f := range filenames {
		if shouldCompress(f) {
			return true
		}
	}
	return false
}

// unaryCompressionInterceptor applies compression to unary RPCs based on the context.
func unaryCompressionInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, opts = compressionInterceptor(ctx, opts)
	return invoker(ctx, method, req, reply, cc, opts...)
}

// streamCompressionInterceptor applies compression to streaming RPCs based on the given context.
func streamCompressionInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, opts = compressionInterceptor(ctx, opts)
	return streamer(ctx, desc, cc, method, opts...)
}

func compressionInterceptor(ctx context.Context, opts []grpc.CallOption) (context.Context, []grpc.CallOption) {
	if v := ctx.Value(compressionKey{}); v == nil || v.(bool) {
		return ctx, append(opts, grpc.UseCompressor(gzip.Name))
	}
	return grpcutil.SkipCompression(ctx), opts
}
