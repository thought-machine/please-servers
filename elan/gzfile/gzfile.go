// Package gzfile provides a blob implementation akin to fileblob,
// but with (optional) gzip compression to save space.
//
// It does not support any of the signing features or nearly any
// of the attribute stuff either #dealwithit
package gzfile

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/xattr"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"

	"github.com/thought-machine/please-servers/grpcutil"
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, &URLOpener{})
}

// Scheme is the URL scheme gzfile registers its URLOpener under on blob.DefaultMux.
const Scheme = "gzfile"

// xattrName is the xattr name we use to identify whether a file is stored compressed.
const xattrName = "user.elan_gz"

// gzipTag is the attr tag we apply to gzip-compressed files.
var gzipTag = []byte("gzip")

// URLOpener opens file bucket URLs like "gzfile:///foo/bar/baz".
type URLOpener struct{}

// OpenBucketURL opens a blob.Bucket based on u.
func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	return OpenBucket(u.Path)
}

type bucket struct {
	dir string
}

// openBucket creates a driver.Bucket that reads and writes to dir.
// dir will be created if it does not exist.
func openBucket(dir string) (driver.Bucket, error) {
	dir = filepath.Clean(dir)
	info, err := os.Stat(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		} else if err := os.MkdirAll(dir, os.ModeDir|0755); err != nil {
			return nil, err
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	return &bucket{dir: dir}, nil
}

// OpenBucket creates a *blob.Bucket backed by the filesystem and rooted at
// dir. See the package documentation for an example.
func OpenBucket(dir string) (*blob.Bucket, error) {
	drv, err := openBucket(dir)
	if err != nil {
		return nil, err
	}
	return blob.NewBucket(drv), nil
}

func (b *bucket) Close() error {
	return nil
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	switch {
	case os.IsNotExist(err):
		return gcerrors.NotFound
	default:
		return gcerrors.Unknown
	}
}

// ListPaged implements driver.ListPaged, although without any actual concept of paging.
func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	if opts.Delimiter != "" {
		return nil, fmt.Errorf("Unsupported delimiter %s", opts.Delimiter)
	}
	root := path.Join(b.dir, opts.Prefix)
	result := &driver.ListPage{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// We don't generate attrs files, but ignore them for compatibility with fileblob.
		if strings.HasSuffix(path, ".attrs") {
			return nil
		}
		// Strip the <b.dir> prefix from path; +1 is to include the separator.
		key := path[len(b.dir)+1:]
		// Skip all directories. If opts.Delimiter is set, we'll create
		// pseudo-directories later.
		// Note that returning nil means that we'll still recurse into it;
		// we're just not adding a result for the directory itself.
		if info.IsDir() {
			key += "/"
			// Avoid recursing into subdirectories if the directory name already
			// doesn't match the prefix; any files in it are guaranteed not to match.
			if len(key) > len(opts.Prefix) && !strings.HasPrefix(key, opts.Prefix) {
				return filepath.SkipDir
			}
			return nil
		}
		// Skip files/directories that don't match the Prefix.
		if !strings.HasPrefix(key, opts.Prefix) {
			return nil
		}
		result.Objects = append(result.Objects, &driver.ListObject{
			Key:     key,
			ModTime: info.ModTime(),
			Size:    info.Size(),
		})
		return nil
	})
	return result, err
}

// As implements driver.As.
func (b *bucket) As(i interface{}) bool { return false }

// As implements driver.ErrorAs.
func (b *bucket) ErrorAs(err error, i interface{}) bool {
	if perr, ok := err.(*os.PathError); ok {
		if p, ok := i.(**os.PathError); ok {
			*p = perr
			return true
		}
	}
	return false
}

// Attributes implements driver.Attributes.
func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	return nil, gcerrors.Unimplemented
}

// NewRangeReader implements driver.NewRangeReader.
func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	gzr, info, err := b.newReader(key)
	if err != nil {
		return nil, err
	}
	if opts.BeforeRead != nil {
		if err := opts.BeforeRead(func(interface{}) bool { return false }); err != nil {
			return nil, err
		}
	}
	if offset > 0 {
		if _, err := gzr.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	r := io.Reader(gzr)
	if length >= 0 {
		r = io.LimitReader(r, length)
	}
	return &reader{
		r: r,
		c: gzr,
		attrs: driver.ReaderAttributes{
			ModTime: info.ModTime(),
			Size:    info.Size(),
		},
	}, nil
}

func (b *bucket) newReader(key string) (ReadSeekCloser, os.FileInfo, error) {
	f, err := os.Open(path.Join(b.dir, key))
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if tag, err := xattr.FGet(f, xattrName); err == nil && bytes.Equal(tag, gzipTag) {
		r, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, info, err
		}
		return &gzreader{f: f, r: r}, info, nil
	}
	return f, info, nil
}

type reader struct {
	r     io.Reader
	c     io.Closer
	attrs driver.ReaderAttributes
}

func (r *reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r *reader) Close() error {
	return r.c.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

func (r *reader) As(i interface{}) bool { return false }

// NewTypedWriter implements driver.NewTypedWriter.
func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	path := path.Join(b.dir, key)
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return nil, err
	}
	f, err := ioutil.TempFile(filepath.Dir(path), "tmp")
	if err != nil {
		return nil, err
	}
	if opts.BeforeWrite != nil {
		if err := opts.BeforeWrite(func(interface{}) bool { return false }); err != nil {
			return nil, err
		}
	}
	w := &writer{
		ctx:  ctx,
		f:    f,
		path: path,
		temp: f.Name(),
	}
	if grpcutil.ShouldCompress(ctx) {
		if err := xattr.FSet(f, xattrName, gzipTag); err != nil {
			return nil, err
		}
		gzw, err := gzip.NewWriterLevel(f, compressionLevel)
		if err != nil {
			return nil, err
		}
		f.w = gzw
	}
	return w, nil
}

type writer struct {
	ctx  context.Context
	f    io.WriteCloser
	path string
	temp string
}

func (w *writer) Write(p []byte) (n int, err error) {
	return w.f.Write(p)
}

func (w *writer) Close() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	// Check if the write was cancelled.
	if err := w.ctx.Err(); err != nil {
		os.Remove(w.temp)
		return err
	}
	if err := os.Rename(w.temp, w.path); err != nil {
		os.Remove(w.temp)
		return err
	}
	return nil
}

// Copy is not implemented for gzfile.
func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	return gcerrors.Unimplemented
}

// Delete implements driver.Delete.
func (b *bucket) Delete(ctx context.Context, key string) error {
	return os.Remove(path.Join(b.dir, key))
}

// SignedURL is not implemented for gzfile.
func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	return gcerrors.Unimplemented
}
