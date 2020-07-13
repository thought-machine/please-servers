package gzfile

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
)

// compressionLevel is the compression level we use.
const compressionLevel = gzip.DefaultCompression

// A ReadSeekCloser combines the three standard io interfaces.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// A gzreader implements gzip compression over a file.
type gzreader struct {
	f *os.File
	r *gzip.Reader
}

// Read normally passes through to our
func (r *gzreader) Read(buf []byte) (int, error) {
	return r.r.Read(buf)
}

func (r *gzreader) Close() error {
	defer r.f.Close()
	return r.r.Close()
}

// N.B. Seek is not a general-purpose Seek implementation; we know that we only call it once
//      upfront to seek forward from the start of the file.
func (r *gzreader) Seek(offset int64, whence int) (int64, error) {
	_, err := io.Copy(ioutil.Discard, &io.LimitedReader{R: r.r, N: offset})
	return offset, err
}

// A gzwriter implements gzip compression over a file.
type gzwriter struct {
	f *os.File
	w *gzip.Writer
}

func (w *gzwriter) Write(buf []byte) (int, error) {
	return w.w.Write(buf)
}

func (w *gzwriter) Close() error {
	if err := w.w.Close(); err != nil {
		w.f.Close() // Must still close the file
		return err
	}
	return w.f.Close()
}
