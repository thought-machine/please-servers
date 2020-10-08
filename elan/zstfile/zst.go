package zstfile

import (
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// A ReadSeekCloser combines the three standard io interfaces.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// A zstdreader implements gzip compression over a file.
type zstdreader struct {
	f *os.File
	r *zstd.Decoder
	p *sync.Pool
}

// Read normally passes through to our
func (r *zstdreader) Read(buf []byte) (int, error) {
	return r.r.Read(buf)
}

func (r *zstdreader) Close() error {
	r.p.Put(r.r)
	return r.f.Close()
}

// N.B. Seek is not a general-purpose Seek implementation; we know that we only call it once
//      upfront to seek forward from the start of the file.
func (r *zstdreader) Seek(offset int64, whence int) (int64, error) {
	_, err := io.Copy(ioutil.Discard, &io.LimitedReader{R: r.r, N: offset})
	return offset, err
}

func finalizeReader(r *zstdreader) {
	r.r.Close()
}

// A zstdwriter implements gzip compression over a file.
type zstdwriter struct {
	f *os.File
	w *zstd.Encoder
	p *sync.Pool
}

func (w *zstdwriter) Write(buf []byte) (int, error) {
	return w.w.Write(buf)
}

func (w *zstdwriter) Close() error {
	defer w.p.Put(w.w)
	if err := w.w.Close(); err != nil {
		w.f.Close() // Must still close the file
		return err
	}
	return w.f.Close()
}

func finalizeWriter(w *zstdwriter) {
	w.w.Close()
}
