// Package main implements a very simple zstd compressor for generating test files.
package main

import (
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	w, err := zstd.NewWriter(os.Stdout)
	check(err)
	_, err = io.Copy(os.Stdout, os.Stdin)
	check(err)
	err = w.Close()
	check(err)
}
