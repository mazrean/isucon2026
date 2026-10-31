// Command gotest produces the raw deflate bytes archive/zip writes for an
// entry created with zip.Writer.Create.
//
//	gotest                 : stdin -> stdout (one Write + Close)
//	gotest -split N        : stdin, written in N roughly equal Write calls
//	gotest -batch IN OUT   : for each file in dir IN, write OUT/<name>.go.deflate
//	                         (single process, so the zip flate writer pool is reused)
package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
)

func zipDeflate(data []byte, split int) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("manifest.json")
	if err != nil {
		panic(err)
	}
	if split <= 1 {
		if _, err := w.Write(data); err != nil {
			panic(err)
		}
	} else {
		step := len(data)/split + 1
		for off := 0; off < len(data); off += step {
			end := min(off+step, len(data))
			if _, err := w.Write(data[off:end]); err != nil {
				panic(err)
			}
		}
	}
	if err := zw.Close(); err != nil {
		panic(err)
	}
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		panic(err)
	}
	rc, err := zr.File[0].OpenRaw()
	if err != nil {
		panic(err)
	}
	out, err := io.ReadAll(rc)
	if err != nil {
		panic(err)
	}
	return out
}

func main() {
	split := flag.Int("split", 1, "number of Write calls")
	batch := flag.Bool("batch", false, "batch mode: IN OUT dirs")
	flag.Parse()
	if *batch {
		in, out := flag.Arg(0), flag.Arg(1)
		ents, err := os.ReadDir(in)
		if err != nil {
			panic(err)
		}
		for _, e := range ents {
			data, err := os.ReadFile(filepath.Join(in, e.Name()))
			if err != nil {
				panic(err)
			}
			res := zipDeflate(data, *split)
			if err := os.WriteFile(filepath.Join(out, e.Name()+".go"+splitSuffix(*split)), res, 0o644); err != nil {
				panic(err)
			}
		}
		return
	}
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}
	os.Stdout.Write(zipDeflate(data, *split))
}

func splitSuffix(n int) string {
	if n <= 1 {
		return ""
	}
	return ".split"
}
