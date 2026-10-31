//go:build !linux

package isureal

import "os"

func fdatasyncPath(p string) error { return fsyncPath(p) }

func fdatasyncFile(f *os.File) error { return f.Sync() }
