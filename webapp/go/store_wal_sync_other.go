//go:build !linux

package isureal

import "os"

func fdatasyncFile(f *os.File) error { return f.Sync() }
