//go:build linux

package isureal

import (
	"os"
	"syscall"
)

// fdatasyncFile: 追記したデータ (とサイズ) だけを永続化する。
func fdatasyncFile(f *os.File) error { return syscall.Fdatasync(int(f.Fd())) }
