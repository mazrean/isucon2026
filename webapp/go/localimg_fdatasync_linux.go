//go:build linux

package isureal

import (
	"os"
	"syscall"
)

func fdatasyncPath(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Fdatasync(int(f.Fd())); err != nil {
		return &os.PathError{Op: "fdatasync", Path: p, Err: err}
	}
	return nil
}
