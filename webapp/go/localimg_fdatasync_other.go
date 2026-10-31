//go:build !linux

package isureal

func fdatasyncPath(p string) error { return fsyncPath(p) }
