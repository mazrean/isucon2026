//go:build linux && amd64

package isureal

import (
	"fmt"
	"os"
	"syscall"
)

const sysSyncfs = 306 // syncfs(2) on linux/amd64 (not in package syscall)

// syncStoreDir: one syncfs(2) on the filesystem holding dir (covers data,
// inodes and directory entries of every file written before the call).
func syncStoreDir(dir string, _ []string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, _, e := syscall.Syscall(sysSyncfs, f.Fd(), 0, 0); e != 0 {
		return fmt.Errorf("syncfs %s: %w", dir, e)
	}
	return nil
}
