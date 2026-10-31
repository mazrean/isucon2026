//go:build linux && amd64

package isureal

import (
	"os"
	"syscall"
)

const (
	sysSyncFileRange   = 277 // sync_file_range(2) on linux/amd64
	syncFileRangeWrite = 2   // SYNC_FILE_RANGE_WRITE: start writeback of dirty pages, do not wait
)

// kickWriteback: ISUREAL_ASIA_LOCALIMG_WB=1 のとき、書いた直後にこのファイルの writeback を始めさせる (待たない)。
// group sync (syncfs) が走る時点で、データの多くは既にディスクへ向かっているので、sync 1 回が短くなる。
// 耐久性の約束は変わらない (201 は今までどおり syncfs の完了後)。
func kickWriteback(f *os.File) {
	_, _, _ = syscall.Syscall6(sysSyncFileRange, f.Fd(), 0, 0, syncFileRangeWrite, 0, 0)
}
