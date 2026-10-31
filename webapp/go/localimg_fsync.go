package isureal

import (
	"os"
	"path/filepath"
	"sync"
)

// ISUREAL_ASIA_LOCALIMG_FSYNC=1 (既定 0): ASIA_LOCALIMG の永続化を syncfs(2) から、書いたファイルだけの
// fdatasync (並列) + 親ディレクトリの fsync に替える。syncfs はファイルシステム全体の dirty (nginx のログ、
// journal、ほかの画像) を書くので、GP で 1 回 1〜5 秒になっていた (job23341: 10 秒で 2〜13 回、580〜970 件)。
//
// 耐久性 (201 は永続化の後) は同じ:
//   - data と meta は新しい inode に書いてから rename する。fdatasync はデータと、読み出しに要るメタデータ (サイズ) を永続化する。
//   - rename / create のディレクトリのエントリは、親ディレクトリの fsync で永続化する。
//   - MkdirAll が新しく作ったディレクトリは、そのエントリが親に入るので、親 (の親…) も fsync する (localImgMarkNewDirs)。
//   - 書き手は、自分のファイルとディレクトリの sync が全部終わってから返る (グループを待たず、各自が並列に)。
//
// ISUREAL_ASIA_LOCALIMG_FSYNC=gp: GP の窓 (gpWindowDeadline が真) の間だけ fdatasync の経路、それ以外は従来の syncfs のグループ。
// job25603: =1 は asia の GP を過去最多 (682 人) にしたが、d5 の開始の書き込みの山が大きくなって am の転送の書き込みを
// 押しのけた (am d5 119 人)。GP の外では syncfs のグループが書き込みの速さを自然に抑えていた。
var (
	asiaLocalImgFsyncMode = os.Getenv("ISUREAL_ASIA_LOCALIMG_FSYNC") // "1" / "gp" / それ以外 = off
	asiaLocalImgFsync     = asiaLocalImgFsyncMode == "1"
	asiaLocalImgFsyncGP   = asiaLocalImgFsyncMode == "gp"
	localImgInGPFn        func() bool // setupMem (pump) が設定: asia1 の地域の窓が GP か
)

// localImgUseFsync: この永続化を fdatasync の経路にするか (呼ぶたびに判定する)。
func localImgUseFsync() bool {
	if asiaLocalImgFsync {
		return true
	}
	if asiaLocalImgFsyncGP {
		if fn := localImgInGPFn; fn != nil {
			return fn()
		}
	}
	return false
}

var localImgNewDirs sync.Map // dir -> struct{}: 作ったばかりで、親のエントリをまだ fsync していない

// localImgMarkNewDirs: dir とその祖先のうち、まだ存在しないものを記録する (MkdirAll の前に呼ぶ)。
func localImgMarkNewDirs(dir string) {
	for d := dir; ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil || d == filepath.Dir(d) {
			return
		}
		localImgNewDirs.Store(d, struct{}{})
	}
}

// localImgFsyncPaths: 各ファイルを並列に fdatasync し、その後で親ディレクトリ (新しいディレクトリなら祖先も) を fsync する。
func localImgFsyncPaths(paths []string) error {
	var wg sync.WaitGroup
	errs := make([]error, len(paths))
	for i, p := range paths {
		wg.Go(func() { errs[i] = fdatasyncPath(p) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	dirs := map[string]struct{}{}
	for _, p := range paths {
		for d := filepath.Dir(p); ; {
			dirs[d] = struct{}{}
			if _, isNew := localImgNewDirs.Load(d); !isNew {
				break
			}
			d = filepath.Dir(d)
		}
	}
	derrs := make(chan error, len(dirs))
	for d := range dirs {
		wg.Go(func() { derrs <- fsyncPath(d) })
	}
	wg.Wait()
	close(derrs)
	for err := range derrs {
		if err != nil {
			return err
		}
	}
	// 親の fsync が済んだので、新しいディレクトリのエントリは永続化された
	for d := range dirs {
		localImgNewDirs.Delete(d)
	}
	return nil
}
