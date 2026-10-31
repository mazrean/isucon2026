package isureal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// ISUREAL_ASIA_LOCALIMG_FSYNC: 新しいディレクトリは祖先まで記録され、sync の後に消える。並行の書き手 (同じ新しい
// shard) も全員成功し、中身は書いたとおり。groupSyncer.wait は syncfs を使わずに自分のファイルを sync して返る。
func TestLocalImgFsyncConcurrentNewDirs(t *testing.T) {
	old := asiaLocalImgFsync
	asiaLocalImgFsync = true
	t.Cleanup(func() { asiaLocalImgFsync = old })
	root := t.TempDir()
	g := &groupSyncer{dir: func() string { return root }}
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := range 64 {
		wg.Go(func() {
			id := fmt.Sprintf("ab%04d", i)
			shard := fmt.Sprintf("s%d", i%4)
			data := filepath.Join(root, "data", shard, id)
			meta := filepath.Join(root, "meta", shard, id)
			body := bytes.Repeat([]byte{byte(i)}, 4096)
			if err := writeFileNoSync(data, body); err != nil {
				errs <- err
				return
			}
			if err := writeFileNoSync(meta, []byte("owner\n1\n")); err != nil {
				errs <- err
				return
			}
			if err := g.wait(data, meta); err != nil {
				errs <- err
				return
			}
			got, err := os.ReadFile(data)
			if err != nil || !bytes.Equal(got, body) {
				errs <- fmt.Errorf("content of %s: %v", data, err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	n := 0
	localImgNewDirs.Range(func(k, _ any) bool { n++; t.Logf("left: %v", k); return true })
	if n != 0 {
		t.Fatalf("%d new dirs left unsynced", n)
	}
	if g.batches.Load() != 64 {
		t.Fatalf("batches=%d want 64 (per-writer sync)", g.batches.Load())
	}
}

func TestLocalImgMarkNewDirs(t *testing.T) {
	root := t.TempDir()
	d := filepath.Join(root, "x", "y", "z")
	localImgMarkNewDirs(d)
	for _, p := range []string{d, filepath.Dir(d), filepath.Join(root, "x")} {
		if _, ok := localImgNewDirs.Load(p); !ok {
			t.Fatalf("%s not marked", p)
		}
	}
	if _, ok := localImgNewDirs.Load(root); ok {
		t.Fatal("existing dir marked")
	}
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(d, "file")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := localImgFsyncPaths([]string{f}); err != nil {
		t.Fatal(err)
	}
	n := 0
	localImgNewDirs.Range(func(any, any) bool { n++; return true })
	if n != 0 {
		t.Fatalf("%d marks left", n)
	}
}
