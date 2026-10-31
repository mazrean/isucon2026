package isureal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ISUREAL_ASIA_LOCALIMG_WB: writeback を始めさせても、書いた中身・rename は変わらない。
func TestLocalImgWritebackKeepsContent(t *testing.T) {
	old := asiaLocalImgWB
	asiaLocalImgWB = true
	t.Cleanup(func() { asiaLocalImgWB = old })
	dir := t.TempDir()
	path := filepath.Join(dir, "a", "data")
	data := bytes.Repeat([]byte{0xff, 0xd8, 1, 2}, 100000)
	if err := writeFileNoSync(path, data); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("content mismatch err=%v len=%d", err, len(got))
	}
	if err := syncStoreDir(dir, []string{path}); err != nil {
		t.Fatal(err)
	}
}

func TestPostTimingAggregates(t *testing.T) {
	old := postTimingOn
	postTimingOn = true
	t.Cleanup(func() { postTimingOn = old })
	postTimingAdd("jpeg", time.Millisecond)
	postTimingAdd("jpeg", 3*time.Millisecond)
	postTiming.mu.Lock()
	n := len(postTiming.d["jpeg"])
	postTiming.mu.Unlock()
	if n < 2 {
		t.Fatalf("recorded %d", n)
	}
}
