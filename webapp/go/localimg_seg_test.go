package isureal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func segImg(i int) []byte { return bytes.Repeat([]byte{0xff, 0xd8, byte(i)}, 1000+i) }

func reopenSeg(t *testing.T, dir string) *segStore {
	t.Helper()
	s := &segStore{}
	if err := s.open(dir); err != nil {
		t.Fatal(err)
	}
	return s
}

func mustGet(t *testing.T, s *segStore, id, owner string, want []byte) {
	t.Helper()
	obj, _ := s.get(id)
	if obj == nil || obj.Owner != owner || !bytes.Equal(obj.Data, want) {
		t.Fatalf("get %s: %+v", id, obj)
	}
}

// 再起動 (開き直し) の後も読める。更新は後勝ち、削除は再起動の後も消えたまま。並行の put (group commit) も全部残る。
func TestSegStoreSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s := reopenSeg(t, dir)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			if err := s.put(fmt.Sprintf("id%03d", i), "owner", int64(i), segImg(i)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := s.put("id007", "owner2", 99, segImg(77)); err != nil { // 更新
		t.Fatal(err)
	}
	s.del("id010")
	s2 := reopenSeg(t, dir)
	for i := range 50 {
		id := fmt.Sprintf("id%03d", i)
		switch i {
		case 7:
			mustGet(t, s2, id, "owner2", segImg(77))
		case 10:
			if obj, _ := s2.get(id); obj != nil {
				t.Fatal("deleted image came back after restart")
			}
		default:
			mustGet(t, s2, id, "owner", segImg(i))
		}
	}
	if _, at := s2.get("id003"); at.UnixNano() != 3 {
		t.Fatalf("createdAt %v", at)
	}
}

// 書きかけのレコード (途中で切れた / crc が合わない) は復旧で切り捨て、その前は読める。後から書いたものも残る。
func TestSegStoreTruncatesTornTail(t *testing.T) {
	for _, mode := range []string{"short", "crc", "garbage"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s := reopenSeg(t, dir)
			for i := range 3 {
				if err := s.put(fmt.Sprintf("a%d", i), "o", 1, segImg(i)); err != nil {
					t.Fatal(err)
				}
			}
			path := s.path(s.cur.n)
			good, _ := os.Stat(path)
			goodSize := good.Size()
			last := segEncode(segKindPut, "torn", "o", 1, segImg(9))
			f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			switch mode {
			case "short":
				_, _ = f.Write(last[:len(last)/2])
			case "crc":
				last[len(last)-1] ^= 0xff
				_, _ = f.Write(last)
			case "garbage":
				_, _ = f.Write([]byte("not a record at all"))
			}
			f.Close()
			s2 := reopenSeg(t, dir)
			for i := range 3 {
				mustGet(t, s2, fmt.Sprintf("a%d", i), "o", segImg(i))
			}
			if obj, _ := s2.get("torn"); obj != nil {
				t.Fatal("torn record was indexed")
			}
			if st, _ := os.Stat(path); st.Size() != goodSize {
				t.Fatalf("not truncated: %d want %d", st.Size(), goodSize)
			}
			if err := s2.put("after", "o", 2, segImg(5)); err != nil {
				t.Fatal(err)
			}
			mustGet(t, reopenSeg(t, dir), "after", "o", segImg(5))
		})
	}
}

// ローテーション: 上限を超えると次のセグメントへ。再起動の後、全セグメントの画像が読める (後のセグメントの更新が勝つ)。
func TestSegStoreRotation(t *testing.T) {
	old := segMaxBytes
	segMaxBytes = 8 << 10
	t.Cleanup(func() { segMaxBytes = old })
	dir := t.TempDir()
	s := reopenSeg(t, dir)
	for i := range 20 {
		if err := s.put(fmt.Sprintf("r%02d", i), "o", 1, segImg(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.put("r00", "new", 1, segImg(42)); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(filepath.Join(dir, "seg"))
	if len(ents) < 3 {
		t.Fatalf("segments=%d, want rotation", len(ents))
	}
	s2 := reopenSeg(t, dir)
	mustGet(t, s2, "r00", "new", segImg(42))
	for i := 1; i < 20; i++ {
		mustGet(t, s2, fmt.Sprintf("r%02d", i), "o", segImg(i))
	}
}

// initialize: ディレクトリごと消えた後の reset で索引は空になり、新しい世代に書ける。
func TestSegStoreReset(t *testing.T) {
	dir := t.TempDir()
	s := reopenSeg(t, dir)
	if err := s.put("x", "o", 1, segImg(1)); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil { // asiaStore.purge 相当
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s.reset(dir)
	if obj, _ := s.get("x"); obj != nil {
		t.Fatal("image survived initialize")
	}
	if err := s.put("y", "o", 1, segImg(2)); err != nil {
		t.Fatal(err)
	}
	mustGet(t, reopenSeg(t, dir), "y", "o", segImg(2))
}

// ISUREAL_ASIA_LOCALIMG_SEG: 入口 (asiaLocalImgKeep / localImgOwn / Drop / Purge) を通しても、セグメントに入り、
// 開き直しても読め、Drop で消え、Purge で空になる。従来のファイルは作らない。
func TestAsiaLocalImgSegIntegration(t *testing.T) {
	setupAsiaLocalImgTest(t)
	old := asiaLocalImgSeg
	asiaLocalImgSeg = true
	t.Cleanup(func() { asiaLocalImgSeg = old; asiaSeg = &segStore{} })
	asiaSeg = &segStore{}
	if err := asiaSeg.open(asiaStore.dir); err != nil {
		t.Fatal(err)
	}
	good := testJPEG(t, 120, 90)
	id := newAsiaLocalImageID()
	if !asiaLocalImgKeep(t.Context(), id, "owner-1", good) {
		t.Fatal("not kept")
	}
	if own := localImgOwn(id); own == nil || own.Owner != "owner-1" || !bytes.Equal(own.Data, good) {
		t.Fatalf("own %v", own)
	}
	if _, err := os.Stat(asiaStore.p("data", id)); err == nil {
		t.Fatal("per-file data written in seg mode")
	}
	if obj, _ := reopenSeg(t, asiaStore.dir).get(id); obj == nil || !bytes.Equal(obj.Data, good) {
		t.Fatal("not readable after reopen")
	}
	asiaLocalImgDrop(id)
	if localImgOwn(id) != nil {
		t.Fatal("dropped image still served")
	}
	id2 := newAsiaLocalImageID()
	if !asiaLocalImgKeep(t.Context(), id2, "o", good) {
		t.Fatal("not kept")
	}
	asiaLocalImgPurge()
	if localImgOwn(id2) != nil {
		t.Fatal("image survived purge")
	}
	id3 := newAsiaLocalImageID()
	if !asiaLocalImgKeep(t.Context(), id3, "o", good) || localImgOwn(id3) == nil {
		t.Fatal("keep after purge")
	}
}
