package isureal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ISUREAL_ASIA_LOCALIMG_SEG=1 (既定 0): asia1 の画像の正本 (ASIA_LOCALIMG) を、画像ごとの data/meta ファイル
// (temp + rename + ディレクトリの sync) ではなく、追記セグメント <dir>/seg/NNNNNNNN.seg にまとめる。
//
// レコード: [magic u32][crc32c u32][kind u8][idLen u8][ownerLen u16][createdAt i64][dataLen u32] id owner data。
// crc は kind 以降すべて (ヘッダの残り + 本体)。kind 1 = 保存、2 = 削除 (tombstone、owner/data 無し)。
//
// 永続化 (応答の前):
//   - 追記はセグメントの排他の中で順に書き、書き手は「自分のレコードの終わり <= durable」まで待つ。
//     fdatasync は 1 本ずつ (走っている間に来た書き手は次の 1 回にまとまる = group commit)。
//   - 新しいセグメントは作った直後に親ディレクトリを fsync してから使う (ファイルのエントリの永続化)。
//   - 索引 (ID → セグメント, 位置) には durable になってから入れる。読み手は索引だけを見る。
//
// 復旧 (起動時): セグメントを番号順に読み、magic / 長さ / crc が合わない所で打ち切り、そのファイルをそこで切り詰める
// (書きかけのレコード)。同じ ID は後のレコードが勝つ (更新)、tombstone は索引から消す。
// 削除 (asiaLocalImgDrop): tombstone を追記する (sync は待たない: 消し損ねても参照の無い画像が残るだけ)。
// initialize (purge): asiaStore.purge がディレクトリごと消すので、索引とファイルを捨てて世代を進める。
// ローテーション: セグメントが memSegMax を超えたら、古い方を最後まで sync してから次の番号へ。
// 移行: 読みはセグメントの次に従来のファイルを見る (切り替え前の画像も読める)。
var (
	asiaLocalImgSeg = os.Getenv("ISUREAL_ASIA_LOCALIMG_SEG") == "1"
	asiaSeg         = &segStore{}
	segMaxBytes     = int64(envIntOrDefault("ISUREAL_ASIA_LOCALIMG_SEG_MB", 512)) << 20
)

const (
	segMagic   = 0x49534731 // "ISG1"
	segHdrLen  = 4 + 4 + 1 + 1 + 2 + 8 + 4
	segKindPut = 1
	segKindDel = 2
)

var segCRC = crc32.MakeTable(crc32.Castagnoli)

type segLoc struct {
	seg       *segFile
	off       int64 // レコードの先頭
	dataOff   int64
	dataLen   int
	owner     string
	createdAt int64
}

type segFile struct {
	n    int
	f    *os.File
	size int64 // 書いた終わり
	// durable: ここまでは fdatasync 済み
	durable int64
	syncing bool
}

type segStore struct {
	mu    sync.Mutex
	cond  *sync.Cond
	dir   string
	cur   *segFile
	files []*segFile
	index map[string]segLoc
	gen   int64
}

func segEncode(kind byte, id, owner string, createdAt int64, data []byte) []byte {
	b := make([]byte, segHdrLen+len(id)+len(owner)+len(data))
	binary.LittleEndian.PutUint32(b[0:], segMagic)
	b[8] = kind
	b[9] = byte(len(id))
	binary.LittleEndian.PutUint16(b[10:], uint16(len(owner)))
	binary.LittleEndian.PutUint64(b[12:], uint64(createdAt))
	binary.LittleEndian.PutUint32(b[20:], uint32(len(data)))
	p := segHdrLen
	p += copy(b[p:], id)
	p += copy(b[p:], owner)
	copy(b[p:], data)
	binary.LittleEndian.PutUint32(b[4:], crc32.Checksum(b[8:], segCRC))
	return b
}

// open: dir/seg を読み直して索引を作る (起動時、purge の後)。
func (s *segStore) open(dir string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cond == nil {
		s.cond = sync.NewCond(&s.mu)
	}
	s.closeLocked()
	s.dir = filepath.Join(dir, "seg")
	s.index = map[string]segLoc{}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var nums []int
	for _, e := range ents {
		var n int
		if _, err := fmt.Sscanf(e.Name(), "%08d.seg", &n); err == nil && strings.HasSuffix(e.Name(), ".seg") {
			nums = append(nums, n)
		}
	}
	sort.Ints(nums)
	for _, n := range nums {
		sf, err := s.recoverFile(n)
		if err != nil {
			return err
		}
		s.files = append(s.files, sf)
		s.cur = sf
	}
	return nil
}

func (s *segStore) path(n int) string { return filepath.Join(s.dir, fmt.Sprintf("%08d.seg", n)) }

// recoverFile: レコードを順に検証して索引に入れ、壊れた所 (書きかけ) で切り詰める。
func (s *segStore) recoverFile(n int) (*segFile, error) {
	f, err := os.OpenFile(s.path(n), os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	sf := &segFile{n: n, f: f}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	total := st.Size()
	var off int64
	hdr := make([]byte, segHdrLen)
	for off+segHdrLen <= total {
		if _, err := f.ReadAt(hdr, off); err != nil {
			break
		}
		if binary.LittleEndian.Uint32(hdr[0:]) != segMagic {
			break
		}
		idLen, ownerLen := int(hdr[9]), int(binary.LittleEndian.Uint16(hdr[10:]))
		dataLen := int(binary.LittleEndian.Uint32(hdr[20:]))
		recLen := int64(segHdrLen + idLen + ownerLen + dataLen)
		if off+recLen > total {
			break
		}
		rec := make([]byte, recLen)
		if _, err := f.ReadAt(rec, off); err != nil {
			break
		}
		if crc32.Checksum(rec[8:], segCRC) != binary.LittleEndian.Uint32(rec[4:]) {
			break
		}
		id := string(rec[segHdrLen : segHdrLen+idLen])
		switch rec[8] {
		case segKindPut:
			s.index[id] = segLoc{seg: sf, off: off, dataOff: off + int64(segHdrLen+idLen+ownerLen), dataLen: dataLen,
				owner: string(rec[segHdrLen+idLen : segHdrLen+idLen+ownerLen]), createdAt: int64(binary.LittleEndian.Uint64(rec[12:]))}
		case segKindDel:
			delete(s.index, id)
		}
		off += recLen
	}
	if off < total {
		slog.Warn("asia localimg seg: truncating torn tail", "seg", n, "at", off, "size", total)
		if err := f.Truncate(off); err != nil {
			return nil, err
		}
		if err := fdatasyncFile(f); err != nil {
			return nil, err
		}
	}
	sf.size, sf.durable = off, off
	return sf, nil
}

func (s *segStore) closeLocked() {
	for _, sf := range s.files {
		sf.f.Close()
	}
	s.files, s.cur = nil, nil
}

// segmentLocked: 追記先。無い / 大きすぎるなら新しいセグメントを作る (親ディレクトリを fsync してから使う)。
func (s *segStore) segmentLocked(need int64) (*segFile, error) {
	if s.cur != nil && s.cur.size+need <= segMaxBytes {
		return s.cur, nil
	}
	if s.cur != nil {
		// ローテーション: 古い方の未 sync 分を、ここで最後まで永続化する (待っている書き手も解放される)
		for s.cur.syncing {
			s.cond.Wait()
		}
		if s.cur.durable < s.cur.size {
			if err := fdatasyncFile(s.cur.f); err != nil {
				return nil, err
			}
			s.cur.durable = s.cur.size
			s.cond.Broadcast()
		}
	}
	n := 0
	if s.cur != nil {
		n = s.cur.n + 1
	}
	f, err := os.OpenFile(s.path(n), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if err := fsyncPath(s.dir); err != nil {
		f.Close()
		return nil, err
	}
	sf := &segFile{n: n, f: f}
	s.files = append(s.files, sf)
	s.cur = sf
	return sf, nil
}

var errSegClosed = errors.New("asia localimg seg: store reset")

// put: 追記して fdatasync を待つ (group commit)。戻ったら応答してよい。
func (s *segStore) put(id, owner string, createdAt int64, data []byte) error {
	if len(id) > 255 || len(owner) > 65535 {
		return fmt.Errorf("asia localimg seg: id/owner too long")
	}
	rec := segEncode(segKindPut, id, owner, createdAt, data)
	s.mu.Lock()
	defer s.mu.Unlock()
	gen := s.gen
	sf, err := s.segmentLocked(int64(len(rec)))
	if err != nil {
		return err
	}
	off := sf.size
	if _, err := sf.f.WriteAt(rec, off); err != nil {
		return err
	}
	sf.size += int64(len(rec))
	end := sf.size
	for sf.durable < end {
		if gen != s.gen {
			return errSegClosed
		}
		if sf.syncing {
			s.cond.Wait()
			continue
		}
		sf.syncing = true
		target := sf.size
		s.mu.Unlock()
		err := fdatasyncFile(sf.f)
		s.mu.Lock()
		sf.syncing = false
		if err == nil && target > sf.durable {
			sf.durable = target
		}
		s.cond.Broadcast()
		if err != nil {
			return err
		}
	}
	if gen != s.gen {
		return errSegClosed
	}
	idLen, ownerLen := len(id), len(owner)
	s.index[id] = segLoc{seg: sf, off: off, dataOff: off + int64(segHdrLen+idLen+ownerLen), dataLen: len(data), owner: owner, createdAt: createdAt}
	return nil
}

// del: tombstone を追記して索引から消す (sync は待たない)。
func (s *segStore) del(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index == nil {
		return
	}
	if _, ok := s.index[id]; !ok {
		return
	}
	delete(s.index, id)
	rec := segEncode(segKindDel, id, "", 0, nil)
	sf, err := s.segmentLocked(int64(len(rec)))
	if err != nil {
		return
	}
	if _, err := sf.f.WriteAt(rec, sf.size); err == nil {
		sf.size += int64(len(rec))
	}
}

// get: 索引から読む。
func (s *segStore) get(id string) (*imageObject, time.Time) {
	s.mu.Lock()
	loc, ok := s.index[id]
	s.mu.Unlock()
	if !ok {
		return nil, time.Time{}
	}
	data := make([]byte, loc.dataLen)
	if _, err := loc.seg.f.ReadAt(data, loc.dataOff); err != nil && !(errors.Is(err, io.EOF) && loc.dataLen == 0) {
		return nil, time.Time{}
	}
	return &imageObject{Owner: loc.owner, MediaType: "image/jpeg", Data: data}, time.Unix(0, loc.createdAt)
}

// reset (initialize): ディレクトリは asiaStore.purge が消している。世代を進めて作り直す。
func (s *segStore) reset(dir string) {
	s.mu.Lock()
	s.gen++
	s.cond.Broadcast()
	s.mu.Unlock()
	if err := s.open(dir); err != nil {
		slog.Error("asia localimg seg: reopen after purge", "error", err)
	}
}
