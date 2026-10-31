package isureal

import (
	"bytes"
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
)

// 画像 ID ごとの中身 (owner, media_type, data) は不変なので、無効化せずに
// メモリ LRU とローカルディスクにキャッシュする。有効性 (削除・アカウント状態・
// RealMoji 置換) は image_api.go 側で毎回 (短い TTL で) DB に確認する。

type imageObject struct {
	Owner     string
	MediaType string
	Data      []byte
}

func (o *imageObject) size() int64 {
	return int64(len(o.Data)+len(o.Owner)+len(o.MediaType)) + 128
}

const (
	imageMemCacheBudgetDefMB = 300
	imageMemCacheShards      = 32
)

type imageLRUShard struct {
	mu    sync.Mutex
	ll    *list.List
	items map[string]*list.Element
	bytes int64
	limit int64
}

type imageLRUEntry struct {
	id  string
	obj *imageObject
}

type imageLRU struct {
	seed   maphash.Seed
	shards [imageMemCacheShards]imageLRUShard
}

func newImageLRU(budget int64) *imageLRU {
	l := &imageLRU{seed: maphash.MakeSeed()}
	for i := range l.shards {
		l.shards[i].ll = list.New()
		l.shards[i].items = make(map[string]*list.Element)
		l.shards[i].limit = budget / imageMemCacheShards
	}
	return l
}

func (l *imageLRU) shard(id string) *imageLRUShard {
	return &l.shards[maphash.String(l.seed, id)%imageMemCacheShards]
}

func (l *imageLRU) get(id string) (*imageObject, bool) {
	s := l.shard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[id]; ok {
		s.ll.MoveToFront(e)
		return e.Value.(*imageLRUEntry).obj, true
	}
	return nil, false
}

func (l *imageLRU) add(id string, obj *imageObject) {
	s := l.shard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; ok {
		return
	}
	s.items[id] = s.ll.PushFront(&imageLRUEntry{id: id, obj: obj})
	s.bytes += obj.size()
	for s.bytes > s.limit && s.ll.Len() > 1 {
		back := s.ll.Back()
		entry := back.Value.(*imageLRUEntry)
		s.ll.Remove(back)
		delete(s.items, entry.id)
		s.bytes -= entry.obj.size()
	}
}

type imageCall struct {
	wg  sync.WaitGroup
	obj *imageObject
	err error
}

type imageStore struct {
	mem      *imageLRU
	dir      string
	writes   chan imageDiskWrite
	flightMu sync.Mutex
	flight   map[string]*imageCall
	recv     map[string]chan struct{} // ISUREAL_IMG_DEDUP: push 受信中
}

type imageDiskWrite struct {
	id  string
	obj *imageObject
}

var images = newImageStore(envOrDefault("ISUREAL_IMAGE_CACHE_DIR", "/home/isucon/imgcache"))

func newImageStore(dir string) *imageStore {
	s := &imageStore{
		mem:    newImageLRU(int64(envIntOrDefault("ISUREAL_IMAGE_MEM_MB", imageMemCacheBudgetDefMB)) << 20),
		flight: make(map[string]*imageCall),
		recv:   make(map[string]chan struct{}),
	}
	if dir != "" && dir != "-" && os.MkdirAll(dir, 0o755) == nil {
		s.dir = dir
		s.writes = make(chan imageDiskWrite, 4096)
		for range 2 {
			go s.diskWriter()
		}
	}
	return s
}

// safeImageID は、ディスクのファイル名に使ってよい ID かどうかを返す。
func safeImageID(id string) bool {
	if len(id) < 3 || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func (s *imageStore) path(id string) string {
	return filepath.Join(s.dir, id[:2], id)
}

func (s *imageStore) readDisk(id string) *imageObject {
	if s.dir == "" || !safeImageID(id) {
		return nil
	}
	raw, err := os.ReadFile(s.path(id))
	if err != nil {
		return nil
	}
	i := bytes.IndexByte(raw, '\n')
	if i < 0 {
		return nil
	}
	j := bytes.IndexByte(raw[i+1:], '\n')
	if j < 0 {
		return nil
	}
	return &imageObject{
		Owner:     string(raw[:i]),
		MediaType: string(raw[i+1 : i+1+j]),
		Data:      raw[i+1+j+1:],
	}
}

func (s *imageStore) diskWriter() {
	for w := range s.writes {
		p := s.path(w.id)
		if _, err := os.Stat(p); err == nil {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			continue
		}
		f, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
		if err != nil {
			continue
		}
		buf := make([]byte, 0, len(w.obj.Owner)+len(w.obj.MediaType)+2+len(w.obj.Data))
		buf = append(buf, w.obj.Owner...)
		buf = append(buf, '\n')
		buf = append(buf, w.obj.MediaType...)
		buf = append(buf, '\n')
		buf = append(buf, w.obj.Data...)
		_, werr := f.Write(buf)
		cerr := f.Close()
		if werr != nil || cerr != nil {
			os.Remove(f.Name())
			continue
		}
		if err := os.Rename(f.Name(), p); err != nil {
			os.Remove(f.Name())
			continue
		}
		// the disk cache only grows (new image IDs every run): once it passes the
		// budget, drop it wholesale. A miss just reloads from memory/DB.
		if imageDiskWritten.Add(int64(len(buf))) > imageDiskBudget {
			imageDiskWritten.Store(0)
			s.dropDisk()
		}
	}
}

var (
	imageDiskBudget  = int64(envIntOrDefault("ISUREAL_IMAGE_DISK_MAX_MB", 6144)) << 20
	imageDiskWritten atomic.Int64
)

// dropDisk empties the disk cache (rename + background delete) without touching the memory LRU.
func (s *imageStore) dropDisk() {
	trash := fmt.Sprintf("%s.trash-%d", s.dir, time.Now().UnixNano())
	if err := os.Rename(s.dir, trash); err == nil {
		go os.RemoveAll(trash)
	}
	os.MkdirAll(s.dir, 0o755)
	slog.Warn("imgcache: disk budget reached, dropped disk cache", "budget_mb", imageDiskBudget>>20)
}

// get は、画像 ID の中身を返す。存在しなければ (nil, nil)。
// 順に メモリ LRU → ディスク → DB (同時取得は 1 本にまとめる)。
func (s *imageStore) get(ctx context.Context, db, primary *sqlx.DB, id string) (*imageObject, error) {
	if obj, ok := s.mem.get(id); ok {
		imgStats.memHit.Add(1)
		return obj, nil
	}
	s.flightMu.Lock()
	if call, ok := s.flight[id]; ok {
		s.flightMu.Unlock()
		imgStats.flightJoin.Add(1)
		call.wg.Wait()
		return call.obj, call.err
	}
	call := &imageCall{}
	call.wg.Add(1)
	s.flight[id] = call
	s.flightMu.Unlock()

	call.obj, call.err = s.load(ctx, db, primary, id)
	call.wg.Done()

	s.flightMu.Lock()
	delete(s.flight, id)
	s.flightMu.Unlock()
	return call.obj, call.err
}

func (s *imageStore) load(ctx context.Context, db, primary *sqlx.DB, id string) (*imageObject, error) {
	if obj := s.readDisk(id); obj != nil {
		imgStats.diskHit.Add(1)
		s.mem.add(id, obj)
		return obj, nil
	}
	if obj := localImgOwn(id); obj != nil {
		imgStats.own.Add(1)
		s.mem.add(id, obj)
		return obj, nil
	}
	if imgDedupOn {
		// push の本文が流れている最中なら、同じバイト列をもう一度回線で取らない。
		if obj := s.waitRecv(ctx, id); obj != nil {
			return obj, nil
		}
	}
	// singleflight の待機者がいるので、呼び出し元のキャンセルに巻き込まれないようにする。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var row struct {
		Owner     string `db:"owner_user_id"`
		MediaType string `db:"media_type"`
		Data      []byte `db:"data"`
	}
	q := imageSelectAll
	var err error
	if imgHotID(id) {
		// one partition; a miss (rare) retries the full query
		err = db.GetContext(ctx, &row, imageSelectHot, id)
		if errors.Is(err, sql.ErrNoRows) {
			err = db.GetContext(ctx, &row, q, id)
		}
	} else {
		err = db.GetContext(ctx, &row, q, id)
	}
	// replica が遅れている可能性に備え、見つからなければ primary で確かめる。
	if errors.Is(err, sql.ErrNoRows) && primary != nil && primary != db {
		err = primary.GetContext(ctx, &row, q, id)
	}
	if errors.Is(err, sql.ErrNoRows) {
		// ISUREAL_LOCALIMG: the bytes may still be only at the origin region.
		obj, perr := localImgPull(ctx, id)
		if obj != nil {
			imgStats.pullMiss.Add(1)
			imgStats.pullB.Add(int64(len(obj.Data)))
			s.put(id, obj)
			return obj, nil
		}
		return nil, perr
	}
	if err != nil {
		return nil, err
	}
	obj := &imageObject{Owner: row.Owner, MediaType: row.MediaType, Data: row.Data}
	imgStats.dbMiss.Add(1)
	imgStats.dbBytes.Add(int64(len(row.Data)))
	s.mem.add(id, obj)
	if s.writes != nil && safeImageID(id) {
		select {
		case s.writes <- imageDiskWrite{id: id, obj: obj}:
		default:
		}
	}
	return obj, nil
}

// put は、INSERT 直後や複製受信時に中身をメモリとディスクへ格納する。
func (s *imageStore) put(id string, obj *imageObject) {
	s.mem.add(id, obj)
	if s.writes != nil && safeImageID(id) {
		select {
		case s.writes <- imageDiskWrite{id: id, obj: obj}:
		default:
		}
	}
}
