package isureal

import (
	"container/list"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_IMG_DEDUP=1: 回線 (asia→eu/am) を渡る画像バイトの重複を減らす。
//   - push 前に受信側へ「もう持っているか」を聞き (GET /internal/images/:id/have)、
//     持っていれば本文を送らない (pull 済み・受信中の画像の二重輸送を防ぐ)。
//   - 受信中 (push の本文が流れている最中) の画像を閲覧が取りに来たら、DB/origin へ
//     取りに行かず受信完了を待つ (push と pull の二重輸送を防ぐ)。
//   - initialize (asia1) で eu/am の画像キャッシュ (メモリ LRU + ディスク) を消す。
//   - 10 秒ごとに hit/miss/bytes を slog に出す。
// 画像の中身は ID ごとに不変 (image_objects に UPDATE は無い。削除・置換は有効性の確認側)。

var imgDedupOn = os.Getenv("ISUREAL_IMG_DEDUP") == "1"

const imgDedupRecvWait = 3 * time.Second

var imgStats struct {
	memHit, diskHit, own, flightJoin   atomic.Int64
	dbMiss, dbBytes, pullMiss, pullB   atomic.Int64
	recvWait, recvWaitHit              atomic.Int64
	recvN, recvBytes, recvDup          atomic.Int64
	pushSent, pushBytes, pushSkip      atomic.Int64
	pushSkipBytes, haveErr, haveServed atomic.Int64
}

func init() {
	if !imgDedupOn {
		return
	}
	go func() {
		for range time.Tick(10 * time.Second) {
			s := &imgStats
			vals := []any{
				"mem_hit", s.memHit.Swap(0), "disk_hit", s.diskHit.Swap(0), "own", s.own.Swap(0), "flight_join", s.flightJoin.Swap(0),
				"db_miss", s.dbMiss.Swap(0), "db_bytes", s.dbBytes.Swap(0), "pull_miss", s.pullMiss.Swap(0), "pull_bytes", s.pullB.Swap(0),
				"recv_wait", s.recvWait.Swap(0), "recv_wait_hit", s.recvWaitHit.Swap(0),
				"recv", s.recvN.Swap(0), "recv_bytes", s.recvBytes.Swap(0), "recv_dup", s.recvDup.Swap(0),
				"push_sent", s.pushSent.Swap(0), "push_bytes", s.pushBytes.Swap(0), "push_skip", s.pushSkip.Swap(0), "push_skip_bytes", s.pushSkipBytes.Swap(0),
				"have_err", s.haveErr.Swap(0), "have_served", s.haveServed.Swap(0),
			}
			nonzero := false
			for i := 1; i < len(vals); i += 2 {
				if vals[i].(int64) != 0 {
					nonzero = true
					break
				}
			}
			if nonzero {
				slog.Info("imgcache", vals...)
			}
		}
	}()
}

// beginRecv は push 受信の開始を登録する。返り値の done は必ず呼ぶ。
func (s *imageStore) beginRecv(id string) (done func()) {
	s.flightMu.Lock()
	if _, ok := s.recv[id]; ok {
		s.flightMu.Unlock()
		return func() {}
	}
	ch := make(chan struct{})
	s.recv[id] = ch
	s.flightMu.Unlock()
	return func() {
		s.flightMu.Lock()
		if s.recv[id] == ch {
			delete(s.recv, id)
		}
		s.flightMu.Unlock()
		close(ch)
	}
}

// waitRecv: 受信中の push があれば完了を待ち、メモリにあれば返す。
func (s *imageStore) waitRecv(ctx context.Context, id string) *imageObject {
	s.flightMu.Lock()
	ch, ok := s.recv[id]
	s.flightMu.Unlock()
	if !ok {
		return nil
	}
	imgStats.recvWait.Add(1)
	t := time.NewTimer(imgDedupRecvWait)
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
		return nil
	case <-ctx.Done():
		return nil
	}
	if obj, ok := s.mem.get(id); ok {
		imgStats.recvWaitHit.Add(1)
		return obj
	}
	return nil
}

// has: この画像のバイト列を持っている (または取得中・受信中) か。
func (s *imageStore) has(id string) bool {
	if _, ok := s.mem.get(id); ok {
		return true
	}
	s.flightMu.Lock()
	_, f := s.flight[id]
	_, r := s.recv[id]
	s.flightMu.Unlock()
	if f || r {
		return true
	}
	if s.dir != "" && safeImageID(id) {
		if _, err := os.Stat(s.path(id)); err == nil {
			return true
		}
	}
	return false
}

// purge は、メモリ LRU とディスクキャッシュを空にする (initialize)。
func (s *imageStore) purge() {
	s.mem.reset()
	if s.dir == "" {
		return
	}
	trash := fmt.Sprintf("%s.trash-%d", s.dir, time.Now().UnixNano())
	if err := os.Rename(s.dir, trash); err == nil {
		go os.RemoveAll(trash)
	} else {
		os.RemoveAll(s.dir)
	}
	os.MkdirAll(s.dir, 0o755)
}

func (l *imageLRU) reset() {
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		sh.ll.Init()
		sh.items = make(map[string]*list.Element)
		sh.bytes = 0
		sh.mu.Unlock()
	}
}

// imgHave は push 送信前の確認。true = 受信側が既に持っている。
func (r *imgReplicator) imgHave(ctx context.Context, peer, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+peer+"/internal/images/"+id+"/have", nil)
	if err != nil {
		return false, err
	}
	req.Header.Set(forwardedSecretHeader, r.secret)
	resp, err := r.client.Do(req)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}
	return false, fmt.Errorf("imghave %s: %d", peer, resp.StatusCode)
}

// imgCachePurgePeers: initialize (asia1) から、push 先 (eu/am) の画像キャッシュを消す。
func imgCachePurgePeers(ctx context.Context) {
	if !imgDedupOn {
		return
	}
	r := imageReplicator
	if len(r.peers) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, peer := range r.peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 2; attempt++ {
				cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
				req, _ := http.NewRequestWithContext(cctx, http.MethodPost, "http://"+peer+"/internal/imgcache-purge", nil)
				req.Header.Set(forwardedSecretHeader, r.secret)
				res, err := r.client.Do(req)
				cancel()
				if err == nil {
					res.Body.Close()
					if res.StatusCode == http.StatusNoContent {
						return
					}
				}
				slog.Warn("imgcache purge", "peer", peer, "error", err)
			}
		}()
	}
	wg.Wait()
}

func imgReplAuthOK(c *echo.Context, secret string) bool {
	req := c.Request()
	return secret != "" && imgReplPeerSource(req.RemoteAddr) &&
		subtleEq(req.Header.Get(forwardedSecretHeader), secret)
}

func registerImgDedupRoutes(e *echo.Echo, store *imageStore, secret string) {
	e.GET("/internal/images/:imageId/have", func(c *echo.Context) error {
		if !imgReplAuthOK(c, secret) {
			return c.NoContent(http.StatusForbidden)
		}
		id := c.Param("imageId")
		if !safeImageID(id) {
			return c.NoContent(http.StatusBadRequest)
		}
		imgStats.haveServed.Add(1)
		if store.has(id) {
			return c.NoContent(http.StatusNoContent)
		}
		return c.NoContent(http.StatusNotFound)
	})
	e.POST("/internal/imgcache-purge", func(c *echo.Context) error {
		if !imgReplAuthOK(c, secret) {
			return c.NoContent(http.StatusForbidden)
		}
		store.purge()
		return c.NoContent(http.StatusNoContent)
	})
}
