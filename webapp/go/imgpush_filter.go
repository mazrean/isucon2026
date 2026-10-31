package isureal

import (
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// ISUREAL_IMG_PUSH_FILTER=1: asia1 の画像 push を、相手の地域の利用者が見る見込みが
// あるときだけに絞る。push しない画像は、相手側の imageStore.load が DB (replica →
// primary) から取りに来る。判定できないときは push する。
var imgPushFilterOn = os.Getenv("ISUREAL_IMG_PUSH_FILTER") == "1"

var imgPushStats struct{ pushed, pushedBytes, skipped, skippedBytes atomic.Int64 }

// imgPushFoFLimit: friends_of_friends の 2 hop 走査の上限。超えたら push する。
const imgPushFoFLimit = 20000

// imgPeerRegion は、ピアのアドレス (host:port) の地域を返す。不明なら ""。
func imgPeerRegion(peer string) string {
	host, _, err := net.SplitHostPort(peer)
	if err != nil {
		host = peer
	}
	for r, addr := range localImgOrigins {
		h, _, err := net.SplitHostPort(addr)
		if err != nil {
			h = addr
		}
		if h == host {
			return r
		}
	}
	return ""
}

// pushRegionsForPost は、作者 authorID・公開範囲 visibility の投稿 (とその RealMoji) を
// 見うる利用者のいる地域の集合を返す。extra は追加で必ず含める利用者 (RealMoji の投稿者)。
// nil は「判定できない (全ピアへ push)」。public は全体フィードに載るので常に nil。
func (d *memData) pushRegionsForPost(authorID, visibility, extra string) map[string]bool {
	if !imgPushFilterOn || d == nil {
		return nil
	}
	a := d.users[authorID]
	if a == nil {
		return nil
	}
	regions := map[string]bool{a.Region: true}
	if extra != "" && extra != authorID {
		x := d.users[extra]
		if x == nil {
			return nil
		}
		regions[x.Region] = true
	}
	switch visibility {
	case "friends":
		for id := range a.friends {
			f := d.users[id]
			if f == nil {
				return nil
			}
			regions[f.Region] = true
		}
	case "friends_of_friends":
		n := 0
		for id := range a.friends {
			f := d.users[id]
			if f == nil {
				return nil
			}
			regions[f.Region] = true
			for id2 := range f.friends {
				if n++; n > imgPushFoFLimit {
					return nil
				}
				g := d.users[id2]
				if g == nil {
					return nil
				}
				regions[g.Region] = true
			}
		}
	default: // public (全体フィード) や未知の値
		return nil
	}
	return regions
}

// memPushRegionsForPost は DB 経路のハンドラ用 (m.mu を持たない状態で呼ぶこと)。
func memPushRegionsForPost(m *memStore, authorID, visibility, extra string) map[string]bool {
	if !imgPushFilterOn || m == nil || !m.ready.Load() {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.d.pushRegionsForPost(authorID, visibility, extra)
}

func imgPushStatsLogger() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	var last [4]int64
	for range t.C {
		cur := [4]int64{imgPushStats.pushed.Load(), imgPushStats.pushedBytes.Load(), imgPushStats.skipped.Load(), imgPushStats.skippedBytes.Load()}
		if cur == last {
			continue
		}
		slog.Info("imgpush", "filter", imgPushFilterOn,
			"pushed", cur[0]-last[0], "pushed_bytes", cur[1]-last[1],
			"skipped", cur[2]-last[2], "skipped_bytes", cur[3]-last[3],
			"pushed_total", cur[0], "skipped_total", cur[2])
		last = cur
	}
}
