package isureal

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// 入場の総量の上限 (fwd-priority のローカル書き込み全部: 枠待ち・枠あり・2 秒で枠なしに通したもの・FWD_YIELD で待ち中のもの)。
// ISUREAL_ADMIT_MAX_REQ=N (既定 0 = off): 同時に入場している要求の数の上限。
// ISUREAL_ADMIT_MAX_IMG_MB=M (既定 0 = off): 入場中の投稿の本文 (Content-Length) の合計の上限。
// ISUREAL_ADMIT_POST_MIN=K (既定 0): N のうち K は投稿だけが使える (投稿以外は N-K まで)。投稿の最低限の処理量の保証。
// ISUREAL_ADMIT_MAX_WAIT_MS (既定 3000): これを超えて待った要求は断らずに入れ、地域/種類ごとに数える (admit_over)。
// CPU の実行枠は fwd-priority の枠 (slots / postSlots)、I/O・ACK の待ちは FWD_YIELD の N で別に数える。この上限はその総量。
var (
	admitMaxReq   = int64(envIntOrDefault("ISUREAL_ADMIT_MAX_REQ", 0))
	admitMaxBytes = int64(envIntOrDefault("ISUREAL_ADMIT_MAX_IMG_MB", 0)) << 20
	admitPostMin  = int64(envIntOrDefault("ISUREAL_ADMIT_POST_MIN", 0))
	admitMaxWait  = time.Duration(envIntOrDefault("ISUREAL_ADMIT_MAX_WAIT_MS", 3000)) * time.Millisecond
)

type admitGate struct {
	maxReq, maxBytes, postMin int64
	maxWait                   time.Duration

	mu      sync.Mutex
	n       int64
	bytes   int64
	changed chan struct{} // 解放のたびに close して作り直す
	over    map[string]int64
	waited  int64
}

func newAdmitGate(maxReq, maxBytes, postMin int64, maxWait time.Duration) *admitGate {
	if maxReq <= 0 && maxBytes <= 0 {
		return nil
	}
	return &admitGate{maxReq: maxReq, maxBytes: maxBytes, postMin: postMin, maxWait: maxWait, changed: make(chan struct{})}
}

func (g *admitGate) fits(post bool, size int64) bool {
	if g.maxReq > 0 {
		limit := g.maxReq
		if !post {
			limit -= g.postMin
		}
		if g.n >= limit {
			return false
		}
	}
	// 1 本で上限を超える本文でも、空いていれば入れる (詰まらせない)
	if g.maxBytes > 0 && size > 0 && g.bytes > 0 && g.bytes+size > g.maxBytes {
		return false
	}
	return true
}

// enter: 入れるまで待つ (上限 maxWait、ctx が終われば戻る)。戻り値の関数で出る。断らない。
func (g *admitGate) enter(ctx context.Context, post bool, size int64, key string) func() {
	if !post {
		size = 0
	}
	var deadline <-chan time.Time
	for {
		g.mu.Lock()
		if g.fits(post, size) {
			break
		}
		ch := g.changed
		g.mu.Unlock()
		if deadline == nil {
			t := time.NewTimer(g.maxWait)
			defer t.Stop()
			deadline = t.C
			g.mu.Lock()
			g.waited++
			g.mu.Unlock()
		}
		select {
		case <-ch:
			continue
		case <-deadline:
		case <-ctx.Done():
		}
		g.mu.Lock()
		if g.over == nil {
			g.over = map[string]int64{}
		}
		g.over[key]++
		break
	}
	g.n++
	g.bytes += size
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		g.n--
		g.bytes -= size
		close(g.changed)
		g.changed = make(chan struct{})
		g.mu.Unlock()
	}
}

func (g *admitGate) logStats() {
	g.mu.Lock()
	over, waited, n, b := g.over, g.waited, g.n, g.bytes
	g.over, g.waited = nil, 0
	g.mu.Unlock()
	if waited > 0 || len(over) > 0 {
		slog.Info("admit", "waited", waited, "over_by", over, "inflight", n, "inflight_bytes", b)
	}
}

func isPostReq(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/api/posts"
}
