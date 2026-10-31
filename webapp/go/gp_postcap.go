package isureal

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_ASIA_POST_CAP=N (既定 0 = off、asia1 = pump だけ): GP の間、asia ローカルの POST /api/posts の
// 同時実行を N 本に絞る (転送された eu/am の投稿は絞らない)。
//
// 19508: GP の締め切りの後、asia1 が asia の投稿 1000 件超の列を流している間、pump → eu/am の push が
// 0.6〜1.5 秒遅れ (peer の "large push delay")、eu/am は wm の上限 300ms で DB 経路に落ち、lease も切れた (notOn)。
// asia1 は 2 CPU。Go には goroutine の優先度が無いので、push の送信・HTTP の writeLoop/readLoop・poll の応答の
// 各段が、実行可能な投稿の goroutine の列の後ろに並ぶ。枠で待つ投稿は park される (実行可能にならない) ので、
// 列が短くなり pump の各段の待ちが縮む。投稿の処理の順番が変わるだけで、正しさ (RYW・slack 0) は変わらない。
//
// ISUREAL_ASIA_POST_CAP_WAIT_MS (既定 3000): 枠をこれだけ待っても取れなければ枠無しで進む (断らない)。
// ISUREAL_ASIA_POST_CAP_LAG_MS (既定 0): >0 なら、直近の push の往復 (送信 → ack) がこれを超えている間だけ絞る。
// 枠は書き込みの barrier (memWriteBarrier) の内側で取る: barrier の待ち (サイクル完了) の間は枠を握らない。
var (
	asiaPostCapN    = envIntOrDefault("ISUREAL_ASIA_POST_CAP", 0)
	asiaPostCapWait = time.Duration(envIntOrDefault("ISUREAL_ASIA_POST_CAP_WAIT_MS", 3000)) * time.Millisecond
	asiaPostCapLag  = time.Duration(envIntOrDefault("ISUREAL_ASIA_POST_CAP_LAG_MS", 0)) * time.Millisecond

	memPushLastRTT atomic.Int64 // 直近に ack が返った push の送信 → ack (ns)。pump が更新
)

var asiaPostCapStats struct{ gated, waited, timeouts, bypass atomic.Int64 }

type postCap struct {
	slots   chan struct{}
	maxWait time.Duration
	lag     time.Duration
	// active: この要求を絞るか (GP 中の asia ローカルの投稿か)
	active func(c *echo.Context) bool
}

func newPostCap(n int, maxWait, lag time.Duration, active func(c *echo.Context) bool) *postCap {
	return &postCap{slots: make(chan struct{}, n), maxWait: maxWait, lag: lag, active: active}
}

// acquire: 枠を取る (取れなければ maxWait 後に枠無しで進む)。戻り値は解放関数。
func (p *postCap) acquire(ctx context.Context) func() {
	select {
	case p.slots <- struct{}{}:
		return func() { <-p.slots }
	default:
	}
	asiaPostCapStats.waited.Add(1)
	t := time.NewTimer(p.maxWait)
	defer t.Stop()
	select {
	case p.slots <- struct{}{}:
		return func() { <-p.slots }
	case <-t.C:
		asiaPostCapStats.timeouts.Add(1)
	case <-ctx.Done():
	}
	return func() {}
}

func (p *postCap) middleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		req := c.Request()
		if req.Method != http.MethodPost || req.URL.Path != "/api/posts" || !p.active(c) {
			return next(c)
		}
		if p.lag > 0 && time.Duration(memPushLastRTT.Load()) <= p.lag {
			asiaPostCapStats.bypass.Add(1)
			return next(c)
		}
		asiaPostCapStats.gated.Add(1)
		release := p.acquire(req.Context())
		defer release()
		return next(c)
	}
}

// asiaPostCapMiddleware: pump (asia1) で memWriteBarrier の後に e.Use する (barrier の内側)。
func asiaPostCapMiddleware(h *HandlerContext) echo.MiddlewareFunc {
	region := h.Config.Region
	p := newPostCap(asiaPostCapN, asiaPostCapWait, asiaPostCapLag, func(c *echo.Context) bool {
		if requestRegion(h, c) != region || h.Mem == nil {
			return false
		}
		_, gp := h.Mem.gpWindowDeadline(region, time.Now())
		return gp
	})
	slog.Info("asia-post-cap: on", "n", asiaPostCapN, "max_wait", asiaPostCapWait, "lag", asiaPostCapLag)
	go func() {
		for range time.Tick(10 * time.Second) {
			g := asiaPostCapStats.gated.Swap(0)
			b := asiaPostCapStats.bypass.Swap(0)
			if g == 0 && b == 0 {
				continue
			}
			slog.Info("asia-post-cap", "gated", g, "waited", asiaPostCapStats.waited.Swap(0),
				"timeouts", asiaPostCapStats.timeouts.Swap(0), "bypass", b, "push_rtt", time.Duration(memPushLastRTT.Load()))
		}
	}()
	return p.middleware
}
