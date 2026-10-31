package isureal

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_FWD_PRIORITY=1 のとき、eu/am から転送された書き込み (検証済みの
// X-Isureal-Fwd-Region 付き非 GET /api/*) は待たずに処理し、自地域の非 GET /api/*
// は同時実行数 ISUREAL_FWD_PRIORITY_LOCAL_SLOTS のセマフォを通す。
// 待ちは ISUREAL_FWD_PRIORITY_MAX_WAIT_MS で打ち切り、503 は返さずにそのまま処理する。
// 直近 ISUREAL_FWD_PRIORITY_IDLE_MS 以内に転送書き込みが無ければ、ローカルも待たせない。
var (
	fwdPriorityOn         = os.Getenv("ISUREAL_FWD_PRIORITY") == "1"
	fwdPriorityLocalSlots = envIntOrDefault("ISUREAL_FWD_PRIORITY_LOCAL_SLOTS", 32)
	fwdPriorityMaxWait    = time.Duration(envIntOrDefault("ISUREAL_FWD_PRIORITY_MAX_WAIT_MS", 2000)) * time.Millisecond
	fwdPriorityIdle       = time.Duration(envIntOrDefault("ISUREAL_FWD_PRIORITY_IDLE_MS", 2000)) * time.Millisecond
)

type fwdPriority struct {
	region   string
	slots    chan struct{}
	maxWait  time.Duration
	idle     time.Duration
	lastFwd  atomic.Int64 // 最後に転送書き込みを受けた時刻 (UnixNano)
	inflight atomic.Int64 // 処理中の転送書き込み

	mu       sync.Mutex
	fwdDur   []time.Duration // 転送書き込みの処理時間
	localWt  []time.Duration // ローカル書き込みのセマフォ待ち
	localDur []time.Duration // ローカル書き込みの処理時間 (待ちを含む)
	timeouts int64
	bypass   int64 // 転送が無いため待たずに通した件数
}

func newFwdPriority(region string, slots int, maxWait, idle time.Duration) *fwdPriority {
	return &fwdPriority{region: region, slots: make(chan struct{}, slots), maxWait: maxWait, idle: idle}
}

// FwdPriorityMiddleware は ISUREAL_FWD_PRIORITY が off なら何もしない。
func FwdPriorityMiddleware(h *HandlerContext) echo.MiddlewareFunc {
	if !fwdPriorityOn {
		return func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	}
	p := newFwdPriority(h.Config.Region, fwdPriorityLocalSlots, fwdPriorityMaxWait, fwdPriorityIdle)
	go p.statsLoop()
	slog.Info("fwd-priority: on", "local_slots", fwdPriorityLocalSlots, "max_wait", fwdPriorityMaxWait, "idle", fwdPriorityIdle)
	return p.middleware(func(c *echo.Context) bool { return requestRegion(h, c) != h.Config.Region })
}

// fwdPriorityLocalExempt: ローカル枠を取らずに通すローカル書き込み。
// asia ローカルの投稿は画像の INSERT を画像専用プール (h.Img) で待つので、枠を握ったままそこで並ぶと
// 枠 128 が投稿で埋まり、友達申請・login が枠待ち (最大 2 秒) になっていた。投稿の同時数は h.Img が抑える。
var fwdPriorityLocalExempt = func(*http.Request) bool { return false }

func fwdPriorityTarget(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	path := req.URL.Path
	return strings.HasPrefix(path, "/api/") && path != "/api/initialize" && !strings.HasPrefix(path, "/api/admin/")
}

func (p *fwdPriority) middleware(isForwarded func(*echo.Context) bool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if !fwdPriorityTarget(c.Request()) {
				return next(c)
			}
			start := time.Now()
			if isForwarded(c) {
				p.lastFwd.Store(start.UnixNano())
				p.inflight.Add(1)
				err := next(c)
				p.inflight.Add(-1)
				p.record(&p.fwdDur, time.Since(start))
				return err
			}
			if (p.inflight.Load() == 0 && start.UnixNano()-p.lastFwd.Load() > int64(p.idle)) || fwdPriorityLocalExempt(c.Request()) {
				p.mu.Lock()
				p.bypass++
				p.mu.Unlock()
				err := next(c)
				p.record(&p.localDur, time.Since(start))
				return err
			}
			acquired := false
			select {
			case p.slots <- struct{}{}:
				acquired = true
			default:
				t := time.NewTimer(p.maxWait)
				select {
				case p.slots <- struct{}{}:
					acquired = true
				case <-t.C:
				case <-c.Request().Context().Done():
				}
				t.Stop()
			}
			wait := time.Since(start)
			if !acquired {
				p.mu.Lock()
				p.timeouts++
				p.mu.Unlock()
			}
			p.record(&p.localWt, wait)
			err := next(c)
			if acquired {
				<-p.slots
			}
			p.record(&p.localDur, time.Since(start))
			return err
		}
	}
}

func (p *fwdPriority) record(s *[]time.Duration, d time.Duration) {
	p.mu.Lock()
	if len(*s) < 1<<16 {
		*s = append(*s, d)
	}
	p.mu.Unlock()
}

func (p *fwdPriority) statsLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		p.mu.Lock()
		fd, lw, ld, to, bp := p.fwdDur, p.localWt, p.localDur, p.timeouts, p.bypass
		p.fwdDur, p.localWt, p.localDur, p.timeouts, p.bypass = nil, nil, nil, 0, 0
		p.mu.Unlock()
		if len(fd) == 0 && len(ld) == 0 {
			continue
		}
		f50, f90, _ := memDurPercentiles(fd)
		w50, w90, wmax := memDurPercentiles(lw)
		l50, l90, _ := memDurPercentiles(ld)
		slog.Info("fwd-priority", "fwd_n", len(fd), "fwd_p50", f50, "fwd_p90", f90,
			"local_n", len(ld), "local_waited_n", len(lw), "wait_p50", w50, "wait_p90", w90, "wait_max", wmax,
			"local_p50", l50, "local_p90", l90, "wait_timeouts", to, "bypass", bp)
	}
}
