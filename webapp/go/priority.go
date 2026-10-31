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
	// ISUREAL_FWD_PRIORITY_POST_SLOTS=N (既定 0 = 共有): ローカルの POST /api/posts を、他のローカル書き込みとは別の N 枠に通す。
	// job23341 (STORE=1): d5/GP で投稿 1 本が 4〜10 秒かかり、共有の 128 枠を投稿が埋めた。login / friend-requests / accept は
	// 処理が 0.1〜0.5 秒なのに、全部が枠待ちの上限 (2 秒) で 2.0〜2.2 秒になった (nginx の request_time)。
	// 投稿どうしの待ちと上限は今までどおり。変わるのは「投稿の列の後ろに他の書き込みが並ばない」ことだけ。
	fwdPriorityPostSlots = envIntOrDefault("ISUREAL_FWD_PRIORITY_POST_SLOTS", 0)
)

type fwdPriority struct {
	region    string
	slots     chan struct{}
	postSlots chan struct{} // ISUREAL_FWD_PRIORITY_POST_SLOTS (nil = slots を共有)
	maxWait   time.Duration
	idle      time.Duration
	lastFwd   atomic.Int64 // 最後に転送書き込みを受けた時刻 (UnixNano)
	inflight  atomic.Int64 // 処理中の転送書き込み

	mu       sync.Mutex
	fwdDur   []time.Duration // 転送書き込みの処理時間
	localWt  []time.Duration // ローカル書き込みのセマフォ待ち
	localDur []time.Duration // ローカル書き込みの処理時間 (待ちを含む)
	timeouts int64
	bypass   int64 // 転送が無いため待たずに通した件数
	// timeoutBy: 枠待ちの上限で枠なしに通した数を「要求の地域/種類 (post|other)」ごとに (statsLoop が 10 秒ごとに出す)
	timeoutBy map[string]int64
	parked    atomic.Int64 // ISUREAL_FWD_PRIORITY_YIELD で枠を返して待ち中の要求
	regionOf  func(*echo.Context) string
	admit     *admitGate // ISUREAL_ADMIT_* (nil = off)
}

func (p *fwdPriority) timeoutKey(c *echo.Context) string {
	r := p.region
	if p.regionOf != nil {
		r = p.regionOf(c)
	}
	kind := "other"
	if c.Request().Method == http.MethodPost && c.Request().URL.Path == "/api/posts" {
		kind = "post"
	}
	return r + "/" + kind
}

func newFwdPriority(region string, slots int, maxWait, idle time.Duration) *fwdPriority {
	p := &fwdPriority{region: region, slots: make(chan struct{}, slots), maxWait: maxWait, idle: idle}
	if fwdPriorityPostSlots > 0 {
		p.postSlots = make(chan struct{}, fwdPriorityPostSlots)
	}
	p.admit = newAdmitGate(admitMaxReq, admitMaxBytes, admitPostMin, admitMaxWait)
	return p
}

// slotsFor: この要求が取る枠。
func (p *fwdPriority) slotsFor(req *http.Request) chan struct{} {
	if p.postSlots != nil && req.Method == http.MethodPost && req.URL.Path == "/api/posts" {
		return p.postSlots
	}
	return p.slots
}

// FwdPriorityMiddleware は ISUREAL_FWD_PRIORITY が off なら何もしない。
func FwdPriorityMiddleware(h *HandlerContext) echo.MiddlewareFunc {
	if !fwdPriorityOn {
		return func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	}
	p := newFwdPriority(h.Config.Region, fwdPriorityLocalSlots, fwdPriorityMaxWait, fwdPriorityIdle)
	p.regionOf = func(c *echo.Context) string { return requestRegion(h, c) }
	go p.statsLoop()
	slog.Info("fwd-priority: on", "local_slots", fwdPriorityLocalSlots, "max_wait", fwdPriorityMaxWait, "idle", fwdPriorityIdle)
	if gpFairOn && gpFairPostExempt {
		prev := fwdPriorityLocalExempt
		fwdPriorityLocalExempt = func(req *http.Request) bool { return prev(req) || gpFairPostExemptReq(h, req) }
	}
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
			if p.admit != nil {
				// ISUREAL_ADMIT_*: 入場の総量 (2 秒で枠なしに通すものも含む) を抑える
				leave := p.admit.enter(c.Request().Context(), isPostReq(c.Request()), c.Request().ContentLength, p.timeoutKey(c))
				defer leave()
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
			slots := p.slotsFor(c.Request())
			select {
			case slots <- struct{}{}:
				acquired = true
			default:
				t := time.NewTimer(p.maxWait)
				select {
				case slots <- struct{}{}:
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
				if p.timeoutBy == nil {
					p.timeoutBy = map[string]int64{}
				}
				p.timeoutBy[p.timeoutKey(c)]++
				p.mu.Unlock()
			}
			p.record(&p.localWt, wait)
			var hold *fwdSlotHold
			if acquired && fwdPriorityYieldMax > 0 {
				hold = &fwdSlotHold{slots: slots, parked: &p.parked, max: fwdPriorityYieldMax}
				c.SetRequest(c.Request().WithContext(withFwdSlotHold(c.Request().Context(), hold)))
			}
			err := next(c)
			if hold != nil {
				hold.finish()
			} else if acquired {
				<-slots
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
		fd, lw, ld, to, bp, tb := p.fwdDur, p.localWt, p.localDur, p.timeouts, p.bypass, p.timeoutBy
		p.fwdDur, p.localWt, p.localDur, p.timeouts, p.bypass, p.timeoutBy = nil, nil, nil, 0, 0, nil
		p.mu.Unlock()
		if p.admit != nil {
			p.admit.logStats()
		}
		if len(fd) == 0 && len(ld) == 0 {
			continue
		}
		f50, f90, _ := memDurPercentiles(fd)
		w50, w90, wmax := memDurPercentiles(lw)
		l50, l90, _ := memDurPercentiles(ld)
		slog.Info("fwd-priority", "fwd_n", len(fd), "fwd_p50", f50, "fwd_p90", f90,
			"local_n", len(ld), "local_waited_n", len(lw), "wait_p50", w50, "wait_p90", w90, "wait_max", wmax,
			"local_p50", l50, "local_p90", l90, "wait_timeouts", to, "bypass", bp,
			"timeouts_by", tb, "yielded", fwdYieldStats.yielded.Swap(0), "parked", p.parked.Load())
	}
}
