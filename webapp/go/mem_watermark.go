package isureal

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_MEM_WATERMARK=1: 書き込みの確定点を「peer の ack」から「peer の読み側の watermark 待ち」に移す。
//
// watermark W (unix ns) の意味: 「実時刻 W より前に COMMIT の応答が返ったトランザクションの変更は、
// 連番 seq(W) までのバッチにすべて含まれている」。
//
// 作り方 (pump, poll ごと): fetchChanges を送る直前に asia1 で x := time.Now() を取る。
//   - PG は CommitTransaction で ProcArrayEndTransaction (他から可視になる) を済ませてから COMMIT の応答を返す。
//   - fetchChanges は単一文の autocommit で、文のスナップショットは PG がクエリを受け取った後 (x より後) に取られる。
//   - よって応答が x より前に返ったコミット → スナップショット取得より前に可視 → このバッチ以前に含まれる。
//   - 同じ文の中の clock_timestamp() は「スナップショット取得の後」に評価されるので使えない (X が大きすぎる)。
//
// peer: GET を受けた時刻 T_r に対し W >= T_r + ε を確認してからメモリで答える (ε は時計ずれ + 余裕)。
// クライアントが書き込みの 2xx を受けてから GET を送るなら、commit の応答 < 2xx < T_r なので、
// W >= T_r + ε の時点でその書き込みは peer のメモリに反映済み。確認できなければ (上限 memWMMaxWait) DB 経路。
// 正しさはメモリ読みの入口のガード (onCtx / wmOK) が担保し、middleware の待ちは「ガードを通りやすくする」だけ。
//
// 混在構成でも安全: pump が ack を省くのは dump 取得時に wm=1 を申告した peer だけ。
// peer が読みを止めて待つのは、dump が「pump は wm モード」と言ったときだけ。

var (
	memWatermark  = os.Getenv("ISUREAL_MEM_WATERMARK") == "1"
	memWMEps      = envDurationMS("ISUREAL_MEM_WM_EPS_MS", 3*time.Millisecond)
	memWMMaxWait  = envDurationMS("ISUREAL_MEM_WM_MAXWAIT_MS", 300*time.Millisecond)
	memWMTick     = envDurationMS("ISUREAL_MEM_WM_TICK_MS", 20*time.Millisecond)
	memNow        = time.Now // テストで差し替える
	memWMStatsLog sync.Once
)

func envDurationMS(key string, def time.Duration) time.Duration {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return time.Duration(v) * time.Millisecond
	}
	return def
}

// memWM は peer 側の適用済み watermark。
type memWM struct {
	wm     atomic.Int64 // 適用済み watermark (unix ns)。0 = 未知
	pumpWM atomic.Bool  // 同期元の pump が wm モード (= ack を待たずに返している) か
	mu     sync.Mutex
	ch     chan struct{} // wm が進むたびに close して作り直す (Broadcast)
}

func (w *memWM) notify() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ch == nil {
		w.ch = make(chan struct{})
	}
	return w.ch
}

func (w *memWM) broadcast() {
	w.mu.Lock()
	if w.ch != nil {
		close(w.ch)
		w.ch = nil
	}
	w.mu.Unlock()
}

// advance: 単調増加 (max)。呼び出し側は seq(v) までのバッチを適用済みであること。
func (w *memWM) advance(v int64) {
	for {
		cur := w.wm.Load()
		if v <= cur {
			return
		}
		if w.wm.CompareAndSwap(cur, v) {
			w.broadcast()
			return
		}
	}
}

// set: メモリの中身を入れ替えたとき (resync / invalidate) に置き換える。
func (w *memWM) set(v int64) {
	w.wm.Store(v)
	w.broadcast()
}

type memWMTargetKey struct{}

// wmOK: このリクエストがメモリを読んでよいか (watermark の観点)。目標の無いリクエスト (pump、フラグ off) は常に true。
func (m *memStore) wmOK(ctx context.Context) bool {
	if m == nil {
		return false
	}
	t, ok := ctx.Value(memWMTargetKey{}).(int64)
	return !ok || m.w.wm.Load() >= t
}

func (m *memStore) onCtx(ctx context.Context) bool { return m.on() && m.wmOK(ctx) }

func memWMWaitPath(req *http.Request) bool {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return false
	}
	p := req.URL.Path
	return strings.HasPrefix(p, "/api/") && !strings.HasPrefix(p, "/api/images/")
}

// memReadWait (peer, フラグ on): GET /api/* に目標 T_r + ε を付け、watermark が届くまで待つ (上限 memWMMaxWait)。
func memReadWait(m *memStore) echo.MiddlewareFunc {
	memWMStatsLog.Do(func() { go memWMStats.loop() })
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			if !memWMWaitPath(req) || !m.w.pumpWM.Load() {
				return next(c)
			}
			start := memNow()
			target := start.Add(memWMEps).UnixNano()
			c.SetRequest(req.WithContext(context.WithValue(req.Context(), memWMTargetKey{}, target)))
			memWMStats.record(m.waitWM(target, start))
			return next(c)
		}
	}
}

type memWMOutcome struct {
	wait  time.Duration
	ok    bool // watermark が目標に届いた (メモリで答える)
	notOn bool // メモリが使えない状態 (DB 経路、待たない)
}

// waitWM: wm >= target まで待つ。メモリが not-on なら待たない。上限で打ち切る (ガードにより DB 経路)。
func (m *memStore) waitWM(target int64, start time.Time) memWMOutcome {
	for {
		if m.w.wm.Load() >= target {
			return memWMOutcome{wait: memNow().Sub(start), ok: true}
		}
		if !m.on() {
			return memWMOutcome{wait: memNow().Sub(start), notOn: true}
		}
		left := memWMMaxWait - memNow().Sub(start)
		if left <= 0 {
			return memWMOutcome{wait: memNow().Sub(start)}
		}
		ch := m.w.notify()
		if m.w.wm.Load() >= target {
			continue
		}
		timer := time.NewTimer(left)
		select {
		case <-ch:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// ---- stats (10 秒ごとに slog) ----

type memWMStatsT struct {
	mu       sync.Mutex
	waits    []time.Duration
	fallback int
	notOn    int
}

var memWMStats memWMStatsT

func (s *memWMStatsT) record(o memWMOutcome) {
	s.mu.Lock()
	switch {
	case o.notOn:
		s.notOn++
	case !o.ok:
		s.fallback++
		s.waits = append(s.waits, o.wait)
	default:
		if len(s.waits) < 1<<16 {
			s.waits = append(s.waits, o.wait)
		}
	}
	s.mu.Unlock()
}

func (s *memWMStatsT) loop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		waits, fb, no := s.waits, s.fallback, s.notOn
		s.waits, s.fallback, s.notOn = nil, 0, 0
		s.mu.Unlock()
		if len(waits) == 0 && fb == 0 && no == 0 {
			continue
		}
		p50, p90, p99 := memPercentiles(waits)
		slog.Info("mem: wm wait", "n", len(waits), "p50", p50, "p90", p90, "p99", p99, "fallback", fb, "notOn", no)
	}
}

func memPercentiles(d []time.Duration) (p50, p90, p99 time.Duration) {
	if len(d) == 0 {
		return
	}
	s := append([]time.Duration(nil), d...)
	slices.Sort(s)
	at := func(q float64) time.Duration { return s[min(len(s)-1, int(q*float64(len(s))))] }
	return at(0.5), at(0.9), at(0.99)
}

// ---- pump side ----

// setWM は cycleMu 保持中、poll の反映 (と publish) の後に呼ぶ。wm は現在の ps.seq に対して有効。
// wm モードの peer へ heartbeat を送り (並列数の上限内)、watermark を早く届ける。
func (ps *memPeers) setWM(wm int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if wm > ps.wm {
		ps.wm = wm
	}
	for _, peer := range ps.peers {
		if peer.wmMode {
			ps.kick(peer, true)
		}
	}
}

// markWMMode: dump を取りに来た peer (RemoteAddr) の申告を記録する (cycleMu 保持中に呼ぶ)。
// 一致する peer が無ければ、申告が wm=0 のときだけ全 peer を ack 待ちに戻す (安全側)。
func (ps *memPeers) markWMMode(remoteAddr string, wm bool) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	matched := false
	for _, peer := range ps.peers {
		if h, _, err := net.SplitHostPort(peer.addr); err == nil && h == host {
			peer.wmMode = wm
			matched = true
		}
	}
	if !matched && !wm {
		for _, peer := range ps.peers {
			peer.wmMode = false
		}
	}
}

// ---- peer side ----

// wmAfterPush: push が seq まで反映できていれば watermark を進める。
func (r *memReceiver) wmAfterPush(req memPushRequest) {
	if req.WM <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m.ready.Load() && req.Epoch == r.epoch && r.epoch != "" && r.seq >= req.Seq {
		r.m.w.advance(req.WM)
	}
}
