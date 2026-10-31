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
	memWMTick     = envDurationMS("ISUREAL_MEM_WM_TICK_MS", memWMTickDefault())
	memNow        = time.Now // テストで差し替える
	memWMStatsLog sync.Once
)

// ISUREAL_MEM_WM_HB=1 (既定 0): watermark の待ちを短くする (読み手は限定しない。すべての GET が従来どおり待つ)。
//   - poll (= wm の heartbeat) の周期を 20ms → 10ms。wm は poll のスナップショットに結び付くので、poll 無しの
//     時刻だけの heartbeat では進められない (送信時刻を wm にすると x〜送信の間のコミットを取りこぼす)。
//   - wm モードの peer へのデータ push の並列上限を 4 → 16、wm 通知の並列上限を 8 → 16。
//     wm モードの peer の ack は書き込みの完了条件ではない (waitAcked が飛ばす) ので、変わるのは届く速さだけ。
//     上限 4 だと RTT 100ms / 周期 20ms ≒ 5 本の push のうち 1 本以上が ack 待ちで 1 RTT 遅れ、その seq の wm も保留される。
//   - wm をデータ push に同梱する (publishWM)。
//
// 正しさ (変わらない): wm=x は「x の後に取ったスナップショットまでを seq S に publish した」後にだけ S に対して有効になり、
// peer は seq S まで適用してから wm を x に進める。読みは到着時刻 T_r に対し wm >= T_r + ε − slack を待つ。
// slack = 0 なら、2xx が T_r より前に返ったすべての書き込み (書いた人を問わない) が、読みの前に peer のメモリに入っている。
// slack > 0 (ISUREAL_MEM_WM_SLACK_MS) は T_r の直前 slack 以内に完了した書き込みを見落としうる (「見えるべき」違反の危険) ので既定 0。
var (
	memWMHB    = os.Getenv("ISUREAL_MEM_WM_HB") == "1"
	memWMSlack = envDurationMS("ISUREAL_MEM_WM_SLACK_MS", 0)
)

func memWMTickDefault() time.Duration {
	if os.Getenv("ISUREAL_MEM_WM_HB") == "1" {
		return 10 * time.Millisecond
	}
	return 20 * time.Millisecond
}

// ISUREAL_MEM_WM_EARLY=1 (既定 0): poll で取った変更を、asia1 のメモリへの反映 (applyBatch) より先に peer へ出す
// (publish + wm 通知)。wm=x の意味は「x より前に応答したコミットは seq S 以前のバッチに含まれる」で、
// asia1 自身の反映とは無関係 (peer は seq S を自分で適用してから wm を進める)。asia1 の書き込みの 2xx は
// 従来どおりサイクル完了 (= asia1 のメモリ反映後) まで待つので、asia1 の読みの RYW も変わらない。
// applyBatch が失敗したら、出してしまったバッチを再送しないよう p.snap を進めて invalidate (全ホスト作り直し)。
// wm はデータ push にも同梱する (publishWM)。
var memWMEarly = os.Getenv("ISUREAL_MEM_WM_EARLY") == "1"

// ISUREAL_MEM_PUSH_INFLIGHT / ISUREAL_MEM_WM_INFLIGHT (>0 で上書き): wm モードの peer へのデータ push と
// wm 通知の並列上限を、HB の他の変更 (周期・同梱) と切り離して試すためのノブ。
// 上限を上げるなら ISUREAL_MEM_IDLE_CONNS も (push + wm 通知 + heartbeat) 以上にすること (溢れると毎回 TCP を張り直す = +1 RTT)。
var (
	memPushInflightEnv = envInt("ISUREAL_MEM_PUSH_INFLIGHT", 0)
	memWMInflightEnv   = envInt("ISUREAL_MEM_WM_INFLIGHT", 0)
)

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

// memPushInflight: peer ごとのデータ push の並列上限。
func memPushInflight(peer *memPeer) int {
	if memWatermark && peer.wmMode {
		if memPushInflightEnv > 0 {
			return memPushInflightEnv
		}
		if memWMHB {
			return 16
		}
	}
	return memMaxInflight
}

func memWMInflightLimit() int {
	if memWMInflightEnv > 0 {
		return memWMInflightEnv
	}
	if memWMHB {
		return 16
	}
	return memWMInflight
}

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
	// pumpWM が false = 同期元の pump は ack を待っている (従来方式で正しい)。
	// 判定は読む時点で行う (入口で false でも、処理中に wm モードの dump へ入れ替わりうる)。
	return !ok || !m.w.pumpWM.Load() || m.w.wm.Load() >= t
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
			if !memWMWaitPath(req) {
				return next(c)
			}
			// 目標は常に付ける (ガードが読む時点の pumpWM で判断する)。待つのは pump が wm モードのときだけ。
			start := memNow()
			target := start.Add(memWMEps - memWMSlack).UnixNano()
			c.SetRequest(req.WithContext(context.WithValue(req.Context(), memWMTargetKey{}, target)))
			if m.w.pumpWM.Load() {
				memWMStats.record(m.waitWM(target, start))
			}
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

// memWMMsg: watermark だけの軽い通知 (毎サイクル)。データの push (逐次、1 RTT に 1 本) とは別に、並列に送る。
type memWMMsg struct {
	Epoch string `json:"e"`
	Seq   int64  `json:"q"`
	WM    int64  `json:"w"`
}

const memWMInflight = 8 // peer ごとの並列数 (RTT 100ms / 周期 20ms ≒ 5)

// setWM は cycleMu 保持中、poll の反映 (と publish) の後に呼ぶ。wm は現在の ps.seq に対して有効。
func (ps *memPeers) setWM(wm int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if wm > ps.wm {
		ps.wm = wm
	}
	msg := memWMMsg{Epoch: ps.epoch, Seq: ps.seq, WM: ps.wm}
	for _, peer := range ps.peers {
		if !peer.wmMode || peer.wmInflight >= memWMInflightLimit() {
			continue
		}
		peer.wmInflight++
		go func(peer *memPeer) {
			_, _ = memHTTPPost(peer.addr, "/internal/mem/wm", msg, time.Second)
			ps.mu.Lock()
			peer.wmInflight--
			ps.mu.Unlock()
		}(peer)
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
	if !matched {
		slog.Warn("mem: dump from unknown peer address", "remote", remoteAddr, "wm", wm)
		if !wm {
			for _, peer := range ps.peers {
				peer.wmMode = false
			}
		}
	}
}

// ---- peer side ----

// wmMsg: seq まで反映済みなら進める。未着なら保留し、push の反映後に適用する。
func (r *memReceiver) wmMsg(msg memWMMsg) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.m.ready.Load() || r.epoch == "" || msg.Epoch != r.epoch || msg.WM <= 0 {
		return
	}
	if r.seq >= msg.Seq {
		r.m.w.advance(msg.WM)
		return
	}
	r.wmPending = append(r.wmPending, msg)
	if len(r.wmPending) > 64 {
		r.wmPending = append([]memWMMsg(nil), r.wmPending[len(r.wmPending)-32:]...)
	}
}

// wmAfterPush: push が seq まで反映できていれば watermark を進める (保留分も)。
func (r *memReceiver) wmAfterPush(req memPushRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.m.ready.Load() || r.epoch == "" || req.Epoch != r.epoch {
		return
	}
	if req.WM > 0 && r.seq >= req.Seq {
		r.m.w.advance(req.WM)
	}
	rest := r.wmPending[:0]
	for _, msg := range r.wmPending {
		if msg.Epoch != r.epoch {
			continue
		}
		if r.seq >= msg.Seq {
			r.m.w.advance(msg.WM)
		} else {
			rest = append(rest, msg)
		}
	}
	r.wmPending = rest
}
