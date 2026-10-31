package isureal

import (
	"container/heap"
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_GP_FAIR=1: GP の間だけ、asia1 で投稿の DB 区間 (createPostBatch / 投稿の tx。posted_at はこの中で
// 決まる) に入る順番を、締め切りの早い順 (EDF) にする。
// 鍵 = 投稿者の地域の GP の締め切り − (転送された投稿なら ISUREAL_GP_FAIR_FWD_LEAD_MS)。
// GP は 3 地域が同時に開くので、実質「eu/am から転送された投稿を先に、同じ鍵なら到着順」になる。
// 18156: 転送の書き込み p90 が GP で 1.5s (DB のプール 60 本を asia ローカルの約 1000 件と取り合う)。
//
// GP の判定: 3 地域の開いている窓の starts_at の幅が ISUREAL_GP_FAIR_SYNC_MS 以内 (= 同時に開いた)。
// 通常の日は 3 秒おきに地域を回して開くので当たらない (nginx のログ: GP は 3 本が同じ秒、他は 3s 間隔)。
// on_time の秒数では区別できない (最初の版はそれで判定して、18975 で一度も効かなかった)。
// GP は starts_at から ISUREAL_GP_FAIR_ACTIVE_MS の間。締め切りは min(on_time_deadline, starts_at + ACTIVE)。
//
// 枠 (ISUREAL_GP_FAIR_SLOTS) は DB のプール (60) 近くにする: 絞るのではなく、プールの待ち行列の順番を変えるだけ。
// 順序を変えるだけで、断らない: 枠を ISUREAL_GP_FAIR_MAX_WAIT_MS 待っても取れなければ枠無しで進む。
// クライアントが切れた場合も進む (結果は使われないが、201 の前に永続化する既存の流れを変えない)。
var (
	gpFairOn         = os.Getenv("ISUREAL_GP_FAIR") == "1"
	gpFairSlots      = envIntOrDefault("ISUREAL_GP_FAIR_SLOTS", 52)
	gpFairFwdLead    = time.Duration(envIntOrDefault("ISUREAL_GP_FAIR_FWD_LEAD_MS", 1000)) * time.Millisecond
	gpFairMaxWait    = time.Duration(envIntOrDefault("ISUREAL_GP_FAIR_MAX_WAIT_MS", 3000)) * time.Millisecond
	gpFairSync       = time.Duration(envIntOrDefault("ISUREAL_GP_FAIR_SYNC_MS", 1500)) * time.Millisecond
	gpFairActive     = time.Duration(envIntOrDefault("ISUREAL_GP_FAIR_ACTIVE_MS", 15000)) * time.Millisecond
	gpFairPostExempt = os.Getenv("ISUREAL_GP_FAIR_POST_EXEMPT") == "1"
	gpFairGateG      = newPrioGate(gpFairSlots)
	// ISUREAL_GP_JPEG_REMOTES_MAX=N (既定 0 = 制限なし、GP_FAIR とは独立): 自地域が GP の間、JPEG 検査の委譲先を
	// ISUREAL_JPEG_REMOTES の先頭 N 個 (asia2) に限る。eu/am への委譲は 360KB を asia1 から地域外へ送り
	// (18156 の GP で約 530 件/10s ≈ 150Mbps)、asia1 の送信帯域 (bw_out_allowance_exceeded) を mem の push と取り合う。
	// asia1 の CPU は増えるので、bw_out が原因と確かめたときだけ使う (asia2 の cap も上げる)。
	gpJPEGRemotesMax = envIntOrDefault("ISUREAL_GP_JPEG_REMOTES_MAX", 0)
	gpMem            atomic.Pointer[memStore] // pump (asia1) のメモリ。setupMem が入れる
)

var gpRegions = [3]string{"asia", "europe", "america"}

func gpRegionIdx(region string) int {
	for i, r := range gpRegions {
		if r == region {
			return i
		}
	}
	return -1
}

// gpSnap: 3 地域の GP の締め切り (GP でなければ 0)。50ms ごとに作り直す。
type gpSnap struct {
	at       int64
	deadline [3]int64
	until    [3]int64
}

// gpLocalActive: region が GP 中か。
func gpLocalActive(region string) bool {
	_, ok := gpMem.Load().gpWindowDeadline(region, time.Now())
	return ok
}

// gpWindowDeadline: region が GP 中なら、その締め切りを返す。
// メモリの読みロックは 50ms に 1 回だけ、TryRLock で取る (pump の適用中は古い結果を使い、投稿を止めない)。
func (m *memStore) gpWindowDeadline(region string, now time.Time) (time.Time, bool) {
	if m == nil || !m.ready.Load() {
		return time.Time{}, false
	}
	i := gpRegionIdx(region)
	if i < 0 {
		return time.Time{}, false
	}
	n := now.UnixNano()
	s := m.gp.Load()
	if s == nil || n-s.at > int64(50*time.Millisecond) || n < s.at {
		if ns := m.buildGPSnap(now); ns != nil {
			s = ns
			m.gp.Store(ns)
		}
	}
	if s == nil || s.deadline[i] == 0 || n > s.until[i] {
		return time.Time{}, false
	}
	return time.Unix(0, s.deadline[i]), true
}

func (m *memStore) buildGPSnap(now time.Time) *gpSnap {
	if !m.mu.TryRLock() {
		return nil
	}
	var starts, dls [3]time.Time
	all := m.d != nil
	if all {
		for i, r := range gpRegions {
			w := m.d.open[r]
			if w == nil {
				all = false
				break
			}
			starts[i], dls[i] = w.StartsAt, w.OnTimeDeadline
		}
	}
	m.mu.RUnlock()
	s := &gpSnap{at: now.UnixNano()}
	if !all {
		return s
	}
	lo, hi := starts[0], starts[0]
	for _, t := range starts[1:] {
		if t.Before(lo) {
			lo = t
		}
		if t.After(hi) {
			hi = t
		}
	}
	if hi.Sub(lo) > gpFairSync {
		return s
	}
	for i := range gpRegions {
		end := starts[i].Add(gpFairActive)
		dl := dls[i]
		if dl.After(end) {
			dl = end
		}
		s.deadline[i] = dl.UnixNano()
		s.until[i] = end.UnixNano()
	}
	return s
}

var gpFairStats struct {
	waited, bypass atomic.Int64
	mu             sync.Mutex
	waits          [3][]time.Duration
	timeouts       [3]int64
}

func gpFairRecord(i int, d time.Duration, timedOut bool) {
	gpFairStats.mu.Lock()
	if len(gpFairStats.waits[i]) < 1<<15 {
		gpFairStats.waits[i] = append(gpFairStats.waits[i], d)
	}
	if timedOut {
		gpFairStats.timeouts[i]++
	}
	gpFairStats.mu.Unlock()
}

func init() {
	if !gpFairOn {
		return
	}
	go func() {
		for range time.Tick(10 * time.Second) {
			gpFairStats.mu.Lock()
			waits, tos := gpFairStats.waits, gpFairStats.timeouts
			gpFairStats.waits, gpFairStats.timeouts = [3][]time.Duration{}, [3]int64{}
			gpFairStats.mu.Unlock()
			for i, r := range gpRegions {
				if len(waits[i]) == 0 {
					continue
				}
				p50, p90, mx := memDurPercentiles(waits[i])
				slog.Info("gp-fair", "region", r, "n", len(waits[i]), "wait_p50", p50, "wait_p90", p90, "wait_max", mx,
					"max_wait_hits", tos[i], "free", gpFairGateG.freeSlots())
			}
			if w, b := gpFairStats.waited.Swap(0), gpFairStats.bypass.Swap(0); w+b > 0 {
				slog.Info("gp-fair totals", "queued", w, "post_exempt", b)
			}
		}
	}()
}

// gpFairEnter は投稿の DB 区間の前で呼ぶ。返り値の関数を DB 区間の後で必ず呼ぶ。
func gpFairEnter(h *HandlerContext, c *echo.Context) func() {
	if !gpFairOn || h.Mem == nil {
		return func() {}
	}
	region := requestRegion(h, c)
	start := time.Now()
	dl, ok := h.Mem.gpWindowDeadline(region, start)
	if !ok {
		return func() {}
	}
	key := dl
	if region != h.Config.Region {
		key = key.Add(-gpFairFwdLead)
	}
	rel, got := gpFairGateG.acquire(c.Request().Context(), key.UnixNano(), gpFairMaxWait)
	gpFairRecord(gpRegionIdx(region), time.Since(start), !got)
	if opsTraceOn {
		opsAdd(c.Request().Context(), opsSlot, time.Since(start))
	}
	return rel
}

// gpFairPostExemptReq: GP 中の自地域の投稿は fwd-priority のローカル枠 (最大 2 秒待ち) を通さず、
// gpFairEnter の順序に任せる (ISUREAL_GP_FAIR_POST_EXEMPT=1)。
func gpFairPostExemptReq(h *HandlerContext, req *http.Request) bool {
	if !gpFairOn || !gpFairPostExempt || h == nil || h.Mem == nil {
		return false
	}
	if req.Method != http.MethodPost || req.URL.Path != "/api/posts" {
		return false
	}
	if _, ok := h.Mem.gpWindowDeadline(h.Config.Region, time.Now()); !ok {
		return false
	}
	gpFairStats.bypass.Add(1)
	return true
}

// ---- priority gate ----

type prioWaiter struct {
	key     int64
	seq     uint64
	idx     int
	ch      chan struct{}
	granted bool
}

type prioHeap []*prioWaiter

func (h prioHeap) Len() int { return len(h) }
func (h prioHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].seq < h[j].seq
}
func (h prioHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].idx = i; h[j].idx = j }
func (h *prioHeap) Push(x any)   { w := x.(*prioWaiter); w.idx = len(*h); *h = append(*h, w) }
func (h *prioHeap) Pop() any {
	old := *h
	w := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	w.idx = -1
	return w
}

// prioGate: 同時 n 件。空きが無いときは鍵の小さい順 (同じなら到着順) に通す。
type prioGate struct {
	mu   sync.Mutex
	free int
	q    prioHeap
	seq  uint64
}

func newPrioGate(n int) *prioGate {
	if n <= 0 {
		n = 1
	}
	return &prioGate{free: n}
}

// acquire は枠を取って解放関数を返す。maxWait を過ぎるか ctx が切れたら枠無しで進む (解放関数は何もしない)。
func (g *prioGate) acquire(ctx context.Context, key int64, maxWait time.Duration) (func(), bool) {
	g.mu.Lock()
	if g.free > 0 && len(g.q) == 0 {
		g.free--
		g.mu.Unlock()
		return g.release, true
	}
	g.seq++
	w := &prioWaiter{key: key, seq: g.seq, ch: make(chan struct{})}
	heap.Push(&g.q, w)
	g.mu.Unlock()
	gpFairStats.waited.Add(1)
	t := time.NewTimer(maxWait)
	defer t.Stop()
	select {
	case <-w.ch:
		return g.release, true
	case <-t.C:
	case <-ctx.Done():
	}
	g.mu.Lock()
	if w.granted {
		g.mu.Unlock()
		return g.release, true
	}
	heap.Remove(&g.q, w.idx)
	g.mu.Unlock()
	return func() {}, false
}

func (g *prioGate) freeSlots() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.free
}

func (g *prioGate) release() {
	g.mu.Lock()
	if len(g.q) > 0 {
		w := heap.Pop(&g.q).(*prioWaiter)
		w.granted = true
		close(w.ch)
	} else {
		g.free++
	}
	g.mu.Unlock()
}
