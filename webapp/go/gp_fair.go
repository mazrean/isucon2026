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

// ISUREAL_GP_FAIR=1: GP (on-time の窓が短い投稿期間) の間だけ、asia1 で投稿の DB 区間
// (createPostBatch / 投稿の tx。posted_at はこの中で決まる) に入る順番を、締め切りの早い順 (EDF) にする。
// 鍵 = 投稿者の地域の窓の on_time_deadline − (転送された投稿なら ISUREAL_GP_FAIR_FWD_LEAD_MS)。
// GP は 3 地域ほぼ同時なので、実質「eu/am から転送された投稿を先に、同じ鍵なら到着順」になる。
// 18156: 転送の書き込み p90 が GP で 1.5s (DB のプール 60 本を asia ローカルの約 1000 件と取り合う)。
//
// 順序を変えるだけで、断らない: 枠を ISUREAL_GP_FAIR_MAX_WAIT_MS 待っても取れなければ枠無しで進む。
// クライアントが切れた場合も進む (結果は使われないが、201 の前に永続化する既存の流れを変えない)。
var (
	gpFairOn         = os.Getenv("ISUREAL_GP_FAIR") == "1"
	gpFairSlots      = envIntOrDefault("ISUREAL_GP_FAIR_SLOTS", 32)
	gpFairFwdLead    = time.Duration(envIntOrDefault("ISUREAL_GP_FAIR_FWD_LEAD_MS", 1000)) * time.Millisecond
	gpFairMaxWait    = time.Duration(envIntOrDefault("ISUREAL_GP_FAIR_MAX_WAIT_MS", 3000)) * time.Millisecond
	gpFairMaxWindow  = time.Duration(envIntOrDefault("ISUREAL_GP_FAIR_MAX_WINDOW_SEC", 120)) * time.Second
	gpFairAfter      = time.Duration(envIntOrDefault("ISUREAL_GP_FAIR_AFTER_MS", 5000)) * time.Millisecond
	gpFairPostExempt = os.Getenv("ISUREAL_GP_FAIR_POST_EXEMPT") == "1"
	gpFairGateG      = newPrioGate(gpFairSlots)
	// ISUREAL_GP_JPEG_REMOTES_MAX=N (既定 0 = 制限なし、GP_FAIR とは独立): 自地域が GP の間、JPEG 検査の委譲先を
	// ISUREAL_JPEG_REMOTES の先頭 N 個 (asia2) に限る。eu/am への委譲は 360KB を asia1 から地域外へ送り
	// (18156 の GP で約 530 件/10s ≈ 150Mbps)、asia1 の送信帯域 (bw_out_allowance_exceeded) を mem の push と取り合う。
	gpJPEGRemotesMax = envIntOrDefault("ISUREAL_GP_JPEG_REMOTES_MAX", 0)
	gpMem            atomic.Pointer[memStore] // pump (asia1) のメモリ。setupMem が入れる
)

// gpLocalActive: 自地域 (pump の地域) が GP 中か。
func gpLocalActive(region string) bool {
	_, ok := gpMem.Load().gpWindowDeadline(region, time.Now())
	return ok
}

// gpWindowDeadline: region の開いている窓が GP (on-time が gpFairMaxWindow 以下) で、
// 締め切り + gpFairAfter までなら、その締め切りを返す。
func (m *memStore) gpWindowDeadline(region string, now time.Time) (time.Time, bool) {
	if m == nil || !m.ready.Load() {
		return time.Time{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.d == nil {
		return time.Time{}, false
	}
	w := m.d.open[region]
	if w == nil {
		return time.Time{}, false
	}
	dl := w.OnTimeDeadline
	if dl.Sub(w.StartsAt) > gpFairMaxWindow || now.Before(w.StartsAt) || now.After(dl.Add(gpFairAfter)) {
		return time.Time{}, false
	}
	return dl, true
}

var gpFairStats struct{ gated, waited, timeouts, bypass atomic.Int64 }

func init() {
	if !gpFairOn {
		return
	}
	go func() {
		for range time.Tick(10 * time.Second) {
			g, w, t := gpFairStats.gated.Swap(0), gpFairStats.waited.Swap(0), gpFairStats.timeouts.Swap(0)
			if g > 0 {
				slog.Info("gp-fair", "gated", g, "waited", w, "timeouts", t, "post_exempt", gpFairStats.bypass.Swap(0))
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
	now := time.Now()
	dl, ok := h.Mem.gpWindowDeadline(region, now)
	if !ok {
		return func() {}
	}
	key := dl
	if region != h.Config.Region {
		key = key.Add(-gpFairFwdLead)
	}
	gpFairStats.gated.Add(1)
	return gpFairGateG.acquire(c.Request().Context(), key.UnixNano(), gpFairMaxWait)
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
func (g *prioGate) acquire(ctx context.Context, key int64, maxWait time.Duration) func() {
	g.mu.Lock()
	if g.free > 0 && len(g.q) == 0 {
		g.free--
		g.mu.Unlock()
		return g.release
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
		return g.release
	case <-t.C:
	case <-ctx.Done():
	}
	g.mu.Lock()
	if w.granted {
		g.mu.Unlock()
		return g.release
	}
	heap.Remove(&g.q, w.idx)
	g.mu.Unlock()
	gpFairStats.timeouts.Add(1)
	return func() {}
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
