package isureal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

func wmTestStore() *memStore {
	m := &memStore{d: newMemData(), useLease: true}
	m.ready.Store(true)
	m.extendLeaseFrom(0)
	m.w.pumpWM.Store(true)
	return m
}

func withMemNow(t *testing.T, now time.Time) {
	old := memNow
	memNow = func() time.Time { return now }
	t.Cleanup(func() { memNow = old })
}

// 目標が無いリクエスト (pump、フラグ off) はガードを素通り。目標があれば wm >= 目標のときだけ。
func TestMemWMGuard(t *testing.T) {
	m := wmTestStore()
	if !m.onCtx(context.Background()) {
		t.Fatal("no target must pass")
	}
	ctx := context.WithValue(context.Background(), memWMTargetKey{}, int64(1000))
	m.w.set(999)
	if m.onCtx(ctx) {
		t.Fatal("wm < target must not read memory")
	}
	m.w.advance(1000)
	if !m.onCtx(ctx) {
		t.Fatal("wm == target must read memory")
	}
	m.ready.Store(false)
	if m.onCtx(ctx) {
		t.Fatal("not ready must not read memory")
	}
	var nilStore *memStore
	if nilStore.onCtx(ctx) || nilStore.wmOK(context.Background()) {
		t.Fatal("nil store must be off")
	}
}

// 単調増加: 古い watermark (順序の入れ替わり) で後退しない。set は置き換える。
func TestMemWMAdvanceMonotonic(t *testing.T) {
	var w memWM
	w.advance(50)
	w.advance(30)
	if got := w.wm.Load(); got != 50 {
		t.Fatalf("wm=%d want 50", got)
	}
	w.set(10)
	if got := w.wm.Load(); got != 10 {
		t.Fatalf("set: wm=%d want 10", got)
	}
}

// 待ち: 遅れて届いた watermark で解放される。
func TestMemWMWaitReleasedByAdvance(t *testing.T) {
	m := wmTestStore()
	now := time.Unix(1000, 0)
	withMemNow(t, now)
	target := now.Add(memWMEps).UnixNano()
	m.w.set(now.UnixNano()) // 目標より前
	done := make(chan memWMOutcome, 1)
	go func() { done <- m.waitWM(target, now) }()
	time.Sleep(30 * time.Millisecond)
	select {
	case o := <-done:
		t.Fatalf("released early: %+v", o)
	default:
	}
	m.w.advance(target - 1) // まだ足りない
	time.Sleep(20 * time.Millisecond)
	select {
	case o := <-done:
		t.Fatalf("released by insufficient wm: %+v", o)
	default:
	}
	m.w.advance(target)
	select {
	case o := <-done:
		if !o.ok {
			t.Fatalf("want ok, got %+v", o)
		}
	case <-time.After(time.Second):
		t.Fatal("not released")
	}
}

// 上限: watermark が進まなければ memWMMaxWait で打ち切り、ガードで DB 経路になる (pump 停止・回線詰まり)。
func TestMemWMWaitTimeoutFallsBack(t *testing.T) {
	m := wmTestStore()
	old := memWMMaxWait
	memWMMaxWait = 80 * time.Millisecond
	t.Cleanup(func() { memWMMaxWait = old })
	start := time.Now()
	target := start.Add(memWMEps).UnixNano()
	o := m.waitWM(target, start) // memNow は実時刻
	if o.ok || o.notOn {
		t.Fatalf("want fallback, got %+v", o)
	}
	if el := time.Since(start); el < 80*time.Millisecond || el > 500*time.Millisecond {
		t.Fatalf("waited %v", el)
	}
	ctx := context.WithValue(context.Background(), memWMTargetKey{}, target)
	if m.onCtx(ctx) {
		t.Fatal("after timeout the request must use the DB path")
	}
	// 遅れて watermark が届いたら、同じリクエストの後続の読みはメモリでよい
	m.w.advance(target)
	if !m.onCtx(ctx) {
		t.Fatal("late wm should allow memory")
	}
}

// not-on (lease 切れ・再構築中) なら待たない。
func TestMemWMWaitNotOnReturnsImmediately(t *testing.T) {
	m := wmTestStore()
	m.leaseUntil.Store(0)
	start := time.Now()
	o := m.waitWM(start.Add(time.Hour).UnixNano(), start)
	if !o.notOn || time.Since(start) > 50*time.Millisecond {
		t.Fatalf("want immediate notOn, got %+v after %v", o, time.Since(start))
	}
}

// middleware: 時刻を注入して目標 = T_r + ε。pump が wm モードでなければ目標を付けない (ack 方式で正しい)。
func TestMemReadWaitMiddleware(t *testing.T) {
	m := wmTestStore()
	now := time.Unix(2000, 0)
	withMemNow(t, now)
	want := now.Add(memWMEps).UnixNano()
	m.w.set(want) // 待たずに通る
	var got any
	var ok bool
	e := echo.New()
	e.Use(memReadWait(m))
	e.GET("/api/feeds/:feed", func(c *echo.Context) error {
		got = c.Request().Context().Value(memWMTargetKey{})
		ok = m.onCtx(c.Request().Context())
		return c.NoContent(http.StatusOK)
	})
	e.GET("/api/images/:id", func(c *echo.Context) error {
		got = c.Request().Context().Value(memWMTargetKey{})
		return c.NoContent(http.StatusOK)
	})
	do := func(path string) {
		got, ok = nil, false
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	}
	do("/api/feeds/friends")
	if got != want || !ok {
		t.Fatalf("target=%v ok=%v want %d", got, ok, want)
	}
	do("/api/images/x")
	if got != nil {
		t.Fatalf("images must not wait: %v", got)
	}
	// pump が wm モードでない: 目標は付くが待たず、ガードは通る。処理中に wm モードの dump へ入れ替わったら読む時点で判定。
	m.w.pumpWM.Store(false)
	m.w.set(0)
	var ctx context.Context
	e.GET("/api/me", func(c *echo.Context) error {
		ctx = c.Request().Context()
		return c.NoContent(http.StatusOK)
	})
	do("/api/me")
	if ctx.Value(memWMTargetKey{}) != want || !m.onCtx(ctx) {
		t.Fatal("ack-mode pump: target set and guard passes")
	}
	m.w.pumpWM.Store(true)
	m.w.set(want - 1)
	if m.onCtx(ctx) {
		t.Fatal("resynced to wm-mode pump mid-request with wm < target: must use DB")
	}
	m.w.advance(want)
	if !m.onCtx(ctx) {
		t.Fatal("wm reached target")
	}
}

func wmTestReceiver(m *memStore, epoch string, seq int64) *memReceiver {
	r := &memReceiver{m: m, pumpAddr: "127.0.0.1:1", epoch: epoch, seq: seq}
	r.rsCond = sync.NewCond(&r.rsMu)
	r.seqCond = sync.NewCond(&r.mu)
	return r
}

// peer: seq まで反映済みのときだけ wm を進める。epoch 違い (pump 再起動) の wm は無視。並列 push の入れ替わりで後退しない。
func TestMemReceiverWMAfterPush(t *testing.T) {
	m := wmTestStore()
	m.w.set(0)
	r := wmTestReceiver(m, "e1", 5)
	r.wmAfterPush(memPushRequest{Epoch: "e1", Seq: 6, WM: 600}) // seq 6 が未着
	if m.w.wm.Load() != 0 {
		t.Fatal("wm advanced before its batches were applied")
	}
	r.wmAfterPush(memPushRequest{Epoch: "e1", Seq: 5, WM: 500})
	if m.w.wm.Load() != 500 {
		t.Fatalf("wm=%d want 500", m.w.wm.Load())
	}
	r.wmAfterPush(memPushRequest{Epoch: "e2", Seq: 1, WM: 900}) // 新 epoch
	if m.w.wm.Load() != 500 {
		t.Fatal("wm from another epoch applied")
	}
	r.seq = 8
	r.wmAfterPush(memPushRequest{Epoch: "e1", Seq: 8, WM: 800})
	r.wmAfterPush(memPushRequest{Epoch: "e1", Seq: 7, WM: 700}) // 遅れて届いた古い push
	if m.w.wm.Load() != 800 {
		t.Fatalf("wm=%d want 800 (no regress)", m.w.wm.Load())
	}
	m.ready.Store(false)
	r.wmAfterPush(memPushRequest{Epoch: "e1", Seq: 8, WM: 1000})
	if m.w.wm.Load() != 800 {
		t.Fatal("wm advanced while not ready")
	}
}

// push の経路全体: バッチを反映した push の WM で進む。heartbeat (バッチ無し) でも進む。
func TestMemReceiverPushCarriesWM(t *testing.T) {
	m := wmTestStore()
	r := wmTestReceiver(m, "e1", 0)
	req := memPushRequest{Epoch: "e1", Seq: 1, Batches: []memBatch{{Seq: 1}}, WM: 111}
	if !r.push(req) {
		t.Fatal("push failed")
	}
	r.wmAfterPush(req)
	if m.w.wm.Load() != 111 {
		t.Fatalf("wm=%d", m.w.wm.Load())
	}
	hb := memPushRequest{Epoch: "e1", Seq: 1, WM: 222}
	if !r.push(hb) {
		t.Fatal("heartbeat failed")
	}
	r.wmAfterPush(hb)
	if m.w.wm.Load() != 222 {
		t.Fatalf("heartbeat wm=%d", m.w.wm.Load())
	}
}

// peer 再起動・epoch 切替: invalidate で wm は 0 (メモリは読まない)。
func TestMemReceiverInvalidateResetsWM(t *testing.T) {
	m := wmTestStore()
	r := wmTestReceiver(m, "e1", 3)
	m.w.set(12345)
	r.invalidate()
	if m.w.wm.Load() != 0 || m.ready.Load() {
		t.Fatalf("after invalidate wm=%d ready=%v", m.w.wm.Load(), m.ready.Load())
	}
}

// pump: wm モードの peer は ack を待たない。そうでない peer (落ちている) は従来どおり lease 切れまで待つ。
func TestMemWaitAckedSkipsWMPeers(t *testing.T) {
	old := memWatermark
	memWatermark = true
	t.Cleanup(func() { memWatermark = old })
	wmPeer := &memPeer{addr: "127.0.0.1:1", lastGrantAt: time.Now(), wmMode: true}
	ps := &memPeers{epoch: "e1", peers: []*memPeer{wmPeer}}
	ps.cond = sync.NewCond(&ps.mu)
	ps.seq = 3
	start := time.Now()
	ps.waitAcked()
	if el := time.Since(start); el > 50*time.Millisecond {
		t.Fatalf("waitAcked on wm peer took %v", el)
	}
	ps.peers = append(ps.peers, &memPeer{addr: "127.0.0.1:2", lastGrantAt: time.Now()})
	start = time.Now()
	ps.waitAcked()
	if el := time.Since(start); el < memLease {
		t.Fatalf("non-wm peer must still be waited for (%v)", el)
	}
	// フラグ off なら wm 申告があっても待つ (既定の挙動は不変)
	memWatermark = false
	wmPeer.lastGrantAt = time.Now()
	ps.peers = ps.peers[:1]
	start = time.Now()
	ps.waitAcked()
	if el := time.Since(start); el < memLease {
		t.Fatalf("flag off must wait (%v)", el)
	}
}

// dump 時の申告: RemoteAddr のホストで peer を特定。特定できない wm=0 は全 peer を ack 待ちへ (安全側)。
func TestMemMarkWMMode(t *testing.T) {
	ps := &memPeers{peers: []*memPeer{{addr: "192.168.2.10:8080"}, {addr: "192.168.3.10:8080"}}}
	ps.markWMMode("192.168.2.10:51234", true)
	if !ps.peers[0].wmMode || ps.peers[1].wmMode {
		t.Fatal("wrong peer marked")
	}
	ps.markWMMode("192.168.3.10:4444", true)
	ps.markWMMode("10.0.0.1:1", true) // 不明 + wm=1: 何もしない
	if !ps.peers[0].wmMode || !ps.peers[1].wmMode {
		t.Fatal("unknown wm=1 must not change modes")
	}
	ps.markWMMode("192.168.2.10:9", false) // 再起動した peer がフラグ無し
	if ps.peers[0].wmMode || !ps.peers[1].wmMode {
		t.Fatal("restarted non-wm peer must be acked again")
	}
	ps.markWMMode("10.0.0.1:1", false)
	if ps.peers[1].wmMode {
		t.Fatal("unknown wm=0 must reset all peers")
	}
}

// pump: setWM は単調で、newEpoch で 0 に戻る。並列数の上限を超えて送らない。
func TestMemPeersSetWM(t *testing.T) {
	ps := &memPeers{epoch: "e1", peers: []*memPeer{{addr: "127.0.0.1:1", wmInflight: memWMInflight, wmMode: true}, {addr: "127.0.0.1:2"}}}
	ps.cond = sync.NewCond(&ps.mu)
	ps.setWM(100)
	ps.setWM(90)
	if ps.wm != 100 || ps.peers[0].wmInflight != memWMInflight || ps.peers[1].wmInflight != 0 {
		t.Fatalf("wm=%d inflight=%d/%d", ps.wm, ps.peers[0].wmInflight, ps.peers[1].wmInflight)
	}
	ps.mu.Lock()
	ps.epoch = memNewEpoch()
	ps.seq, ps.wm = 0, 0 // newEpoch と同じ (invalidateAll はネットワークを使うので直接)
	ps.mu.Unlock()
	ps.setWM(5)
	if ps.wm != 5 {
		t.Fatalf("after epoch wm=%d", ps.wm)
	}
}

// 専用の wm 通知: データ (push) より先に届いたら保留し、push の反映後に適用。順序の入れ替わりで後退しない。
func TestMemReceiverWMMsgPending(t *testing.T) {
	m := wmTestStore()
	m.w.set(0)
	r := wmTestReceiver(m, "e1", 1)
	r.wmMsg(memWMMsg{Epoch: "e1", Seq: 1, WM: 100})
	r.wmMsg(memWMMsg{Epoch: "e1", Seq: 3, WM: 300}) // seq 2,3 が未着
	r.wmMsg(memWMMsg{Epoch: "e1", Seq: 2, WM: 200})
	r.wmMsg(memWMMsg{Epoch: "e0", Seq: 1, WM: 999}) // 古い epoch
	if m.w.wm.Load() != 100 {
		t.Fatalf("wm=%d want 100", m.w.wm.Load())
	}
	req := memPushRequest{Epoch: "e1", Seq: 2, Batches: []memBatch{{Seq: 2}}}
	if !r.push(req) {
		t.Fatal("push failed")
	}
	r.wmAfterPush(req)
	if m.w.wm.Load() != 200 || len(r.wmPending) != 1 {
		t.Fatalf("after seq2 wm=%d pending=%d", m.w.wm.Load(), len(r.wmPending))
	}
	req = memPushRequest{Epoch: "e1", Seq: 3, Batches: []memBatch{{Seq: 3}}, WM: 250}
	if !r.push(req) {
		t.Fatal("push failed")
	}
	r.wmAfterPush(req)
	if m.w.wm.Load() != 300 || len(r.wmPending) != 0 {
		t.Fatalf("after seq3 wm=%d pending=%d", m.w.wm.Load(), len(r.wmPending))
	}
	r.wmMsg(memWMMsg{Epoch: "e1", Seq: 3, WM: 150}) // 遅れて届いた古い通知
	if m.w.wm.Load() != 300 {
		t.Fatal("wm regressed")
	}
}

func TestMemPercentiles(t *testing.T) {
	var d []time.Duration
	for i := 1; i <= 100; i++ {
		d = append(d, time.Duration(i)*time.Millisecond)
	}
	p50, p90, p99 := memPercentiles(d)
	if p50 != 51*time.Millisecond || p90 != 91*time.Millisecond || p99 != 100*time.Millisecond {
		t.Fatalf("%v %v %v", p50, p90, p99)
	}
}
