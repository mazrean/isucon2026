package isureal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_FWD_PRIORITY_YIELD: 待ちに入った投稿は枠を返すので次の要求が入れる。返した数は N で止まる。
func TestFwdPriorityYieldBounded(t *testing.T) {
	old := fwdPriorityYieldMax
	fwdPriorityYieldMax = 2
	t.Cleanup(func() { fwdPriorityYieldMax = old })
	p := newFwdPriority("asia", 1, 300*time.Millisecond, time.Hour)
	p.lastFwd.Store(time.Now().UnixNano())
	release := make(chan struct{})
	var entered atomic.Int64
	h := p.middleware(func(*echo.Context) bool { return false })(func(c *echo.Context) error {
		entered.Add(1)
		fwdSlotYield(c.Request().Context()) // fsync / PG / ack の待ち
		fwdSlotYield(c.Request().Context()) // 2 回目は何もしない
		<-release
		return nil
	})
	e := echo.New()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			_ = h(e.NewContext(httptest.NewRequest(http.MethodPost, "/api/posts", nil), httptest.NewRecorder()))
		})
	}
	time.Sleep(100 * time.Millisecond)
	// 枠 1 + 待ち中 2 = 3 本 (4 本目は枠待ち)
	if got := entered.Load(); got != 3 {
		t.Fatalf("entered = %d, want 3 (1 slot + 2 parked)", got)
	}
	if got := p.parked.Load(); got != 2 {
		t.Fatalf("parked = %d, want 2", got)
	}
	close(release)
	wg.Wait()
	if p.parked.Load() != 0 || len(p.slots) != 0 {
		t.Fatalf("leak: parked=%d slots=%d", p.parked.Load(), len(p.slots))
	}
}

func TestFwdPriorityYieldOffAndNoHold(t *testing.T) {
	fwdSlotYield(context.Background()) // 枠の無い要求では何もしない
	p := newFwdPriority("asia", 1, 100*time.Millisecond, time.Hour)
	p.lastFwd.Store(time.Now().UnixNano())
	release := make(chan struct{})
	var entered atomic.Int64
	h := p.middleware(func(*echo.Context) bool { return false })(func(c *echo.Context) error {
		entered.Add(1)
		fwdSlotYield(c.Request().Context())
		<-release
		return nil
	})
	e := echo.New()
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			_ = h(e.NewContext(httptest.NewRequest(http.MethodPost, "/api/auth/login", nil), httptest.NewRecorder()))
		})
	}
	time.Sleep(50 * time.Millisecond)
	if entered.Load() != 1 {
		t.Fatalf("off: entered = %d, want 1", entered.Load())
	}
	time.Sleep(100 * time.Millisecond) // 上限 (100ms) で枠なしに通す
	p.mu.Lock()
	tb := p.timeoutBy["asia/other"]
	p.mu.Unlock()
	if entered.Load() != 2 || tb != 1 {
		t.Fatalf("after max wait: entered=%d timeoutBy=%d", entered.Load(), tb)
	}
	close(release)
	wg.Wait()
	if len(p.slots) != 0 {
		t.Fatal("slot leak")
	}
}

// ISUREAL_STORE_PRIO: 優先列が先に取り出され、通常列は空きに足される。
func TestStoreNextPrioBatch(t *testing.T) {
	e := &storeEngine{reqs: make(chan *storeReq, 1024), reqsHi: make(chan *storeReq, 1024)}
	var lo, hi []*storeReq
	for range 3 {
		r := &storeReq{}
		lo = append(lo, r)
		e.reqs <- r
	}
	for range 2 {
		r := &storeReq{}
		hi = append(hi, r)
		e.reqsHi <- r
	}
	b := e.nextPrioBatch()
	want := append(append([]*storeReq{}, hi...), lo...)
	if len(b) != len(want) {
		t.Fatalf("len %d want %d", len(b), len(want))
	}
	for i := range want {
		if b[i] != want[i] {
			t.Fatalf("order at %d", i)
		}
	}
	// 上限: 優先列だけで埋まれば通常列は次へ
	for range storeMaxBatch + 1 {
		e.reqsHi <- &storeReq{}
	}
	e.reqs <- &storeReq{}
	if b := e.nextPrioBatch(); len(b) != storeMaxBatch || len(e.reqs) != 1 {
		t.Fatalf("batch=%d lo left=%d", len(b), len(e.reqs))
	}
	if b := e.nextPrioBatch(); len(b) != 2 {
		t.Fatalf("rest batch=%d want 2", len(b))
	}
	// 通常列だけでもブロックせずに取れる
	e.reqs <- &storeReq{}
	if b := e.nextPrioBatch(); len(b) != 1 {
		t.Fatal("lo only")
	}
}

