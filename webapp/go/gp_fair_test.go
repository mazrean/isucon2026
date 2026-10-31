package isureal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 優先付けがあっても全件が処理され、同時数が上限を超えないこと。
func TestPrioGateAllComplete(t *testing.T) {
	g := newPrioGate(4)
	var inflight, maxIn, done atomic.Int64
	var wg sync.WaitGroup
	const n = 2000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rel := g.acquire(context.Background(), int64(i%7), 10*time.Second)
			c := inflight.Add(1)
			for {
				m := maxIn.Load()
				if c <= m || maxIn.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(50 * time.Microsecond)
			inflight.Add(-1)
			rel()
			done.Add(1)
		}(i)
	}
	wg.Wait()
	if done.Load() != n {
		t.Fatalf("done=%d want %d", done.Load(), n)
	}
	if maxIn.Load() > 4 {
		t.Fatalf("max inflight %d > 4", maxIn.Load())
	}
	if g.free != 4 || len(g.q) != 0 {
		t.Fatalf("gate not drained: free=%d q=%d", g.free, len(g.q))
	}
}

// 待っている間は鍵の小さい順に通る (eu/am の転送を asia より先に)。
func TestPrioGateOrder(t *testing.T) {
	g := newPrioGate(1)
	hold := g.acquire(context.Background(), 0, time.Second)
	var mu sync.Mutex
	var order []int64
	var wg sync.WaitGroup
	keys := []int64{50, 10, 40, 20, 30}
	for _, k := range keys {
		wg.Add(1)
		go func(k int64) {
			defer wg.Done()
			rel := g.acquire(context.Background(), k, 10*time.Second)
			mu.Lock()
			order = append(order, k)
			mu.Unlock()
			rel()
		}(k)
	}
	for {
		g.mu.Lock()
		n := len(g.q)
		g.mu.Unlock()
		if n == len(keys) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	hold()
	wg.Wait()
	for i := 1; i < len(order); i++ {
		if order[i-1] > order[i] {
			t.Fatalf("order %v not ascending", order)
		}
	}
}

// 枠が取れないまま maxWait / ctx 切れになっても、断らずに進み、枠の数を壊さない。
func TestPrioGateTimeoutProceeds(t *testing.T) {
	g := newPrioGate(1)
	hold := g.acquire(context.Background(), 0, time.Second)
	start := time.Now()
	rel := g.acquire(context.Background(), 1, 20*time.Millisecond)
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("did not wait")
	}
	rel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.acquire(ctx, 2, time.Second)()
	hold()
	if g.free != 1 || len(g.q) != 0 {
		t.Fatalf("free=%d q=%d", g.free, len(g.q))
	}
}

func TestGPWindowDeadline(t *testing.T) {
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	now := time.Now()
	m.d.open["europe"] = &memWindow{PostingWindow: PostingWindow{StartsAt: now.Add(-2 * time.Second), OnTimeDeadline: now.Add(8 * time.Second)}}
	m.d.open["asia"] = &memWindow{PostingWindow: PostingWindow{StartsAt: now.Add(-time.Hour), OnTimeDeadline: now.Add(23 * time.Hour)}}
	if _, ok := m.gpWindowDeadline("europe", now); !ok {
		t.Fatal("short window should be GP")
	}
	if _, ok := m.gpWindowDeadline("asia", now); ok {
		t.Fatal("day window is not GP")
	}
	if _, ok := m.gpWindowDeadline("europe", now.Add(time.Minute)); ok {
		t.Fatal("after deadline+after is not GP")
	}
	if _, ok := m.gpWindowDeadline("america", now); ok {
		t.Fatal("no window")
	}
	if gpMem.Load() == nil && gpLocalActive("europe") {
		t.Fatal("nil mem must not be GP")
	}
	gpMem.Store(m)
	defer gpMem.Store(nil)
	if !gpLocalActive("europe") || gpLocalActive("asia") {
		t.Fatal("gpLocalActive mismatch")
	}
}
