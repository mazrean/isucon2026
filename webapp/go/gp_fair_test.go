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
			rel, _ := g.acquire(context.Background(), int64(i%7), 10*time.Second)
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
	hold, _ := g.acquire(context.Background(), 0, time.Second)
	var mu sync.Mutex
	var order []int64
	var wg sync.WaitGroup
	keys := []int64{50, 10, 40, 20, 30}
	for _, k := range keys {
		wg.Add(1)
		go func(k int64) {
			defer wg.Done()
			rel, _ := g.acquire(context.Background(), k, 10*time.Second)
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
	hold, _ := g.acquire(context.Background(), 0, time.Second)
	start := time.Now()
	rel, got := g.acquire(context.Background(), 1, 20*time.Millisecond)
	if time.Since(start) < 20*time.Millisecond || got {
		t.Fatal("did not wait / should not have a slot")
	}
	rel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r2, _ := g.acquire(ctx, 2, time.Second)
	r2()
	hold()
	if g.free != 1 || len(g.q) != 0 {
		t.Fatalf("free=%d q=%d", g.free, len(g.q))
	}
}

func gpTestMem(starts [3]time.Time, onTime time.Duration) *memStore {
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	for i, r := range gpRegions {
		m.d.open[r] = &memWindow{PostingWindow: PostingWindow{Region: r, StartsAt: starts[i], OnTimeDeadline: starts[i].Add(onTime)}}
	}
	return m
}

func TestGPWindowDeadline(t *testing.T) {
	now := time.Now()
	// 3 地域が同時に開いた = GP
	gp := gpTestMem([3]time.Time{now.Add(-2 * time.Second), now.Add(-2*time.Second + 300*time.Millisecond), now.Add(-2 * time.Second)}, 24*time.Hour)
	dl, ok := gp.gpWindowDeadline("europe", now)
	if !ok {
		t.Fatal("simultaneous open should be GP")
	}
	if want := now.Add(-2*time.Second + 300*time.Millisecond + gpFairActive); !dl.Equal(want) {
		t.Fatalf("deadline %v want %v", dl, want)
	}
	if _, ok := gp.gpWindowDeadline("asia", now.Add(gpFairActive)); ok {
		t.Fatal("after ACTIVE is not GP")
	}
	// 3 秒おきに回して開く通常の日 = GP ではない
	day := gpTestMem([3]time.Time{now.Add(-6 * time.Second), now.Add(-3 * time.Second), now}, 9*time.Second)
	for _, r := range gpRegions {
		if _, ok := day.gpWindowDeadline(r, now); ok {
			t.Fatalf("staggered day is not GP (%s)", r)
		}
	}
	// 窓が欠けている
	miss := gpTestMem([3]time.Time{now, now, now}, time.Hour)
	delete(miss.d.open, "america")
	if _, ok := miss.gpWindowDeadline("asia", now); ok {
		t.Fatal("missing window is not GP")
	}
	if gpMem.Load() == nil && gpLocalActive("europe") {
		t.Fatal("nil mem must not be GP")
	}
	gpMem.Store(gp)
	defer gpMem.Store(nil)
	if !gpLocalActive("europe") {
		t.Fatal("gpLocalActive mismatch")
	}
}

// pump の適用 (書きロック) 中も、判定は古いキャッシュで即答して投稿を止めない。
func TestGPWindowDeadlineNoBlock(t *testing.T) {
	now := time.Now()
	m := gpTestMem([3]time.Time{now, now, now}, time.Hour)
	if _, ok := m.gpWindowDeadline("asia", now); !ok {
		t.Fatal("want GP")
	}
	m.mu.Lock()
	done := make(chan bool)
	go func() {
		_, ok := m.gpWindowDeadline("asia", now.Add(200*time.Millisecond))
		done <- ok
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("stale cache should still say GP")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked on the write lock")
	}
	m.mu.Unlock()
}
