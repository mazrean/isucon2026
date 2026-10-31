package isureal

import (
	"context"
	"testing"
	"time"
)

func TestAdmitGateOff(t *testing.T) {
	if newAdmitGate(0, 0, 3, time.Second) != nil {
		t.Fatal("want nil when both limits are 0")
	}
}

// POST_MIN: 投稿以外は N-K まで。投稿は残りの K を使える。上限待ちは maxWait で断らずに入れて数える。
func TestAdmitGatePostMinAndOver(t *testing.T) {
	g := newAdmitGate(2, 0, 1, 80*time.Millisecond)
	ctx := context.Background()
	l1 := g.enter(ctx, false, 0, "asia/other")
	start := time.Now()
	l2 := g.enter(ctx, true, 100, "asia/post") // 投稿用の 1 枠
	if time.Since(start) > 20*time.Millisecond {
		t.Fatal("post should use the reserved slot without waiting")
	}
	start = time.Now()
	l3 := g.enter(ctx, false, 0, "asia/other") // 上限で待ち、maxWait で入る
	if el := time.Since(start); el < 70*time.Millisecond {
		t.Fatalf("non-post entered too early: %v", el)
	}
	g.mu.Lock()
	if g.over["asia/other"] != 1 || g.n != 3 {
		t.Fatalf("over=%v n=%d", g.over, g.n)
	}
	g.mu.Unlock()
	l1()
	l2()
	l3()
	if g.n != 0 || g.bytes != 0 {
		t.Fatalf("leak n=%d bytes=%d", g.n, g.bytes)
	}
}

// 解放で待ちが起きる。画像のバイト数の上限も効く (1 本目は大きくても入る)。
func TestAdmitGateBytesWake(t *testing.T) {
	g := newAdmitGate(0, 1000, 0, 2*time.Second)
	ctx := context.Background()
	l1 := g.enter(ctx, true, 5000, "asia/post")
	done := make(chan time.Duration, 1)
	start := time.Now()
	go func() {
		l := g.enter(ctx, true, 10, "asia/post")
		done <- time.Since(start)
		l()
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("second post entered over the byte limit")
	default:
	}
	l1()
	if el := <-done; el > time.Second {
		t.Fatalf("not woken on release: %v", el)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.over) != 0 {
		t.Fatalf("over=%v", g.over)
	}
	// 投稿以外は本文のバイト数を数えない
	if !g.fits(false, 1<<30) {
		t.Fatal("non-post should not count bytes")
	}
}

func TestAdmitGateCtxDone(t *testing.T) {
	g := newAdmitGate(1, 0, 0, time.Hour)
	l1 := g.enter(context.Background(), true, 0, "k")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l2 := g.enter(ctx, true, 0, "k")
	l2()
	l1()
}
