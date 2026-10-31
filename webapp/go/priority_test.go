package isureal

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

func TestFwdPriorityLimitsLocalOnlyWhileForwarding(t *testing.T) {
	p := newFwdPriority("asia", 2, 200*time.Millisecond, time.Hour)
	var cur, peak atomic.Int64
	release := make(chan struct{})
	h := p.middleware(func(c *echo.Context) bool { return c.Request().Header.Get("X-Fwd") == "1" })(func(c *echo.Context) error {
		n := cur.Add(1)
		for {
			m := peak.Load()
			if n <= m || peak.CompareAndSwap(m, n) {
				break
			}
		}
		<-release
		cur.Add(-1)
		return nil
	})
	e := echo.New()
	run := func(method, path string, fwd bool) {
		req := httptest.NewRequest(method, path, nil)
		if fwd {
			req.Header.Set("X-Fwd", "1")
		}
		_ = h(e.NewContext(req, httptest.NewRecorder()))
	}

	// 転送を一度見せて、ローカルの制限を有効にする。
	p.lastFwd.Store(time.Now().UnixNano())
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); run(http.MethodPost, "/api/posts", false) }()
	}
	time.Sleep(50 * time.Millisecond)
	if got := peak.Load(); got != 2 {
		t.Fatalf("local concurrency peak = %d, want 2", got)
	}
	// 転送書き込みはセマフォ待ち無しで入る。
	wg.Add(1)
	go func() { defer wg.Done(); run(http.MethodPost, "/api/posts", true) }()
	time.Sleep(50 * time.Millisecond)
	if got := cur.Load(); got != 3 {
		t.Fatalf("in-flight with forwarded = %d, want 3", got)
	}
	// 待ちの上限を過ぎたローカルは 503 にせず処理される。
	time.Sleep(250 * time.Millisecond)
	if got := cur.Load(); got != 6 {
		t.Fatalf("in-flight after max wait = %d, want 6", got)
	}
	close(release)
	wg.Wait()
}

// ISUREAL_FWD_PRIORITY_POST_SLOTS: 投稿が自分の枠を埋めても、他のローカル書き込みは共有の枠で待たずに入る。
func TestFwdPriorityPostSlotsSeparate(t *testing.T) {
	old := fwdPriorityPostSlots
	fwdPriorityPostSlots = 2
	t.Cleanup(func() { fwdPriorityPostSlots = old })
	p := newFwdPriority("asia", 2, time.Second, time.Hour)
	p.lastFwd.Store(time.Now().UnixNano())
	release := make(chan struct{})
	var posts, others atomic.Int64
	h := p.middleware(func(*echo.Context) bool { return false })(func(c *echo.Context) error {
		if c.Request().URL.Path == "/api/posts" {
			posts.Add(1)
			<-release
			return nil
		}
		others.Add(1)
		return nil
	})
	e := echo.New()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			_ = h(e.NewContext(httptest.NewRequest(http.MethodPost, "/api/posts", nil), httptest.NewRecorder()))
		})
	}
	time.Sleep(50 * time.Millisecond)
	if posts.Load() != 2 {
		t.Fatalf("posts in flight = %d, want 2 (own pool)", posts.Load())
	}
	start := time.Now()
	for range 2 {
		_ = h(e.NewContext(httptest.NewRequest(http.MethodPost, "/api/auth/login", nil), httptest.NewRecorder()))
	}
	if el := time.Since(start); others.Load() != 2 || el > 100*time.Millisecond {
		t.Fatalf("login waited behind posts: n=%d el=%v", others.Load(), el)
	}
	close(release)
	wg.Wait()
}

func TestFwdPriorityTarget(t *testing.T) {
	cases := []struct {
		method, path string
		want         bool
	}{
		{http.MethodPost, "/api/posts", true},
		{http.MethodPost, "/api/auth/login", true},
		{http.MethodGet, "/api/posts/1", false},
		{http.MethodPost, "/api/initialize", false},
		{http.MethodPost, "/api/admin/posting-window", false},
		{http.MethodPost, "/internal/mem/push", false},
	}
	for _, c := range cases {
		if got := fwdPriorityTarget(httptest.NewRequest(c.method, c.path, nil)); got != c.want {
			t.Errorf("%s %s = %v, want %v", c.method, c.path, got, c.want)
		}
	}
}
