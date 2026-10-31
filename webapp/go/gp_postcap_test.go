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

// 枠 N を超えて同時に走らない。枠が空かなければ maxWait 後に枠無しで進む (断らない)。対象外の要求は素通り。
func TestPostCapLimitsConcurrency(t *testing.T) {
	var cur, peak atomic.Int64
	p := newPostCap(2, 2*time.Second, 0, func(*echo.Context) bool { return true })
	h := p.middleware(func(c *echo.Context) error {
		n := cur.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		return c.NoContent(http.StatusCreated)
	})
	e := echo.New()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			req := httptest.NewRequest(http.MethodPost, "/api/posts", nil)
			rec := httptest.NewRecorder()
			if err := h(e.NewContext(req, rec)); err != nil || rec.Code != http.StatusCreated {
				t.Errorf("err=%v code=%d", err, rec.Code)
			}
		})
	}
	wg.Wait()
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency %d, want 2", peak.Load())
	}
}

func TestPostCapTimeoutAndBypass(t *testing.T) {
	p := newPostCap(1, 30*time.Millisecond, 0, func(*echo.Context) bool { return true })
	release := p.acquire(t.Context()) // 枠を埋める
	defer release()
	ran := 0
	h := p.middleware(func(c *echo.Context) error { ran++; return nil })
	e := echo.New()
	start := time.Now()
	_ = h(e.NewContext(httptest.NewRequest(http.MethodPost, "/api/posts", nil), httptest.NewRecorder()))
	if el := time.Since(start); ran != 1 || el < 30*time.Millisecond {
		t.Fatalf("timeout path ran=%d el=%v", ran, el)
	}
	start = time.Now()
	_ = h(e.NewContext(httptest.NewRequest(http.MethodGet, "/api/posts", nil), httptest.NewRecorder()))
	_ = h(e.NewContext(httptest.NewRequest(http.MethodPost, "/api/friend-requests", nil), httptest.NewRecorder()))
	if el := time.Since(start); ran != 3 || el > 20*time.Millisecond {
		t.Fatalf("non-target requests must pass through (ran=%d el=%v)", ran, el)
	}
	// lag > 0: push の往復が lag 以下なら絞らない
	pl := newPostCap(1, time.Second, 100*time.Millisecond, func(*echo.Context) bool { return true })
	r2 := pl.acquire(t.Context())
	defer r2()
	memPushLastRTT.Store(int64(50 * time.Millisecond))
	t.Cleanup(func() { memPushLastRTT.Store(0) })
	start = time.Now()
	_ = pl.middleware(func(c *echo.Context) error { return nil })(e.NewContext(httptest.NewRequest(http.MethodPost, "/api/posts", nil), httptest.NewRecorder()))
	if el := time.Since(start); el > 20*time.Millisecond {
		t.Fatalf("low push lag must bypass the cap (%v)", el)
	}
}
