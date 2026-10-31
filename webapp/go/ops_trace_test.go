package isureal

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

func TestOpsTraceObservesWithoutChangingResponses(t *testing.T) {
	h := &HandlerContext{Config: &Config{Region: "asia"}}
	run := func(trace bool) []string {
		e := echo.New()
		if trace {
			e.Pre(opsTraceMiddleware(h)) // 本番と同じ Pre
		}
		e.POST("/api/auth/login", func(c *echo.Context) error {
			return c.JSON(http.StatusOK, map[string]string{"token": "secret-token-1"})
		})
		e.POST("/api/posts", func(c *echo.Context) error {
			opsAdd(c.Request().Context(), opsSlot, 3*time.Millisecond)
			opsAdd(c.Request().Context(), opsPersist, 2*time.Millisecond)
			opsAdd(c.Request().Context(), opsRepl, time.Millisecond)
			return c.JSON(http.StatusCreated, map[string]string{"id": "p1"})
		})
		e.GET("/api/memories", func(c *echo.Context) error { return c.JSON(http.StatusOK, map[string]any{"items": []int{}}) })
		e.GET("/api/posts/:id", func(c *echo.Context) error { return c.String(http.StatusOK, "post") })
		e.POST("/api/posts/:id/comments", func(c *echo.Context) error { return c.String(http.StatusCreated, "c") })
		e.POST("/api/posts/:id/realmojis", func(c *echo.Context) error {
			return NewAPIError(http.StatusForbidden, "forbidden")
		})
		e.HTTPErrorHandler = APIErrorHandler
		var out []string
		do := func(method, path string) {
			req := httptest.NewRequest(method, path, strings.NewReader(""))
			if path != "/api/auth/login" {
				req.Header.Set("Authorization", "Bearer secret-token-1")
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			b, _ := io.ReadAll(rec.Body)
			out = append(out, rec.Result().Status+" "+rec.Header().Get("Content-Type")+" "+string(b))
		}
		do("POST", "/api/auth/login")
		do("GET", "/api/memories?month=2001-01")
		do("POST", "/api/posts")
		do("GET", "/api/posts/p1")
		do("POST", "/api/posts/p1/comments")
		do("POST", "/api/posts/p1/realmojis")
		return out
	}
	defer func(v any) {
		if v != nil {
			opsCur.Store(v)
		}
	}(opsCur.Load())
	opsCur.Store("2026-11")
	plain := run(false)
	traced := run(true)
	for i := range plain {
		if plain[i] != traced[i] {
			t.Fatalf("response %d changed: %q vs %q", i, plain[i], traced[i])
		}
	}
	ops.mu.Lock()
	defer ops.mu.Unlock()
	cid := opsCID("secret-token-1")
	if len(cid) != 8 || ops.users[cid] == nil {
		t.Fatalf("cid %q not tracked", cid)
	}
	for k := range ops.users {
		if strings.Contains(k, "secret") {
			t.Fatal("raw token stored")
		}
	}
	if len(ops.loginMem) != 1 || ops.views != 1 || ops.comments != 1 || ops.realmojis != 0 {
		t.Fatalf("loginMem=%d views=%d comments=%d realmojis=%d", len(ops.loginMem), ops.views, ops.comments, ops.realmojis)
	}
	if len(ops.loginDur) != 1 || len(ops.loginMemS) != 1 || ops.loginMemS[0] < ops.loginMem[0] {
		t.Fatalf("login start/response not both recorded: dur=%d memS=%d", len(ops.loginDur), len(ops.loginMemS))
	}
	ws := ops.waits["asia"]
	if ws == nil || len(ws[0]) != 1 || ws[0][0] != 3*time.Millisecond || ws[1][0] != 2*time.Millisecond || ws[2][0] != time.Millisecond {
		t.Fatalf("waits %+v", ws)
	}
}

func TestOpsPastMonthUsesAppPeriod(t *testing.T) {
	r := func(u string) *http.Request { return httptest.NewRequest("GET", u, nil) }
	// 実時間 (10 月) ではなく、アプリの最新の投稿期間の月 (11 月) で判定する
	cases := []struct {
		u, cur string
		want   bool
	}{
		{"/api/memories?month=2026-10", "2026-11", true},
		{"/api/memories?month=2026-11", "2026-11", false},
		{"/api/memories/2026-10-31", "2026-11", true},
		{"/api/memories/2026-11-01", "2026-11", false},
		{"/api/memories?month=2026-10", "", false},
	}
	for _, cs := range cases {
		if got := opsPastMonth(r(cs.u), cs.cur); got != cs.want {
			t.Errorf("%s cur=%q: %v", cs.u, cs.cur, got)
		}
	}
}

func TestOpsKeyFromForwardSecret(t *testing.T) {
	if forwardSecret == "" {
		t.Skip("ISUREAL_FWD_SECRET unset: random key")
	}
	if a, b := opsCID("t"), opsCID("t"); a != b {
		t.Fatal("cid not stable")
	}
}
