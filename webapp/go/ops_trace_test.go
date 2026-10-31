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
			e.Use(opsTraceMiddleware(h))
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
	ws := ops.waits["asia"]
	if ws == nil || len(ws[0]) != 1 || ws[0][0] != 3*time.Millisecond || ws[1][0] != 2*time.Millisecond || ws[2][0] != time.Millisecond {
		t.Fatalf("waits %+v", ws)
	}
}
