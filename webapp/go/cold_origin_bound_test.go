package isureal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// 冷たい読みのノードが使えないときは、試し走りも転送もせず 1 回だけ走る (COLD_ORIGIN 無しと同じ)。
func TestColdOriginGateClosedRunsOnce(t *testing.T) {
	var hits atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer origin.Close()
	withColdOrigin(t, strings.TrimPrefix(origin.URL, "http://"))
	coldOriginGate = func() bool { return false }
	coldPumpSeqFn = func() int64 { return 1 }
	var runs atomic.Int64
	rec := coldServe(t, coldMiddleware(true), coldTestHandler(&runs, true), "/api/memories?month=2026-10")
	if runs.Load() != 1 || hits.Load() != 0 || !strings.Contains(rec.Body.String(), "local-pg") {
		t.Fatalf("closed gate: runs=%d origin hits=%d body=%s", runs.Load(), hits.Load(), rec.Body.String())
	}
}

// 転送先が遅いときは coldForwardMax で諦めて自分の経路で答える。
func TestColdOriginForwardIsBounded(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer origin.Close()
	withColdOrigin(t, strings.TrimPrefix(origin.URL, "http://"))
	coldPumpSeqFn = func() int64 { return 1 }
	var runs atomic.Int64
	start := time.Now()
	rec := coldServe(t, coldMiddleware(false), coldTestHandler(&runs, true), "/api/memories?month=2026-10")
	el := time.Since(start)
	if el > coldForwardMax+time.Second || runs.Load() != 2 || !strings.Contains(rec.Body.String(), "local-pg") {
		t.Fatalf("slow origin: %v runs=%d body=%s", el, runs.Load(), rec.Body.String())
	}
}
