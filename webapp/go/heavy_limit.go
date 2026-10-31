package isureal

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ISUREAL_HEAVY_LIMIT=N (0 = off): at most N heavy DB reads (export, the DB
// path of Memories and of user search) run at once in this process, so the
// DB connections and CPU stay free for posts and feeds. A request waits for a
// slot at most ISUREAL_HEAVY_LIMIT_MAX_WAIT_MS (default 3000), then runs
// anyway (never rejected); a cancelled request stops waiting and runs on to
// its own error path.

var (
	heavyLimitN       = envIntOrDefault("ISUREAL_HEAVY_LIMIT", 0)
	heavyLimitMaxWait = time.Duration(envIntOrDefault("ISUREAL_HEAVY_LIMIT_MAX_WAIT_MS", 3000)) * time.Millisecond
	heavySlots        chan struct{}
	heavyInit         sync.Once
	heavyWaited       atomic.Int64
	heavyTimeouts     atomic.Int64
	heavyTotal        atomic.Int64
)

func init() {
	if heavyLimitN > 0 {
		heavySlots = make(chan struct{}, heavyLimitN)
	}
}

func heavyStatsLoop() {
	for range time.Tick(10 * time.Second) {
		t := heavyTotal.Swap(0)
		if t == 0 {
			continue
		}
		slog.Info("heavy-limit", "total", t, "waited", heavyWaited.Swap(0), "timeouts", heavyTimeouts.Swap(0), "inflight", len(heavySlots))
	}
}

// heavyAcquire takes a slot (or not, after the max wait) and returns the release func.
func heavyAcquire(ctx context.Context) func() {
	if heavySlots == nil {
		return func() {}
	}
	heavyInit.Do(func() {
		slog.Info("heavy-limit: on", "n", heavyLimitN, "max_wait", heavyLimitMaxWait)
		go heavyStatsLoop()
	})
	heavyTotal.Add(1)
	select {
	case heavySlots <- struct{}{}:
		return heavyRelease
	default:
	}
	heavyWaited.Add(1)
	t := time.NewTimer(heavyLimitMaxWait)
	defer t.Stop()
	select {
	case heavySlots <- struct{}{}:
		return heavyRelease
	case <-t.C:
		heavyTimeouts.Add(1)
	case <-ctx.Done():
	}
	return func() {}
}

func heavyRelease() { <-heavySlots }
