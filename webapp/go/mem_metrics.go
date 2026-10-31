package isureal

import (
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// 書き込みバリアの待ち時間と、lease 切れで ack を待たずに進んだ回数を 10 秒ごとにログに出す。
var memBarrierStats struct {
	mu         sync.Mutex
	samples    []time.Duration
	leaseSkips int64
}

// memBarrierObserve はテスト用のフック (nil なら何もしない)。
var memBarrierObserve func(time.Duration)

func memRecordBarrier(d time.Duration) {
	memBarrierStats.mu.Lock()
	if len(memBarrierStats.samples) < 1<<16 {
		memBarrierStats.samples = append(memBarrierStats.samples, d)
	}
	memBarrierStats.mu.Unlock()
	if f := memBarrierObserve; f != nil {
		f(d)
	}
}

func memRecordLeaseSkip() {
	memBarrierStats.mu.Lock()
	memBarrierStats.leaseSkips++
	memBarrierStats.mu.Unlock()
	memLeaseSkipsTotal.Add(1)
}

var memLeaseSkipsTotal atomic.Int64

func memDurPercentiles(s []time.Duration) (p50, p90, max time.Duration) {
	if len(s) == 0 {
		return 0, 0, 0
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2], s[len(s)*9/10], s[len(s)-1]
}

func memBarrierStatsLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		memBarrierStats.mu.Lock()
		s := memBarrierStats.samples
		skips := memBarrierStats.leaseSkips
		memBarrierStats.samples = nil
		memBarrierStats.leaseSkips = 0
		memBarrierStats.mu.Unlock()
		if len(s) == 0 && skips == 0 {
			continue
		}
		p50, p90, max := memDurPercentiles(s)
		slog.Info("mem: barrier", "n", len(s), "p50", p50, "p90", p90, "max", max, "lease_skips", skips)
	}
}
