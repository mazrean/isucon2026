package isureal

import (
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

// ISUREAL_POST_TIMING=1 (既定 0): POST /api/posts の 1 本の内訳を段階ごとに 10 秒ごとに slog ("post timing")。
// 段階: jpeg (checkJPEG: ローカル枠 + asia2/eu/am への委譲の待ち)、localimg (ASIA_LOCALIMG の書き込み + group sync の待ち)、
// localimg_sync (そのうち sync の待ち)、apply (store の適用役の列 + 排他)、pgflush (AWAIT_PG の書き戻しのレーン)、
// ack (peer の ack)。応答・挙動は変えない。
var postTimingOn = os.Getenv("ISUREAL_POST_TIMING") == "1"

var postTiming struct {
	once sync.Once
	mu   sync.Mutex
	d    map[string][]time.Duration
}

func postTimingAdd(phase string, d time.Duration) {
	if !postTimingOn {
		return
	}
	postTiming.once.Do(func() {
		postTiming.d = map[string][]time.Duration{}
		go postTimingLoop()
	})
	postTiming.mu.Lock()
	if s := postTiming.d[phase]; len(s) < 1<<15 {
		postTiming.d[phase] = append(s, d)
	}
	postTiming.mu.Unlock()
}

func postTimingLoop() {
	for range time.Tick(10 * time.Second) {
		postTiming.mu.Lock()
		cur := postTiming.d
		postTiming.d = map[string][]time.Duration{}
		postTiming.mu.Unlock()
		if len(cur) == 0 {
			continue
		}
		keys := make([]string, 0, len(cur))
		for k := range cur {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		attrs := []any{}
		for _, k := range keys {
			p50, p90, _ := memPercentiles(cur[k])
			attrs = append(attrs, k+"_n", len(cur[k]), k+"_p50", p50, k+"_p90", p90)
		}
		slog.Info("post timing", attrs...)
	}
}
