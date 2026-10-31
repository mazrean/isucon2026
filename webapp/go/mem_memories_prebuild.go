package isureal

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ISUREAL_MEMORIES_PREBUILD=1 (needs ISUREAL_MEM=1 ISUREAL_MEMORIES_CACHE=1):
// GET /api/memories?month= answers for the periods the DB path serves
// (month start < hotFrom) are built ahead of the request into the response
// cache (mem_respcache.go), by the same code as a cache miss (begin ->
// listMemories -> encodeJSONLikeEcho -> put with memoriesTags). Bytes,
// tags and the fill race rule are therefore the miss path's; every change
// that drops a cached answer (posts / comments / RealMojis / users / windows
// in applyChange) drops a prebuilt one too.
//
// What is built:
//   - only right after each (re)load: the users of this host's region present
//     at load, all such months. A region with more than
//     ISUREAL_MEMORIES_PREBUILD_INIT_USERS (default 100; europe 23 / america 29
//     initial users) gets only ISUREAL_MEMORIES_PREBUILD_MAX_USERS of them
//     (default 0 = none; asia's ~4,950 users x 12 months is ~600MB).
//   - ISUREAL_MEMORIES_PREBUILD_TOUCH=1 (default 0; job19256: queue 11k and
//     asia1 stalled at GP): also a user's other months on that user's miss.
//
// Bounds: ISUREAL_MEMORIES_PREBUILD_MB (default 48) of built bytes per load
// (the cache's own ISUREAL_MEMORIES_CACHE_MB still applies), a 16k-job queue
// (full = drop), ISUREAL_MEMORIES_PREBUILD_PAR workers (default 2), jobs not
// started within ISUREAL_MEMORIES_PREBUILD_SECS (default 30) of the load are
// dropped, and workers pause while any posting window has started and not
// ended. Always computed by this host's own DB path (never the cache origin).

var (
	memPrebuildOn        = os.Getenv("ISUREAL_MEMORIES_PREBUILD") == "1"
	memPrebuildMaxBytes  = int64(envIntDefault("ISUREAL_MEMORIES_PREBUILD_MB", 48)) << 20
	memPrebuildInitUsers = envIntDefault("ISUREAL_MEMORIES_PREBUILD_INIT_USERS", 100)
	memPrebuildWorkers   = envIntDefault("ISUREAL_MEMORIES_PREBUILD_PAR", 2)
	memPrebuildMaxUsers  = envIntDefault("ISUREAL_MEMORIES_PREBUILD_MAX_USERS", 0)
	memPrebuildWindow    = time.Duration(envIntDefault("ISUREAL_MEMORIES_PREBUILD_SECS", 30)) * time.Second
	memPrebuildTouchOn   = os.Getenv("ISUREAL_MEMORIES_PREBUILD_TOUCH") == "1"

	memPrebuildJobs, memPrebuildPuts, memPrebuildSkip, memPrebuildDrop atomic.Int64 // 観測用

	memPrebuildMu  sync.Mutex
	memPrebuildH   *HandlerContext
	memPrebuildCh  chan memPrebuildJob
	memPrebuildRun sync.Once
)

type memPrebuildJob struct {
	d          *memData
	user       string
	start, end time.Time
}

// memPrebuildState lives in memData (dropped with it on reload).
type memPrebuildState struct {
	mu      sync.Mutex
	inited  bool
	initial map[string]struct{} // users present at load
	months  []time.Time         // month starts served by the DB path, newest first
	done    map[string]struct{} // users already queued
	until   time.Time           // jobs not started by then are dropped
	bytes   atomic.Int64
}

// memPrebuildSetup is called by setupMem once h.Mem is set.
func memPrebuildSetup(h *HandlerContext) {
	if !memPrebuildOn || !memRespCacheOn || h == nil || h.Mem == nil {
		return
	}
	memPrebuildMu.Lock()
	memPrebuildH = h
	memPrebuildMu.Unlock()
	memPrebuildRun.Do(func() {
		memPrebuildCh = make(chan memPrebuildJob, 16384)
		for i := 0; i < memPrebuildWorkers; i++ {
			go memPrebuildWorker()
		}
		go memPrebuildStatsLoop()
	})
}

func memPrebuildHandler() *HandlerContext {
	memPrebuildMu.Lock()
	defer memPrebuildMu.Unlock()
	return memPrebuildH
}

// memPrebuildMonths lists the month starts (UTC, as time.Parse("2006-01") gives)
// from the earliest window's month up to the last one starting before hotFrom.
func (d *memData) memPrebuildMonths() []time.Time {
	if !d.hotKnown || len(d.windows) == 0 {
		return nil
	}
	var first time.Time
	for _, w := range d.windows {
		if first.IsZero() || w.LocalDate.Before(first) {
			first = w.LocalDate
		}
	}
	hot := time.Date(d.hotFrom.Year(), d.hotFrom.Month(), d.hotFrom.Day(), 0, 0, 0, 0, time.UTC)
	var out []time.Time
	for s := time.Date(first.Year(), first.Month(), 1, 0, 0, 0, 0, time.UTC); s.Before(hot); s = s.AddDate(0, 1, 0) {
		out = append(out, s)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// memPrebuildLoaded is called right after m.d is replaced by a fresh load and m is ready.
func memPrebuildLoaded(m *memStore) {
	h := memPrebuildHandler()
	if h == nil || h.Mem != m {
		return
	}
	m.mu.RLock()
	d := m.d
	region := ""
	if h.Config != nil {
		region = h.Config.Region
	}
	var local []string
	initial := make(map[string]struct{}, len(d.users))
	for id, u := range d.users {
		initial[id] = struct{}{}
		if u.active() && u.Region == region {
			local = append(local, id)
		}
	}
	months := d.memPrebuildMonths()
	m.mu.RUnlock()

	pb := &d.pb
	pb.mu.Lock()
	if pb.inited {
		pb.mu.Unlock()
		return
	}
	pb.inited, pb.initial, pb.months, pb.done = true, initial, months, map[string]struct{}{}
	pb.until = time.Now().Add(memPrebuildWindow)
	pb.mu.Unlock()
	if len(local) > memPrebuildInitUsers {
		if len(local) > memPrebuildMaxUsers {
			local = local[:memPrebuildMaxUsers]
		}
		if len(local) == 0 {
			slog.Info("mem: memories prebuild: init skipped", "region", region, "months", len(months))
			return
		}
	}
	for _, id := range local {
		d.memPrebuildUser(id)
	}
	slog.Info("mem: memories prebuild: init queued", "region", region, "users", len(local), "months", len(months))
}

// memPrebuildUser queues every month of userID once per load (initial users only).
func (d *memData) memPrebuildUser(userID string) {
	pb := &d.pb
	if pb.bytes.Load() >= memPrebuildMaxBytes {
		return
	}
	pb.mu.Lock()
	if !pb.inited {
		pb.mu.Unlock()
		return
	}
	if _, ok := pb.initial[userID]; !ok {
		pb.mu.Unlock()
		return
	}
	if _, ok := pb.done[userID]; ok {
		pb.mu.Unlock()
		return
	}
	pb.done[userID] = struct{}{}
	months := pb.months
	pb.mu.Unlock()
	for _, s := range months {
		select {
		case memPrebuildCh <- memPrebuildJob{d: d, user: userID, start: s, end: s.AddDate(0, 1, 0)}:
			memPrebuildJobs.Add(1)
		default:
			memPrebuildDrop.Add(1)
		}
	}
}

// memPrebuildTouch: a Memories cache miss of userID (serveMemories).
func (m *memStore) memPrebuildTouch(userID string) {
	if !memPrebuildOn || !memPrebuildTouchOn || m == nil || memPrebuildCh == nil {
		return
	}
	m.mu.RLock()
	d := m.d
	m.mu.RUnlock()
	d.memPrebuildUser(userID)
}

func memPrebuildWorker() {
	for job := range memPrebuildCh {
		memPrebuildOne(job)
	}
}

func memPrebuildOne(job memPrebuildJob) {
	h := memPrebuildHandler()
	if h == nil || h.Mem == nil {
		return
	}
	m := h.Mem
	for {
		if time.Now().After(job.d.pb.until) {
			memPrebuildSkip.Add(1)
			return
		}
		m.mu.RLock()
		busy := m.d == job.d && job.d.windowStarted(time.Now())
		m.mu.RUnlock()
		if !busy {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	m.mu.RLock()
	cur, on := m.d, m.on()
	m.mu.RUnlock()
	if !on || cur != job.d || job.d.pb.bytes.Load() >= memPrebuildMaxBytes {
		memPrebuildSkip.Add(1)
		return
	}
	rc := &cur.rc
	key := memoriesRespKey(job.user, job.start, job.end, nil, "")
	if rc.has(key) {
		memPrebuildSkip.Add(1)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fill := rc.begin()
	response, err := listMemories(ctx, h.Reader(), job.user, job.start, job.end, nil, "")
	if err != nil {
		return
	}
	b, err := encodeJSONLikeEcho(response)
	if err != nil {
		return
	}
	rc.put(key, fill, b, memoriesTags(job.user, response))
	job.d.pb.bytes.Add(int64(len(b)))
	memPrebuildPuts.Add(1)
}

func memPrebuildStatsLoop() {
	var last int64
	for range time.Tick(10 * time.Second) {
		j := memPrebuildJobs.Load()
		p := memPrebuildPuts.Load()
		if j+p == last {
			continue
		}
		last = j + p
		slog.Info("mem: memories prebuild", "jobs", j, "puts", p, "skip", memPrebuildSkip.Load(), "drop", memPrebuildDrop.Load(), "queue", len(memPrebuildCh), "originDeadline", memOriginDeadlineHits.Load())
	}
}

// windowStarted: some posting window has started and not ended (call with m.mu held).
func (d *memData) windowStarted(now time.Time) bool {
	for _, w := range d.open {
		if !w.StartsAt.After(now) {
			return true
		}
	}
	return false
}
