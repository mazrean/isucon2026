package isureal

import (
	"bytes"
	"crypto/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Concurrent keeps share syncs (fewer syncs than keeps) and every kept image
// is on disk and readable by a fresh store when keep returns.
func TestAsiaLocalImgGroupCommit(t *testing.T) {
	setupAsiaLocalImgTest(t)
	good := testJPEG(t, 64, 48)
	b0, m0 := asiaSyncer.batches.Load(), asiaSyncer.members.Load()
	const n = 64
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := newAsiaLocalImageID()
			if !asiaLocalImgKeep(t.Context(), id, "owner-"+strconv.Itoa(i), good) {
				t.Error("not kept")
				return
			}
			fresh := &localImgStore{dir: asiaStore.dir}
			if obj, _ := fresh.read(id); obj == nil || obj.Owner != "owner-"+strconv.Itoa(i) || !bytes.Equal(obj.Data, good) {
				t.Error("not on disk after keep")
			}
			ids[i] = id
		}()
	}
	wg.Wait()
	b, m := asiaSyncer.batches.Load()-b0, asiaSyncer.members.Load()-m0
	t.Logf("keeps=%d syncs=%d members=%d", n, b, m)
	if m != n || b < 1 || b >= n {
		t.Fatalf("syncs=%d members=%d for %d keeps", b, m, n)
	}
	// the per-file path still works (A/B switch)
	asiaLocalImgPerFileSync = true
	t.Cleanup(func() { asiaLocalImgPerFileSync = false })
	id := newAsiaLocalImageID()
	if !asiaLocalImgKeep(t.Context(), id, "o", good) || localImgOwn(id) == nil {
		t.Fatal("per-file keep")
	}
}

// ASIA_LOCALIMG_BENCH=1 go test -run TestAsiaLocalImgSyncBench -v .
// GP-like burst: W concurrent writers, 360KB each; per-file fsync vs group commit.
func TestAsiaLocalImgSyncBench(t *testing.T) {
	if os.Getenv("ASIA_LOCALIMG_BENCH") == "" {
		t.Skip("ASIA_LOCALIMG_BENCH=1")
	}
	setupAsiaLocalImgTest(t)
	if d := os.Getenv("ASIA_LOCALIMG_BENCH_DIR"); d != "" {
		asiaStore.dir, _ = os.MkdirTemp(d, "bench-")
		t.Cleanup(func() { os.RemoveAll(asiaStore.dir) })
	}
	img := make([]byte, 360<<10)
	rand.Read(img)
	writers, total := 64, 850
	if v, _ := strconv.Atoi(os.Getenv("ASIA_LOCALIMG_BENCH_W")); v > 0 {
		writers = v
	}
	run := func(perFile bool) {
		asiaLocalImgPerFileSync = perFile
		defer func() { asiaLocalImgPerFileSync = false }()
		b0 := asiaSyncer.batches.Load()
		lat := make([]time.Duration, total)
		next := make(chan int, total)
		for i := range total {
			next <- i
		}
		close(next)
		start := time.Now()
		var wg sync.WaitGroup
		for range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range next {
					t0 := time.Now()
					if !asiaLocalImgKeep(t.Context(), newAsiaLocalImageID(), "o", img) {
						t.Error("keep failed")
					}
					lat[i] = time.Since(t0)
				}
			}()
		}
		wg.Wait()
		el := time.Since(start)
		sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
		t.Logf("perFile=%v writers=%d n=%d: %.0f rps p50=%v p99=%v max=%v syncs=%d",
			perFile, writers, total, float64(total)/el.Seconds(),
			lat[total/2].Round(time.Microsecond), lat[total*99/100].Round(time.Microsecond), lat[total-1].Round(time.Microsecond),
			asiaSyncer.batches.Load()-b0)
		asiaLocalImgPurge()
	}
	for range 2 {
		run(true)
		run(false)
	}
}
