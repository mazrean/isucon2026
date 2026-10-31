package isureal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

func TestMemPrebuildMonths(t *testing.T) {
	d := newMemData()
	d.windows["a"] = &memWindow{PostingWindow: PostingWindow{LocalDate: time.Date(2025, 10, 28, 0, 0, 0, 0, time.Local)}}
	d.windows["b"] = &memWindow{PostingWindow: PostingWindow{LocalDate: time.Date(2026, 10, 31, 0, 0, 0, 0, time.Local)}}
	d.hotFrom, d.hotKnown = time.Date(2026, 11, 1, 0, 0, 0, 0, time.Local), true
	ms := d.memPrebuildMonths()
	if len(ms) != 13 || ms[0].Format("2006-01") != "2026-10" || ms[12].Format("2006-01") != "2025-10" {
		t.Fatalf("months %v", ms)
	}
	want, _ := time.Parse("2006-01", "2026-10")
	if !ms[0].Equal(want) || ms[0].Location() != want.Location() {
		t.Fatalf("month start %v want %v", ms[0], want)
	}
	d.hotFrom = time.Date(2026, 10, 30, 0, 0, 0, 0, time.Local) // 2026-10 starts before hotFrom: DB path
	if ms := d.memPrebuildMonths(); len(ms) != 13 {
		t.Fatalf("partial hot month: %d", len(ms))
	}
}

// Differential test against a real database (read-only): prebuilt answers
// are byte-identical to the plain DB path, and are dropped by the change stream.
//
//	ISUREAL_TEST_PG_DSN='postgres://isureal:isureal@127.0.0.1:15433/isureal?sslmode=disable' TZ=UTC go test -run MemPrebuild .
func TestMemPrebuildMatchesDB(t *testing.T) {
	dsn := os.Getenv("ISUREAL_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("ISUREAL_TEST_PG_DSN not set")
	}
	defer func(a, b, c bool, n int) {
		memRespCacheOn, memPrebuildOn, memPrebuildTouchOn, memPrebuildInitUsers = a, b, c, n
	}(memRespCacheOn, memPrebuildOn, memPrebuildTouchOn, memPrebuildInitUsers)
	memRespCacheOn, memPrebuildOn, memPrebuildTouchOn, memPrebuildInitUsers = true, true, true, 100
	db := sqlx.MustOpen("pgx", dsn)
	defer db.Close()
	ctx := context.Background()
	start := time.Now()
	d, _, err := loadMemFromDB(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("load %v, hotFrom %v, months %d", time.Since(start), d.hotFrom, len(d.memPrebuildMonths()))
	// workers pause while a window is open: the test DB keeps the last run's open windows
	openW := d.open
	d.open = map[string]*memWindow{}
	m := &memStore{d: d}
	m.ready.Store(true)
	h := &HandlerContext{DB: db, reads: &readRouter{primary: db}, Config: &Config{Region: "europe"}, Mem: m}
	memPrebuildSetup(h)
	defer func() { memPrebuildMu.Lock(); memPrebuildH = nil; memPrebuildMu.Unlock() }()
	wait := func() {
		for i := 0; i < 600 && (len(memPrebuildCh) > 0 || memPrebuildJobs.Load() > memPrebuildPuts.Load()+memPrebuildSkip.Load()); i++ {
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond)
	}
	start = time.Now()
	memPrebuildLoaded(m)
	wait()
	t.Logf("init prebuild: jobs %d puts %d bytes %d in %v", memPrebuildJobs.Load(), memPrebuildPuts.Load(), d.pb.bytes.Load(), time.Since(start))
	if memPrebuildPuts.Load() == 0 {
		t.Fatal("nothing prebuilt")
	}

	e := echo.New()
	run := func(target, userID string) (int, []byte) {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.Set("session", &Session{UserID: userID})
		if err := getMemoriesHandler(h)(c); err != nil {
			return -1, []byte(err.Error())
		}
		return rec.Code, rec.Body.Bytes()
	}
	compareAll := func(users []string, wantHit bool) (n int, size int) {
		for _, u := range users {
			for _, s := range d.memPrebuildMonths() {
				target := "/api/memories?month=" + s.Format("2006-01")
				memRespCacheOn = false
				c0, b0 := run(target, u)
				memRespCacheOn = true
				hits, puts := memRespHits.Load(), memRespPuts.Load()
				c1, b1 := run(target, u)
				if c0 != c1 || !bytes.Equal(b0, b1) {
					t.Fatalf("%s user=%s: %d/%d\nplain=%.300s\ncache=%.300s", target, u, c0, c1, b0, b1)
				}
				if wantHit && (memRespHits.Load() != hits+1 || memRespPuts.Load() != puts) {
					t.Fatalf("%s user=%s: not served from prebuild", target, u)
				}
				n++
				size += len(b0)
			}
		}
		return
	}
	var eu []string
	for id, u := range d.users {
		if u.Region == "europe" && u.active() {
			eu = append(eu, id)
		}
	}
	n, size := compareAll(eu, true)
	t.Logf("europe: %d users, %d prebuilt answers identical, %d bytes", len(eu), n, size)

	// the change stream drops a prebuilt answer: a RealMoji on a shown post
	var shown getMemoriesResponse
	var key memRespKey
	for _, u := range eu {
		for _, s := range d.memPrebuildMonths() {
			_, b := run("/api/memories?month="+s.Format("2006-01"), u)
			_ = json.Unmarshal(b, &shown)
			if len(shown.Items) > 0 {
				key = memoriesRespKey(u, s, s.AddDate(0, 1, 0), nil, "")
				break
			}
		}
		if len(shown.Items) > 0 {
			break
		}
	}
	if !d.rc.has(key) {
		t.Fatal("no entry to invalidate")
	}
	row, _ := json.Marshal(memRealMojiRow{ID: "x", PostID: shown.Items[0].ID, ReactorID: "nobody", CreatedAt: time.Now()})
	m.mu.Lock()
	err = d.applyChange(memChange{Tbl: "realmojis", Op: "I", Row: row})
	m.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if d.rc.has(key) {
		t.Fatal("realmoji change kept the prebuilt entry")
	}

	// a miss of an initial asia user queues that user's other months
	var asia string
	for id, u := range d.users {
		if u.Region == "asia" && u.active() && len(u.posts) == 0 {
			asia = id
			break
		}
	}
	months := d.memPrebuildMonths()
	run("/api/memories?month="+months[len(months)/2].Format("2006-01"), asia)
	wait()
	n, size = compareAll([]string{asia}, true)
	t.Logf("asia touch: %d answers identical and prebuilt, %d bytes", n, size)
	// users not present at load are not queued
	if d.memPrebuildUser("not-initial"); len(memPrebuildCh) != 0 {
		t.Fatal("queued a non-initial user")
	}

	// an open window pauses the workers; past the time bound the job is dropped
	if len(openW) > 0 {
		m.mu.Lock()
		d.open = openW
		m.mu.Unlock()
		d.pb.until = time.Now().Add(500 * time.Millisecond)
		d.rc.clearAll()
		skip, puts := memPrebuildSkip.Load(), memPrebuildPuts.Load()
		memPrebuildCh <- memPrebuildJob{d: d, user: eu[0], start: months[0], end: months[0].AddDate(0, 1, 0)}
		memPrebuildJobs.Add(1)
		time.Sleep(1200 * time.Millisecond)
		if memPrebuildPuts.Load() != puts || memPrebuildSkip.Load() != skip+1 {
			t.Fatalf("open window: puts %d skip %d", memPrebuildPuts.Load()-puts, memPrebuildSkip.Load()-skip)
		}
	}

	// ISUREAL_MEMORIES_ORIGIN_DEADLINE_MS: a slow origin is abandoned and the miss computed locally
	defer func(o, s string, dl time.Duration) { memRespOrigin, forwardSecret, memOriginDeadline = o, s, dl }(memRespOrigin, forwardSecret, memOriginDeadline)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(3 * time.Second):
		case <-r.Context().Done():
		}
		w.Write([]byte("slow"))
	}))
	defer slow.Close()
	memRespOrigin, forwardSecret, memOriginDeadline = slow.Listener.Addr().String(), "test-secret", 100*time.Millisecond
	memPrebuildOn = false
	m.mu.Lock()
	m.d = newMemData()
	m.mu.Unlock()
	target := "/api/memories?month=" + months[0].Format("2006-01")
	memRespCacheOn = false
	c0, b0 := run(target, eu[0])
	memRespCacheOn = true
	before := memOriginDeadlineHits.Load()
	t0 := time.Now()
	c1, b1 := run(target, eu[0])
	if el := time.Since(t0); el > 2*time.Second {
		t.Fatalf("deadline not applied: %v", el)
	}
	if c0 != c1 || !bytes.Equal(b0, b1) || memOriginDeadlineHits.Load() != before+1 {
		t.Fatalf("deadline fallback: %d/%d hits %d\nplain=%.300s\nlocal=%.300s", c0, c1, memOriginDeadlineHits.Load()-before, b0, b1)
	}
	if c2, b2 := run(target, eu[0]); c2 != c0 || !bytes.Equal(b0, b2) {
		t.Fatal("fallback answer not cached identically")
	}
}
