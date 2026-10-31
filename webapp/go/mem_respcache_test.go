package isureal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

func TestMemRespCacheInvalidation(t *testing.T) {
	defer func(v bool) { memRespCacheOn = v }(memRespCacheOn)
	memRespCacheOn = true
	d := newMemData()
	k := memoriesRespKey("v", time.Unix(0, 0), time.Unix(100, 0), nil, "")
	tags := []string{"ov", "uv", "pP1", "wW1", "uR"}
	fill := func() { f := d.rc.begin(); d.rc.put(k, f, []byte("x"), tags) }
	has := func() bool { return d.rc.get(k) != nil }
	row := func(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
	apply := func(tbl, op string, v any) {
		if err := d.applyChange(memChange{Tbl: tbl, Op: op, Row: row(v)}); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name string
		hit  bool
		do   func()
	}{
		{"comment on shown post", false, func() {
			apply("comments", "I", memCommentRow{ID: "c", PostID: "P1", AuthorID: "x", CreatedAt: time.Now()})
		}},
		{"comment delete", false, func() { apply("comments", "D", memCommentRow{ID: "c", PostID: "P1"}) }},
		{"realmoji on shown post", false, func() {
			apply("realmojis", "I", memRealMojiRow{ID: "r", PostID: "P1", ReactorID: "x", CreatedAt: time.Now()})
		}},
		{"realmoji without post id", false, func() { apply("realmojis", "D", memRealMojiRow{ID: "r"}) }},
		{"viewer posts", false, func() {
			apply("posts", "I", memPostRow{ID: "P9", AuthorID: "v", PostingWindowID: "W9", PostedAt: time.Now()})
		}},
		{"viewer user row", false, func() { apply("users", "U", memUserRow{ID: "v", Status: "active"}) }},
		{"reactor user row", false, func() { apply("users", "U", memUserRow{ID: "R", Status: "deleted"}) }},
		{"window of shown post", false, func() { apply("posting_windows", "U", memWindowRow{ID: "W1", Region: "asia", LocalDate: "2026-01-01"}) }},
		{"other post comment", true, func() {
			apply("comments", "I", memCommentRow{ID: "c2", PostID: "P2", AuthorID: "x", CreatedAt: time.Now()})
		}},
		{"other user posts", true, func() {
			apply("posts", "I", memPostRow{ID: "P8", AuthorID: "o", PostingWindowID: "W9", PostedAt: time.Now()})
		}},
		{"other window", true, func() { apply("posting_windows", "U", memWindowRow{ID: "W2", Region: "asia", LocalDate: "2026-01-02"}) }},
		{"follow", true, func() { apply("follows", "I", memFollowRow{ID: "f", FollowerID: "a", FolloweeID: "b"}) }},
	}
	for _, cs := range cases {
		fill()
		if !has() {
			t.Fatalf("%s: not stored", cs.name)
		}
		cs.do()
		if has() != cs.hit {
			t.Fatalf("%s: hit=%v want %v", cs.name, has(), cs.hit)
		}
		// fill race: a change applied between begin and put vetoes the put
		d.rc.clearAll()
		f := d.rc.begin()
		cs.do()
		d.rc.put(k, f, []byte("x"), tags)
		if has() != cs.hit {
			t.Fatalf("%s (race): hit=%v want %v", cs.name, has(), cs.hit)
		}
		d.rc.clearAll()
	}
	// deleted -> active clears everything
	apply("users", "I", memUserRow{ID: "z", Status: "deleted"})
	fill()
	apply("users", "U", memUserRow{ID: "z", Status: "active"})
	if has() {
		t.Fatal("reactivation kept entry")
	}
	// a clear between begin and put vetoes
	f := d.rc.begin()
	d.rc.clearAll()
	d.rc.put(k, f, []byte("x"), tags)
	if has() {
		t.Fatal("put after clear stored")
	}
	// a new memData has an empty cache
	fill()
	if newMemData().rc.get(k) != nil {
		t.Fatal("cache shared across memData")
	}
}

// Differential test against a real database (read-only): every response
// through the cache (miss, then hit) is byte-identical to the plain DB path.
//
//	ISUREAL_TEST_PG_DSN='postgres://isureal:isureal@127.0.0.1:15433/isureal?sslmode=disable' TZ=UTC go test -run MemRespCache .
func TestMemRespCacheMatchesDB(t *testing.T) {
	dsn := os.Getenv("ISUREAL_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("ISUREAL_TEST_PG_DSN not set")
	}
	defer func(v bool) { memRespCacheOn = v }(memRespCacheOn)
	db := sqlx.MustOpen("pgx", dsn)
	defer db.Close()
	ctx := context.Background()
	var cases []struct {
		UserID string    `db:"author_id"`
		Date   time.Time `db:"local_date"`
	}
	if err := db.SelectContext(ctx, &cases, `
  (SELECT p.author_id, max(w.local_date) AS local_date FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   GROUP BY p.author_id ORDER BY count(*) DESC LIMIT 20)
  UNION ALL
  (SELECT p.author_id, w.local_date FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   ORDER BY md5(p.id) LIMIT 60)`); err != nil {
		t.Fatal(err)
	}
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	h := &HandlerContext{DB: db, reads: &readRouter{primary: db}, Config: &Config{}, Mem: m}
	e := echo.New()
	run := func(handler echo.HandlerFunc, target string, pv echo.PathValues, userID string) (int, http.Header, []byte) {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		if pv != nil {
			c.SetPathValues(pv)
		}
		c.Set("session", &Session{UserID: userID})
		req.Header.Set("Authorization", "Bearer "+userID)
		if err := handler(c); err != nil {
			return -1, nil, []byte(err.Error())
		}
		return rec.Code, rec.Header(), rec.Body.Bytes()
	}
	n, hits := 0, 0
	compare := func(name string, handler echo.HandlerFunc, target string, pv echo.PathValues, userID string) {
		memRespCacheOn = false
		c0, h0, b0 := run(handler, target, pv, userID)
		memRespCacheOn = true
		before := memRespHits.Load()
		for i := 0; i < 2; i++ { // miss (fill), then hit
			c1, h1, b1 := run(handler, target, pv, userID)
			if c0 != c1 || !bytes.Equal(b0, b1) || h0.Get("Content-Type") != h1.Get("Content-Type") ||
				h0.Get("Content-Disposition") != h1.Get("Content-Disposition") || h0.Get("Cache-Control") != h1.Get("Cache-Control") {
				t.Fatalf("%s %s user=%s pass %d: code %d/%d hdr %v/%v\nplain=%.300s\ncache=%.300s", name, target, userID, i, c0, c1, h0, h1, b0, b1)
			}
		}
		hits += int(memRespHits.Load() - before)
		n++
	}
	all := func() {
		for _, cs := range cases {
			month := cs.Date.Format("2006-01")
			day := cs.Date.Format(time.DateOnly)
			compare("month", getMemoriesHandler(h), "/api/memories?month="+month, nil, cs.UserID)
			compare("date", getMemoriesByDateHandler(h), "/api/memories/"+day, echo.PathValues{{Name: "date", Value: day}}, cs.UserID)
			var r getMemoriesResponse
			_, _, b := run(getMemoriesHandler(h), "/api/memories?month="+month, nil, cs.UserID)
			_ = json.Unmarshal(b, &r)
			if len(r.Items) > 1 {
				last := r.Items[len(r.Items)/2]
				q := url.Values{"month": {month}, "beforePostedAt": {last.PostedAt.Format(time.RFC3339Nano)}, "beforeId": {last.ID}}
				compare("page2", getMemoriesHandler(h), "/api/memories?"+q.Encode(), nil, cs.UserID)
			}
			compare("export", exportPostsHandler(h), "/api/me/export", nil, cs.UserID)
		}
	}
	all()
	compare("bad-pagination", getMemoriesHandler(h), "/api/memories?month=2026-01&beforeId=x", nil, cases[0].UserID)
	t.Logf("local: %d comparisons, %d cache hits", n, hits)

	// misses computed by an origin app (asia2 role: no mem model)
	defer func(o, s, rg string) { memRespOrigin, forwardSecret, memRespOriginRegion = o, s, rg }(memRespOrigin, forwardSecret, memRespOriginRegion)
	// europe1 / america1 as the cache host: the origin must see the request's region (fwd headers)
	memRespOriginRegion = "europe"
	ho := &HandlerContext{DB: db, reads: &readRouter{primary: db}, Config: &Config{}}
	originHits, badRegion := 0, 0
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originHits++
		if r.Header.Get(forwardedRegionHeader) != "europe" || r.Header.Get(forwardedSecretHeader) != "test-secret" {
			badRegion++
		}
		c := e.NewContext(r, w)
		c.Set("session", &Session{UserID: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")})
		var err error
		switch {
		case r.URL.Path == "/api/me/export":
			err = exportPostsHandler(ho)(c)
		case strings.HasPrefix(r.URL.Path, "/api/memories/"):
			c.SetPathValues(echo.PathValues{{Name: "date", Value: strings.TrimPrefix(r.URL.Path, "/api/memories/")}})
			err = getMemoriesByDateHandler(ho)(c)
		default:
			err = getMemoriesHandler(ho)(c)
		}
		if err != nil {
			w.WriteHeader(599)
		}
	}))
	defer origin.Close()
	memRespOrigin, forwardSecret = strings.TrimPrefix(origin.URL, "http://"), "test-secret"
	m.d = newMemData()
	n, hits = 0, 0
	all()
	t.Logf("origin: %d comparisons, %d cache hits, %d origin requests", n, hits, originHits)
	if originHits == 0 {
		t.Fatal("origin not used")
	}
	if badRegion != 0 {
		t.Fatalf("%d origin requests without the europe fwd region headers", badRegion)
	}
	if hits == 0 {
		t.Fatal("no cache hits")
	}
}
