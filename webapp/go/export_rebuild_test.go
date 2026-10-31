package isureal

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

// ISUREAL_EXPORT_REBUILD: after the signed-URL bucket rolls over, the cached
// skeleton is re-assembled with the new signatures. The bytes must equal what
// the DB path builds in the new bucket.
//
//	ISUREAL_TEST_PG_DSN='postgres://isureal:isureal@127.0.0.1:15433/isureal?sslmode=disable' TZ=UTC go test -run ExportRebuild .
func TestExportRebuildMatchesDB(t *testing.T) {
	dsn := os.Getenv("ISUREAL_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("ISUREAL_TEST_PG_DSN not set")
	}
	defer func(v, r bool) { memRespCacheOn, exportRebuildOn = v, r }(memRespCacheOn, exportRebuildOn)
	defer func(f func() time.Time) { imageURLNow = f }(imageURLNow)
	db := sqlx.MustOpen("pgx", dsn)
	defer db.Close()
	ctx := context.Background()
	var users []string
	if err := db.SelectContext(ctx, &users, `
  (SELECT author_id FROM posts WHERE deleted_at IS NULL GROUP BY author_id ORDER BY count(*) DESC LIMIT 10)
  UNION ALL
  (SELECT author_id FROM posts ORDER BY md5(id) LIMIT 30)
  UNION ALL
  (SELECT author_id FROM posts p WHERE deleted_at IS NULL GROUP BY author_id
   HAVING bool_and(EXISTS (SELECT 1 FROM image_objects i WHERE i.id = p.image_id)) LIMIT 30)`); err != nil {
		t.Fatal(err)
	}
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	h := &HandlerContext{DB: db, reads: &readRouter{primary: db}, Config: &Config{}, Mem: m}
	e := echo.New()
	run := func(userID string) (int, http.Header, []byte) {
		req := httptest.NewRequest(http.MethodGet, "/api/me/export", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.Set("session", &Session{UserID: userID})
		if err := exportPostsHandler(h)(c); err != nil {
			return -1, nil, []byte(err.Error())
		}
		return rec.Code, rec.Header(), rec.Body.Bytes()
	}
	base := time.Now()
	rebuilds := 0
	for _, u := range users {
		imageURLNow = func() time.Time { return base }
		memRespCacheOn, exportRebuildOn = true, true
		m.d = newMemData()
		if code, _, body := run(u); code != 200 { // fill (bucket b0)
			t.Fatalf("%s fill: %d %s", u, code, body)
		}
		imageURLNow = func() time.Time { return base.Add(11 * time.Minute) } // next bucket
		before := exportRebuilds.Load()
		code, hdr, got := run(u)
		if exportRebuilds.Load() > before {
			rebuilds++
		}
		memRespCacheOn, exportRebuildOn = false, false
		wcode, whdr, want := run(u)
		if code != wcode || !bytes.Equal(got, want) {
			t.Fatalf("%s: rebuilt export differs (code %d vs %d, len %d vs %d)", u, code, wcode, len(got), len(want))
		}
		for _, k := range []string{"Content-Type", "Content-Disposition", "Cache-Control"} {
			if hdr.Get(k) != whdr.Get(k) {
				t.Fatalf("%s: header %s %q vs %q", u, k, hdr.Get(k), whdr.Get(k))
			}
		}
	}
	if rebuilds == 0 {
		t.Fatal("no export was rebuilt")
	}
	t.Logf("%d users, %d rebuilt", len(users), rebuilds)
}

// Without a DB: a stale-bucket entry is not served as-is, its skeleton is
// re-assembled and stored for the new bucket, and an invalidation drops it.
func TestExportRebuildCache(t *testing.T) {
	defer func(v bool) { memRespCacheOn = v }(memRespCacheOn)
	defer func(f func() time.Time) { imageURLNow = f }(imageURLNow)
	memRespCacheOn = true
	base := time.Now()
	imageURLNow = func() time.Time { return base }
	user := User{ID: "u1", Username: "a", DisplayName: "A", Status: "active", SecretKeyHash: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"}
	if err := user.setImageKey(); err != nil {
		t.Fatal(err)
	}
	sk := &exportSkel{userID: "u1", author: user,
		posts: []Post{{ID: "p1", AuthorID: "u1", PostingWindowID: "w1", ImageID: "img1", PostedAt: base.Add(-time.Hour), OnTimeDeadline: base}},
		objs:  []*imageObject{{Owner: "u1", MediaType: "image/jpeg", Data: []byte("jpegbytes")}}}
	b0, tags, err := assembleExport(sk)
	if err != nil || tags == nil {
		t.Fatal(err)
	}
	d := newMemData()
	k := memRespKey{kind: memRespExport, user: "u1"}
	d.rc.putAux(k, d.rc.begin(), b0, tags, sk)
	if d.rc.get(k) == nil {
		t.Fatal("same bucket should hit")
	}
	imageURLNow = func() time.Time { return base.Add(11 * time.Minute) }
	if d.rc.get(k) != nil {
		t.Fatal("stale bucket must not be served")
	}
	f := d.rc.begin()
	got, _ := d.rc.staleAux(k).(*exportSkel)
	if got != sk {
		t.Fatal("skeleton not kept")
	}
	b1, tags1, _ := assembleExport(got)
	if bytes.Equal(b0, b1) {
		t.Fatal("new bucket must re-sign")
	}
	d.rc.putAux(k, f, b1, tags1, got)
	if !bytes.Equal(d.rc.get(k), b1) {
		t.Fatal("rebuilt entry should hit")
	}
	d.rc.invalidate("ou1")
	if d.rc.get(k) != nil || d.rc.staleAux(k) != nil {
		t.Fatal("invalidation must drop the skeleton too")
	}
}
