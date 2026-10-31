package isureal

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

// Differential test against a real database (read-only):
//
//	ISUREAL_TEST_PG_DSN='postgres://isureal:isureal@127.0.0.1:15433/isureal?sslmode=disable' TZ=UTC go test -run MemoriesParallel .
func TestMemoriesParallelMatchesSequential(t *testing.T) {
	dsn := os.Getenv("ISUREAL_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("ISUREAL_TEST_PG_DSN not set")
	}
	db := sqlx.MustOpen("pgx", dsn)
	defer db.Close()
	ctx := context.Background()
	var cases []struct {
		UserID string    `db:"author_id"`
		Date   time.Time `db:"local_date"`
	}
	// authors with many posts / interactions first, then a random spread
	if err := db.SelectContext(ctx, &cases, `
  (SELECT p.author_id, max(w.local_date) AS local_date FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   GROUP BY p.author_id ORDER BY count(*) DESC LIMIT 40)
  UNION ALL
  (SELECT p.author_id, w.local_date FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   ORDER BY md5(p.id) LIMIT 120)`); err != nil {
		t.Fatal(err)
	}
	compare := func(name string, userID string, start, end time.Time, beforeAt *time.Time, beforeID string) getMemoriesResponse {
		memoriesParallel = false
		a, errA := listMemories(ctx, db, userID, start, end, beforeAt, beforeID)
		memoriesParallel = true
		b, errB := listMemories(ctx, db, userID, start, end, beforeAt, beforeID)
		memoriesParallel = false
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if (errA == nil) != (errB == nil) || string(ja) != string(jb) {
			t.Fatalf("%s user=%s %v..%v: errs %v / %v\nseq=%s\npar=%s", name, userID, start, end, errA, errB, ja, jb)
		}
		return a
	}
	pages, interactions, hasMore := 0, 0, 0
	for _, cs := range cases {
		month := time.Date(cs.Date.Year(), cs.Date.Month(), 1, 0, 0, 0, 0, time.UTC)
		day := time.Date(cs.Date.Year(), cs.Date.Month(), cs.Date.Day(), 0, 0, 0, 0, time.UTC)
		r := compare("month", cs.UserID, month, month.AddDate(0, 1, 0), nil, "")
		compare("date", cs.UserID, day, day.AddDate(0, 0, 1), nil, "")
		// wide range: more than 50 posts -> hasMore and the LIMIT 50 subquery
		if w := compare("all", cs.UserID, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), nil, ""); w.HasMore {
			hasMore++
		}
		pages++
		for _, it := range r.Items {
			interactions += len(it.RealMojis) + len(it.Comments)
		}
		if len(r.Items) > 1 {
			last := r.Items[len(r.Items)/2]
			at := last.PostedAt
			compare("page2", cs.UserID, month, month.AddDate(0, 1, 0), &at, last.ID)
			pages++
		}
	}
	at := time.Now()
	compare("bad-pagination", cases[0].UserID, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), &at, "")
	t.Logf("%d cases, %d pages, %d hasMore, %d interactions compared", len(cases), pages, hasMore, interactions)
}
