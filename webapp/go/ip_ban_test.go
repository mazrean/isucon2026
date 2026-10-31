package isureal

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

// ISUREAL_IPBAN_ASYNC=1 answers from the snapshot without waiting for the reload, keeps it
// fresh in the background, and gives the same answers as the synchronous path.
func TestIPBanAsync(t *testing.T) {
	dsn := os.Getenv("ISUREAL_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("ISUREAL_TEST_PG_DSN not set")
	}
	defer func(v bool) { ipBanAsync = v }(ipBanAsync)
	db := sqlx.MustOpen("pgx", dsn)
	defer db.Close()
	ctx := context.Background()

	var sync IPBanList
	ipBanAsync = false
	want, err := sync.Banned(ctx, db, "203.0.113.7")
	if err != nil {
		t.Fatal(err)
	}

	var l IPBanList
	ipBanAsync = true
	got, err := l.Banned(ctx, db, "203.0.113.7") // first call: no snapshot -> synchronous load
	if err != nil || got != want {
		t.Fatalf("first: got %v %v want %v", got, err, want)
	}
	first := l.snapshot.Load()
	// a ban in the snapshot is answered without any DB access
	l.snapshot.Store(&ipBanSnapshot{loadedAt: time.Now(), bans: map[string]time.Time{"198.51.100.1": time.Now().Add(time.Hour)}})
	if b, err := l.Banned(ctx, nil, "198.51.100.1"); err != nil || !b {
		t.Fatalf("snapshot ban: %v %v", b, err)
	}
	// the background loop replaces the snapshot within a few intervals
	deadline := time.Now().Add(3 * time.Second)
	for {
		s := l.snapshot.Load()
		if s != first && s.loadedAt.After(first.loadedAt) && s.bans["198.51.100.1"].IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background reload did not refresh the snapshot")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// stale snapshot (background stopped) -> synchronous path
	l.snapshot.Store(&ipBanSnapshot{loadedAt: time.Now().Add(-time.Hour), bans: map[string]time.Time{"198.51.100.2": time.Now().Add(time.Hour)}})
	if b, err := l.Banned(ctx, db, "198.51.100.2"); err != nil || b {
		t.Fatalf("stale snapshot should be reloaded: %v %v", b, err)
	}
}
