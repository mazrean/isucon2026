package isureal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// 書き込むので、使い捨ての DB を指定する (001/010/011 を流しただけの空の DB でよい):
//
//	ISUREAL_TEST_PG_RW_DSN='postgres://isureal:isureal@127.0.0.1:15433/isureal_pb?sslmode=disable' go test -run PostBatch .
func postBatchTestDB(t *testing.T) *sqlx.DB {
	dsn := os.Getenv("ISUREAL_TEST_PG_RW_DSN")
	if dsn == "" {
		t.Skip("ISUREAL_TEST_PG_RW_DSN not set")
	}
	db := sqlx.MustOpen("pgx", dsn)
	db.SetMaxOpenConns(64)
	t.Cleanup(func() { db.Close() })
	if err := ensureMemSchema(db); err != nil {
		t.Fatal(err)
	}
	return db
}

func postBatchSeed(t *testing.T, db *sqlx.DB, region string) (userID string) {
	userID = uuid.NewString()
	db.MustExec(`INSERT INTO users (id, username, secret_key_hash, display_name, region) VALUES ($1, $2, '40e8ca6be96000a310586c2bd165d7de02f1aded50972a5f5e942dbf93566091', 'disp', $3)`, userID, "u"+userID[:8], region)
	var n int
	if err := db.Get(&n, `SELECT count(*) FROM posting_windows WHERE region = $1 AND ended_at IS NULL`, region); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		now := time.Now().UTC()
		db.MustExec(`INSERT INTO posting_windows (id, region, local_date, starts_at, on_time_deadline) VALUES ($1, $2, $3, $4, $5)`,
			uuid.NewString(), region, now.Format("2006-01-02"), now.Add(-time.Minute), now.Add(time.Minute))
	}
	return userID
}

func postBatchInput() (createPostRequest, PostLocation) {
	lat, lng, loc := 35.681236, 139.767125, "Tokyo"
	in := createPostRequest{Caption: "hello", Visibility: "friends", LocationKind: "exact", Latitude: &lat, Longitude: &lng, Locality: &loc}
	return in, PostLocation{Kind: in.LocationKind, Latitude: in.Latitude, Longitude: in.Longitude, Locality: in.Locality}
}

// 同じユーザーの並行投稿で、入るのはちょうど 1 件。
func TestPostBatchConcurrentSingle(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	for round := 0; round < 5; round++ {
		userID := postBatchSeed(t, db, "asia")
		in, loc := postBatchInput()
		const n = 32
		var wg sync.WaitGroup
		var mu sync.Mutex
		ok, rejected := 0, 0
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, _, inserted, err := createPostBatch(ctx, db, userID, uuid.NewString(), uuid.NewString(), in, loc)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					t.Error(err)
					return
				}
				if inserted {
					ok++
				} else {
					rejected++
				}
			}()
		}
		close(start)
		wg.Wait()
		var count int
		if err := db.Get(&count, `SELECT count(*) FROM posts WHERE author_id = $1`, userID); err != nil {
			t.Fatal(err)
		}
		if ok != 1 || rejected != n-1 || count != 1 {
			t.Fatalf("round %d: ok=%d rejected=%d rows=%d", round, ok, rejected, count)
		}
	}
}

// 並行の投稿と削除・アカウント削除が混ざっても、件数の上限 (有効 1・削除 2) と「削除済みユーザーは投稿できない」を守る。
func TestPostBatchWithDeletes(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	userID := postBatchSeed(t, db, "asia")
	in, loc := postBatchInput()
	deleteOne := func() {
		// deletePostHandler と同じ手順 (行ロック → 削除回数 → UPDATE)
		tx := db.MustBegin()
		defer tx.Rollback()
		var id string
		tx.MustExec(`SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE`, userID)
		if err := tx.Get(&id, `SELECT id FROM posts WHERE author_id = $1 AND deleted_at IS NULL`, userID); err != nil {
			return
		}
		var deletions int
		_ = tx.Get(&deletions, `SELECT count(*) FROM posts WHERE author_id = $1 AND deleted_at IS NOT NULL`, userID)
		if deletions >= 2 {
			return
		}
		tx.MustExec(`UPDATE posts SET deleted_at = clock_timestamp() WHERE id = $1`, id)
		_ = tx.Commit()
	}
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, _, _, err := createPostBatch(ctx, db, userID, uuid.NewString(), uuid.NewString(), in, loc); err != nil {
				t.Error(err)
			}
		}()
		go func() { defer wg.Done(); deleteOne() }()
	}
	wg.Wait()
	var c struct {
		Active  int `db:"active"`
		Deleted int `db:"deleted"`
	}
	if err := db.Get(&c, `SELECT count(*) FILTER (WHERE deleted_at IS NULL) AS active, count(*) FILTER (WHERE deleted_at IS NOT NULL) AS deleted FROM posts WHERE author_id = $1`, userID); err != nil {
		t.Fatal(err)
	}
	if c.Active > 1 || c.Deleted > 2 {
		t.Fatalf("active=%d deleted=%d", c.Active, c.Deleted)
	}

	// アカウント削除と並行: 削除後に入った投稿が無いこと
	u2 := postBatchSeed(t, db, "asia")
	var wg2 sync.WaitGroup
	wg2.Add(2)
	var delAt time.Time
	go func() {
		defer wg2.Done()
		tx := db.MustBegin()
		tx.MustExec(`SELECT id FROM users WHERE id=$1 AND status='active' FOR NO KEY UPDATE`, u2)
		time.Sleep(20 * time.Millisecond)
		tx.MustExec(`UPDATE users SET status='deleted', deleted_at=clock_timestamp() WHERE id=$1`, u2)
		_ = tx.Commit()
		delAt = time.Now()
	}()
	go func() {
		defer wg2.Done()
		time.Sleep(5 * time.Millisecond)
		_, _, inserted, err := createPostBatch(ctx, db, u2, uuid.NewString(), uuid.NewString(), in, loc)
		if inserted || !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("post by deleted user: inserted=%v err=%v", inserted, err)
		}
	}()
	wg2.Wait()
	_ = delAt
}

// batch の応答は従来の getPost から作った応答と同じバイト列。
func TestPostBatchResponseMatchesGetPost(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	userID := postBatchSeed(t, db, "asia")
	in, loc := postBatchInput()
	user, post, inserted, err := createPostBatch(ctx, db, userID, uuid.NewString(), uuid.NewString(), in, loc)
	if err != nil || !inserted {
		t.Fatal(inserted, err)
	}
	legacy, err := getPost(ctx, db, post.ID)
	if err != nil {
		t.Fatal(err)
	}
	legacyUser, err := getUser(ctx, db, userID)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(newPostResponse(post, user))
	b, _ := json.Marshal(newPostResponse(legacy, legacyUser))
	if string(a) != string(b) {
		t.Fatalf("batch:  %s\nlegacy: %s", a, b)
	}
	var n int
	if err := db.Get(&n, `SELECT count(*) FROM mem_changelog WHERE tbl = 'posts'`); err != nil {
		t.Logf("mem_changelog check skipped: %v", err)
	} else if n == 0 {
		t.Fatal("mem_changelog trigger did not fire")
	}
	// 2 回目は 409 (枠なし)
	if _, _, inserted, err := createPostBatch(ctx, db, userID, uuid.NewString(), uuid.NewString(), in, loc); err != nil || inserted {
		t.Fatal(inserted, err)
	}
}
