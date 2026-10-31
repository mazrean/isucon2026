package isureal

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// ISUREAL_USER_LOCK_SPLIT=0/1 の両方で通る。退会と、投稿・login・友達申請の競合。
func TestUserLockDeletionRace(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	in, loc := postBatchInput()

	// A: 退会が先に行を取る → 後から来た操作はすべて拒否され、退会後に何も入らない。
	for round := 0; round < 5; round++ {
		x, username, secret := sessionBatchSeed(t, db)
		other := postBatchSeed(t, db, "asia")
		if _, _, err := loginBatch(ctx, db, username, secret); err != nil {
			t.Fatal(err)
		}
		locked := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := db.MustBegin()
			defer tx.Rollback()
			tx.MustExec(deleteAccountLockSQL(), x)
			close(locked)
			time.Sleep(30 * time.Millisecond)
			tx.MustExec(`UPDATE users SET status='deleted', deleted_at=clock_timestamp(), updated_at=clock_timestamp() WHERE id=$1`, x)
			tx.MustExec(`UPDATE login_sessions SET active=false, invalidated_at=clock_timestamp(), reason='account_deletion' WHERE user_id=$1 AND active=true`, x)
			if err := tx.Commit(); err != nil {
				t.Error(err)
			}
		}()
		<-locked
		ops := []func() error{
			func() error {
				_, _, inserted, err := createPostBatch(ctx, db, x, uuid.NewString(), uuid.NewString(), in, loc)
				if inserted || !errors.Is(err, sql.ErrNoRows) {
					return errors.New("post accepted for deleted user")
				}
				return nil
			},
			func() error {
				tx := db.MustBegin()
				defer tx.Rollback()
				if _, err := getUserForUpdate(ctx, tx, x); !errors.Is(err, sql.ErrNoRows) {
					return errors.New("getUserForUpdate saw deleted user as active")
				}
				return nil
			},
			func() error {
				if _, _, err := loginBatch(ctx, db, username, secret); !errors.Is(err, errLoginInvalid) {
					return errors.New("loginBatch accepted deleted user")
				}
				return nil
			},
			func() error {
				if _, err := legacyLogin(ctx, db, username, secret); !errors.Is(err, errLoginInvalid) {
					return errors.New("legacy login accepted deleted user")
				}
				return nil
			},
			func() error {
				if _, err := createFriendRequestBatch(ctx, db, other, x); friendBatchCode(err) != http.StatusNotFound {
					return errors.New("friend request to deleted user accepted")
				}
				return nil
			},
			func() error {
				if _, err := createFriendRequestBatch(ctx, db, x, other); friendBatchCode(err) != http.StatusNotFound {
					return errors.New("friend request from deleted user accepted")
				}
				return nil
			},
			func() error {
				tx, err := beginRelationshipTx(ctx, db, other, x)
				if err == nil {
					tx.Rollback()
					return errors.New("beginRelationshipTx locked deleted user")
				}
				return nil
			},
		}
		for _, op := range ops {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := op(); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		var posts, active, reqs int
		db.Get(&posts, `SELECT count(*) FROM posts WHERE author_id=$1`, x)
		db.Get(&active, `SELECT count(*) FROM login_sessions WHERE user_id=$1 AND active`, x)
		db.Get(&reqs, `SELECT count(*) FROM friend_requests WHERE requester_id=$1 OR recipient_id=$1`, x)
		if posts != 0 || active != 0 || reqs != 0 {
			t.Fatalf("round %d: after deletion posts=%d active sessions=%d requests=%d", round, posts, active, reqs)
		}
	}

	// B: 操作が先に行を取る → 退会はその tx の COMMIT を待つ (login の新しいセッションも退会で無効化される)。
	for round := 0; round < 5; round++ {
		x, _, _ := sessionBatchSeed(t, db)
		other := postBatchSeed(t, db, "asia")
		holders := []func(release <-chan struct{}) error{
			func(release <-chan struct{}) error { // login (行ロック → セッション)
				tx := db.MustBegin()
				defer tx.Rollback()
				var u User
				if err := tx.GetContext(ctx, &u, lockUserRowSQL(`SELECT `+strings.Join(userColumns, ", ")+` FROM users WHERE id = $1`, userAdvSession), x); err != nil {
					return err
				}
				<-release
				if _, err := createLoginSessionTx(ctx, tx, x, true); err != nil {
					return err
				}
				return tx.Commit()
			},
			func(release <-chan struct{}) error { // 友達の組
				tx, err := beginRelationshipTx(ctx, db, other, x)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				<-release
				return tx.Commit()
			},
		}
		for i, hold := range holders {
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() { done <- hold(release) }()
			time.Sleep(20 * time.Millisecond) // holder が行を取るまで
			delDone := make(chan error, 1)
			go func() { delDone <- deleteAccountDB(ctx, db, x) }()
			select {
			case err := <-delDone:
				t.Fatalf("round %d holder %d: deletion did not wait for the holder (err=%v)", round, i, err)
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatalf("holder %d: %v", i, err)
			}
			if err := <-delDone; err != nil {
				t.Fatalf("deletion: %v", err)
			}
			var active int
			db.Get(&active, `SELECT count(*) FROM login_sessions WHERE user_id=$1 AND active`, x)
			if active != 0 {
				t.Fatalf("round %d holder %d: %d active sessions after deletion", round, i, active)
			}
			x, _, _ = sessionBatchSeed(t, db)
		}
	}
}

// 多数の利用者で login・投稿・友達申請・退会を同時に流す。デッドロック・想定外のエラーが無く、不変条件が保たれる。
func TestUserLockMixedStress(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	in, loc := postBatchInput()
	const nUsers = 12
	type cred struct{ id, name, secret string }
	users := make([]cred, nUsers)
	for i := range users {
		id, name, secret := sessionBatchSeed(t, db)
		users[i] = cred{id, name, secret}
	}
	unexpected := func(err error) bool {
		if err == nil || errors.Is(err, sql.ErrNoRows) || errors.Is(err, errLoginInvalid) {
			return false
		}
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			return false
		}
		return true
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	report := func(err error) {
		if unexpected(err) {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}
	for g := 0; g < 48; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				u := users[(g*7+k)%nUsers]
				v := users[(g*3+k*5+1)%nUsers]
				switch (g + k) % 4 {
				case 0:
					_, _, _, err := createPostBatch(ctx, db, u.id, uuid.NewString(), uuid.NewString(), in, loc)
					report(err)
				case 1:
					if k%2 == 0 {
						_, _, err := loginBatch(ctx, db, u.name, u.secret)
						report(err)
					} else {
						_, err := legacyLogin(ctx, db, u.name, u.secret)
						report(err)
					}
				case 2:
					if u.id != v.id {
						_, err := createFriendRequestBatch(ctx, db, u.id, v.id)
						report(err)
					}
				case 3:
					if g%16 == 0 && k == 10 {
						report(deleteAccountDB(ctx, db, u.id))
					}
				}
			}
		}(g)
	}
	wg.Wait()
	for _, err := range errs {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			t.Errorf("pg error %s: %v", pgErr.Code, err)
		} else {
			t.Error(err)
		}
	}
	for _, u := range users {
		var status string
		db.Get(&status, `SELECT status FROM users WHERE id=$1`, u.id)
		var active, posts, pend int
		db.Get(&active, `SELECT count(*) FROM login_sessions WHERE user_id=$1 AND active`, u.id)
		db.Get(&posts, `SELECT count(*) FROM posts WHERE author_id=$1 AND deleted_at IS NULL`, u.id)
		if posts > 1 {
			t.Errorf("user %s: %d active posts", u.id, posts)
		}
		if status == "deleted" && active != 0 || status == "active" && active > 1 {
			t.Errorf("user %s (%s): %d active sessions", u.id, status, active)
		}
		db.Get(&pend, `SELECT count(*) FROM (SELECT least(requester_id,recipient_id), greatest(requester_id,recipient_id) FROM friend_requests
  WHERE status='pending' AND (requester_id=$1 OR recipient_id=$1) GROUP BY 1,2 HAVING count(*)>1) d`, u.id)
		if pend != 0 {
			t.Errorf("user %s: duplicate pending pairs %d", u.id, pend)
		}
	}
}
