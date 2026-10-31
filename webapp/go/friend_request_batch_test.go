package isureal

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func friendBatchCode(err error) int {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status
	}
	return 0
}

// 同じ組の申請を両側から並行に投げても、申請 1 つ + 承認 1 つ、友達は 1 組だけになる。
func TestFriendBatchConcurrent(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	for round := 0; round < 5; round++ {
		a, b := postBatchSeed(t, db, "asia"), postBatchSeed(t, db, "asia")
		var wg sync.WaitGroup
		var mu sync.Mutex
		created := map[string]int{}
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				actor, rcpt := a, b
				if i%2 == 1 {
					actor, rcpt = b, a
				}
				resp, err := createFriendRequestBatch(ctx, db, actor, rcpt)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if friendBatchCode(err) != http.StatusConflict {
						t.Error(err)
					}
					return
				}
				created[resp.Status]++
			}(i)
		}
		wg.Wait()
		var f, pending, accepted int
		db.Get(&f, `SELECT count(*) FROM friendships WHERE user_low_id=$1 AND user_high_id=$2`, min(a, b), max(a, b))
		db.Get(&pending, `SELECT count(*) FROM friend_requests WHERE status='pending' AND requester_id IN ($1,$2)`, a, b)
		db.Get(&accepted, `SELECT count(*) FROM friend_requests WHERE status='accepted' AND requester_id IN ($1,$2)`, a, b)
		if created["pending"] != 1 || created["accepted"] != 1 || f != 1 || pending != 0 || accepted != 1 {
			t.Fatalf("round %d: created=%v friendships=%d pending=%d accepted=%d", round, created, f, pending, accepted)
		}
	}
	// block → 409、相手が削除済み → 404、自分宛て → 400
	a, b := postBatchSeed(t, db, "asia"), postBatchSeed(t, db, "asia")
	db.MustExec(`INSERT INTO block_relations (id, blocker_id, blocked_id) VALUES ($1,$2,$3)`, uuid.NewString(), b, a)
	if _, err := createFriendRequestBatch(ctx, db, a, b); friendBatchCode(err) != http.StatusConflict {
		t.Fatal("block:", err)
	}
	c := postBatchSeed(t, db, "asia")
	db.MustExec(`UPDATE users SET status='deleted' WHERE id=$1`, c)
	if _, err := createFriendRequestBatch(ctx, db, a, c); friendBatchCode(err) != http.StatusNotFound {
		t.Fatal("deleted:", err)
	}
	if _, err := createFriendRequestBatch(ctx, db, a, a); friendBatchCode(err) != http.StatusBadRequest {
		t.Fatal("self:", err)
	}
	// 自分の保留中の申請があれば 409
	d := postBatchSeed(t, db, "asia")
	if _, err := createFriendRequestBatch(ctx, db, a, d); err != nil {
		t.Fatal(err)
	}
	if _, err := createFriendRequestBatch(ctx, db, a, d); friendBatchCode(err) != http.StatusConflict {
		t.Fatal("dup:", err)
	}
}

// accept / reject / cancel を同じ申請に並行に投げても、遷移するのは 1 つだけ。accept が勝ったときだけ友達になる。
func TestFriendTransitionBatchConcurrent(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	statuses := []string{"accepted", "rejected", "cancelled"}
	for round := 0; round < 6; round++ {
		a, b := postBatchSeed(t, db, "asia"), postBatchSeed(t, db, "asia")
		resp, err := createFriendRequestBatch(ctx, db, a, b)
		if err != nil {
			t.Fatal(err)
		}
		req, err := getFriendRequest(ctx, db, resp.RelationID)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		won := map[string]int{}
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func(st string) {
				defer wg.Done()
				r, err := transitionFriendRequestBatch(ctx, db, req, st)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if friendBatchCode(err) != http.StatusForbidden {
						t.Error(err)
					}
					return
				}
				won[r.Status]++
			}(statuses[(i+round)%3])
		}
		wg.Wait()
		total := won["accepted"] + won["rejected"] + won["cancelled"]
		var f int
		db.Get(&f, `SELECT count(*) FROM friendships WHERE user_low_id=$1 AND user_high_id=$2`, min(a, b), max(a, b))
		if total != 1 || f != won["accepted"] {
			t.Fatalf("round %d: won=%v friendships=%d", round, won, f)
		}
	}
	// block があれば accept は 409 で、申請は pending のまま
	a, b := postBatchSeed(t, db, "asia"), postBatchSeed(t, db, "asia")
	resp, err := createFriendRequestBatch(ctx, db, a, b)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := getFriendRequest(ctx, db, resp.RelationID)
	db.MustExec(`INSERT INTO block_relations (id, blocker_id, blocked_id) VALUES ($1,$2,$3)`, uuid.NewString(), a, b)
	if _, err := transitionFriendRequestBatch(ctx, db, req, "accepted"); friendBatchCode(err) != http.StatusConflict {
		t.Fatal("blocked accept:", err)
	}
	if got, _ := getFriendRequest(ctx, db, req.ID); got.Status != "pending" {
		t.Fatal("status changed:", got.Status)
	}
	if r, err := transitionFriendRequestBatch(ctx, db, req, "rejected"); err != nil || r.Status != "rejected" {
		t.Fatal("reject:", r, err)
	}
}
