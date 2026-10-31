package isureal

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func chunkReqChange(seq int64, xid, id, from, to, status string) memChange {
	b, _ := json.Marshal(memFriendRequestRow{ID: id, RequesterID: from, RecipientID: to, Status: status, UpdatedAt: time.Unix(seq, 0)})
	return memChange{Seq: seq, Tbl: "friend_requests", Op: "U", Row: b, Xid: xid}
}

func chunkFriendChange(seq int64, xid, id, low, high string) memChange {
	b, _ := json.Marshal(memFriendshipRow{ID: id, UserLowID: low, UserHighID: high, UpdatedAt: time.Unix(seq, 0)})
	return memChange{Seq: seq, Tbl: "friendships", Op: "I", Row: b, Xid: xid}
}

func chunkTestStore(t *testing.T, pairs int) *memStore {
	m := &memStore{d: newMemData()}
	var init []memChange
	for i := range pairs {
		init = append(init, chunkReqChange(int64(i+1), "", fmt.Sprintf("r%d", i), fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i), "pending"))
	}
	if err := m.applyBatch(init); err != nil {
		t.Fatal(err)
	}
	return m
}

// 承認の tx (request を accepted に + friendship を INSERT) を、tx 同士が seq で交互に並ぶ 1 バッチにする。
func chunkAcceptBatch(pairs int, xidPerChange bool) []memChange {
	var out []memChange
	seq := int64(1000)
	for i := range pairs { // 前半: 各 tx の request の更新
		seq++
		x := fmt.Sprintf("x%d", i)
		if xidPerChange {
			x = fmt.Sprintf("bad%d", seq)
		}
		out = append(out, chunkReqChange(seq, x, fmt.Sprintf("r%d", i), fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i), "accepted"))
	}
	for i := range pairs { // 後半: 各 tx の friendship
		seq++
		x := fmt.Sprintf("x%d", i)
		if xidPerChange {
			x = fmt.Sprintf("bad%d", seq)
		}
		out = append(out, chunkFriendChange(seq, x, fmt.Sprintf("f%d", i), fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i)))
	}
	return out
}

// 読みは、どの組でも「申請が pending で友達でない」か「申請が消えて友達」のどちらかだけを見る (tx の途中を見ない)。
func runChunkReaders(m *memStore, pairs int, batch []memChange, n int) (violations int64, err error) {
	var vio atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				m.mu.RLock()
				for i := range pairs {
					a := m.d.users[fmt.Sprintf("a%d", i)]
					_, pending := a.outgoing[fmt.Sprintf("r%d", i)]
					_, friend := a.friends[fmt.Sprintf("b%d", i)]
					if pending == friend {
						vio.Add(1)
					}
				}
				m.mu.RUnlock()
			}
		})
	}
	time.Sleep(2 * time.Millisecond)
	err = m.applyBatchChunked(batch, n)
	time.Sleep(2 * time.Millisecond)
	close(stop)
	wg.Wait()
	return vio.Load(), err
}

func TestMemApplyChunkKeepsTxAtomic(t *testing.T) {
	const pairs = 400
	for range 5 {
		m := chunkTestStore(t, pairs)
		v, err := runChunkReaders(m, pairs, chunkAcceptBatch(pairs, false), 1)
		if err != nil || v != 0 {
			t.Fatalf("err=%v: readers saw %d half-applied tx", err, v)
		}
		for i := range pairs { // 最終状態
			a := m.d.users[fmt.Sprintf("a%d", i)]
			if _, ok := a.friends[fmt.Sprintf("b%d", i)]; !ok || len(a.outgoing) != 0 {
				t.Fatalf("pair %d not accepted", i)
			}
		}
	}
}

// 負のテスト: tx の境界を誤る (変更ごとに別の xid) と、上の検査は途中の状態を検出する (感度の確認)。
func TestMemApplyChunkDetectsSplitTx(t *testing.T) {
	const pairs = 400
	var total int64
	for range 5 {
		m := chunkTestStore(t, pairs)
		v, err := runChunkReaders(m, pairs, chunkAcceptBatch(pairs, true), 1)
		if err != nil {
			t.Fatal(err)
		}
		total += v
	}
	if total == 0 {
		t.Fatal("split tx produced no visible intermediate state: the check is not sensitive")
	}
}

// 最終状態は seq 順の一括適用と同じ。同じ行を 2 つの tx が書く (後の tx の最初の seq の方が小さい) 場合も、後の tx が勝つ。
func TestMemApplyChunkFinalStateMatches(t *testing.T) {
	batch := []memChange{
		chunkReqChange(1, "B", "other", "c", "d", "pending"), // B が先に始まる
		chunkReqChange(2, "A", "r", "a", "b", "pending"),     // A が r を書いて COMMIT
		chunkReqChange(3, "B", "r", "a", "b", "accepted"),    // B は A の COMMIT を待って r を書く
		chunkFriendChange(4, "B", "f", "a", "b"),
		chunkReqChange(5, "C", "s", "e", "f", "pending"),
	}
	whole := &memStore{d: newMemData()}
	if err := whole.applyBatch(batch); err != nil {
		t.Fatal(err)
	}
	chunked := &memStore{d: newMemData()}
	if err := chunked.applyBatchChunked(batch, 1); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		w, c := whole.d.users[id], chunked.d.users[id]
		if len(w.outgoing) != len(c.outgoing) || len(w.incoming) != len(c.incoming) || len(w.friends) != len(c.friends) {
			t.Fatalf("user %s differs: whole out=%d in=%d fr=%d, chunked out=%d in=%d fr=%d", id,
				len(w.outgoing), len(w.incoming), len(w.friends), len(c.outgoing), len(c.incoming), len(c.friends))
		}
	}
	if _, ok := chunked.d.users["a"].outgoing["r"]; ok {
		t.Fatal("A's write applied after B's (r should be accepted)")
	}
	// xid の無い変更が混じると刻まない (従来どおり)
	if g := memTxGroups(append(batch, memChange{Seq: 6})); g != nil {
		t.Fatal("groups for a batch without xids")
	}
}
