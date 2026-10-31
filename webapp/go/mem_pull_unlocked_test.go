package isureal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ISUREAL_MEM_PULL_UNLOCKED: 欠番の pull の間も、他の push (欠番そのもの) は止まらずに適用され、
// pull の結果は重複分を捨てて連番どおりに適用される。
func TestMemReceiverPullUnlocked(t *testing.T) {
	for _, unlocked := range []bool{false, true} {
		oldU, oldW := memPullUnlocked, memReorderWait
		memPullUnlocked, memReorderWait = unlocked, time.Millisecond
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(150 * time.Millisecond)
			_ = json.NewEncoder(w).Encode([]memBatch{{Seq: 1}, {Seq: 2}})
		}))
		m := &memStore{d: newMemData(), useLease: true}
		m.ready.Store(true)
		r := &memReceiver{m: m, pumpAddr: strings.TrimPrefix(srv.URL, "http://"), epoch: "e1"}
		r.seqCond = sync.NewCond(&r.mu)
		r.rsCond = sync.NewCond(&r.rsMu)
		now := time.Now().UnixNano()
		done := make(chan bool)
		go func() {
			done <- r.push(memPushRequest{Epoch: "e1", Seq: 3, Batches: []memBatch{{Seq: 3}}, SentAt: now})
		}()
		time.Sleep(30 * time.Millisecond) // seq 3 の push は pull 中
		start := time.Now()
		if !r.push(memPushRequest{Epoch: "e1", Seq: 1, Batches: []memBatch{{Seq: 1}}, SentAt: now}) {
			t.Fatal("seq 1 rejected")
		}
		blocked := time.Since(start)
		if !<-done {
			t.Fatalf("unlocked=%v: seq 3 rejected", unlocked)
		}
		if r.seq != 3 {
			t.Fatalf("unlocked=%v: seq=%d", unlocked, r.seq)
		}
		if unlocked && blocked > 60*time.Millisecond {
			t.Fatalf("seq 1 push blocked %v behind the pull", blocked)
		}
		if !unlocked && blocked < 60*time.Millisecond {
			t.Fatalf("locked mode: expected seq 1 to wait for the pull, waited %v", blocked)
		}
		srv.Close()
		memPullUnlocked, memReorderWait = oldU, oldW
	}
}

// pull の間に無効化が割り込んだら、取り寄せた分は適用しない。
func TestMemReceiverPullUnlockedInvalidated(t *testing.T) {
	oldU, oldW := memPullUnlocked, memReorderWait
	memPullUnlocked, memReorderWait = true, time.Millisecond
	defer func() { memPullUnlocked, memReorderWait = oldU, oldW }()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_ = json.NewEncoder(w).Encode([]memBatch{{Seq: 1}})
	}))
	defer srv.Close()
	m := &memStore{d: newMemData(), useLease: true}
	m.ready.Store(true)
	r := &memReceiver{m: m, pumpAddr: strings.TrimPrefix(srv.URL, "http://"), epoch: "e1"}
	r.seqCond = sync.NewCond(&r.mu)
	r.rsCond = sync.NewCond(&r.rsMu)
	done := make(chan bool)
	go func() {
		done <- r.push(memPushRequest{Epoch: "e1", Seq: 2, Batches: []memBatch{{Seq: 2}}, SentAt: time.Now().UnixNano()})
	}()
	time.Sleep(30 * time.Millisecond)
	r.mu.Lock()
	r.gen++ // invalidate の代わり (resync を起動しない)
	r.epoch = ""
	r.mu.Unlock()
	if <-done {
		t.Fatal("push succeeded after invalidation")
	}
	if r.seq != 0 {
		t.Fatalf("applied after invalidation: seq=%d", r.seq)
	}
}
