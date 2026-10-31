package isureal

import (
	"sync"
	"testing"
	"time"
)

// 到達不能な peer は即失敗するが、lease (lastGrantAt + memLease) が切れるまでは書き込みを保留する。
func TestMemWaitAckedDeadPeerWaitsLease(t *testing.T) {
	ps := &memPeers{epoch: "e1", peers: []*memPeer{{addr: "127.0.0.1:1", lastGrantAt: time.Now()}}}
	ps.cond = sync.NewCond(&ps.mu)
	start := time.Now()
	ps.publish(nil)
	ps.waitAcked()
	el := time.Since(start)
	if el < memLease || el > memLease+time.Second {
		t.Fatalf("waitAcked took %v, want ~%v", el, memLease)
	}
	// lease 切れ後は待たない
	start = time.Now()
	ps.publish(nil)
	ps.waitAcked()
	if el := time.Since(start); el > 200*time.Millisecond {
		t.Fatalf("second waitAcked took %v", el)
	}
}

// 並列 push の到着順が入れ替わっても、peer は先行分を少し待って seq 順に適用する (pull しない)。
func TestMemReceiverReorder(t *testing.T) {
	m := &memStore{d: newMemData(), useLease: true}
	m.ready.Store(true)
	r := &memReceiver{m: m, pumpAddr: "127.0.0.1:1", epoch: "e1"}
	r.seqCond = sync.NewCond(&r.mu)
	r.rsCond = sync.NewCond(&r.rsMu)
	now := time.Now().UnixNano()
	done := make(chan bool)
	go func() {
		done <- r.push(memPushRequest{Epoch: "e1", Seq: 2, Batches: []memBatch{{Seq: 2}}, SentAt: now})
	}()
	time.Sleep(10 * time.Millisecond)
	if !r.push(memPushRequest{Epoch: "e1", Seq: 1, Batches: []memBatch{{Seq: 1}}, SentAt: now}) {
		t.Fatal("seq 1 rejected")
	}
	if !<-done {
		t.Fatal("seq 2 rejected")
	}
	if r.seq != 2 || !m.on() {
		t.Fatalf("seq=%d on=%v", r.seq, m.on())
	}
	// probe は lease を延ばさない
	m.leaseUntil.Store(0)
	if !r.push(memPushRequest{Epoch: "e1", Seq: 2, SentAt: time.Now().UnixNano(), Probe: true}) || m.on() {
		t.Fatal("probe extended lease")
	}
}
