package isureal

import (
	"sync"
	"testing"
	"time"
)

func ackOwnPeers(acked int64) (*memPeers, *memPeer) {
	p := &memPeer{addr: "127.0.0.1:1", lastGrantAt: time.Now(), acked: acked}
	ps := &memPeers{epoch: "e1", peers: []*memPeer{p}}
	ps.cond = sync.NewCond(&ps.mu)
	return ps, p
}

// 自分の seq まで ack 済みなら、後から publish された分 (ps.seq) を待たない。
func TestMemAckOwnDoesNotWaitLaterSeqs(t *testing.T) {
	ps, _ := ackOwnPeers(3)
	ps.seq = 10
	start := time.Now()
	ps.waitAckedFor(memAckTarget{epoch: "e1", seq: 3, ok: true})
	if el := time.Since(start); el > 50*time.Millisecond {
		t.Fatalf("own seq acked, but waited %v", el)
	}
}

// 自分の seq が未 ack なら待ち、ack で解放される。lease 切れでも解放 (peer は自発的に DB 経路)。
func TestMemAckOwnWaitsOwnSeq(t *testing.T) {
	ps, p := ackOwnPeers(2)
	ps.seq = 3
	go func() {
		time.Sleep(30 * time.Millisecond)
		ps.mu.Lock()
		p.acked = 3
		ps.cond.Broadcast()
		ps.mu.Unlock()
	}()
	start := time.Now()
	ps.waitAckedFor(memAckTarget{epoch: "e1", seq: 3, ok: true})
	if el := time.Since(start); el < 25*time.Millisecond || el > memLease {
		t.Fatalf("must wait for own ack: %v", el)
	}
	// 切断: ack が来なければ lease 切れまで待つ
	ps2, _ := ackOwnPeers(0)
	ps2.seq = 1
	start = time.Now()
	ps2.waitAckedFor(memAckTarget{epoch: "e1", seq: 1, ok: true})
	if el := time.Since(start); el < memLease {
		t.Fatalf("dead peer must be waited until lease end: %v", el)
	}
}

// epoch が切り替わった: 古い epoch の seq の ack は使えない。新 epoch での invalidate 確認 (または lease 切れ) を待つ。
func TestMemAckOwnEpochSwitch(t *testing.T) {
	ps, p := ackOwnPeers(100) // 新 epoch の ack 値 (古い seq より大きくても使ってはいけない)
	ps.epoch, ps.seq = "e2", 100
	go func() {
		time.Sleep(30 * time.Millisecond)
		ps.mu.Lock()
		p.invalidatedEpoch = "e2"
		ps.cond.Broadcast()
		ps.mu.Unlock()
	}()
	start := time.Now()
	ps.waitAckedFor(memAckTarget{epoch: "e1", seq: 5, ok: true})
	if el := time.Since(start); el < 25*time.Millisecond || el > memLease {
		t.Fatalf("epoch switch must wait for invalidate confirm: %v", el)
	}
}

// waitAcked (従来) は現在の ps.seq を待つ (既定の挙動は不変)。
func TestMemAckOwnLegacyWaitsLatest(t *testing.T) {
	ps, p := ackOwnPeers(3)
	ps.seq = 4
	go func() {
		time.Sleep(30 * time.Millisecond)
		ps.mu.Lock()
		p.acked = 4
		ps.cond.Broadcast()
		ps.mu.Unlock()
	}()
	start := time.Now()
	ps.waitAcked()
	if el := time.Since(start); el < 25*time.Millisecond {
		t.Fatalf("legacy waitAcked must wait for latest seq: %v", el)
	}
}
