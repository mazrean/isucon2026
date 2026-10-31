package isureal

import (
	"sync"
	"testing"
	"time"
)

// 到達不能な peer は即失敗するが、lease (lastSentAt + memLease) が切れるまでは書き込みを保留する。
func TestMemWaitAckedDeadPeerWaitsLease(t *testing.T) {
	ps := &memPeers{epoch: "e1", peers: []*memPeer{{addr: "127.0.0.1:1", lastAckAt: time.Now(), lastSentAt: time.Now()}}}
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
