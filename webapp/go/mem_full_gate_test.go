package isureal

import (
	"testing"
	"time"
)

func TestColdOriginUsableGate(t *testing.T) {
	coldOriginPollOnce.Do(func() {}) // 背景の poll を止めたまま標本を直接入れる
	oldE, oldS := coldPumpEpochFn, coldPumpSeqFn
	defer func() { coldPumpEpochFn, coldPumpSeqFn = oldE, oldS; coldOriginSt.Store(nil) }()
	coldPumpEpochFn = func() string { return "e1" }
	coldPumpSeqFn = func() int64 { return 1000 }
	cases := []struct {
		name string
		st   *coldOriginStatus
		want bool
	}{
		{"no sample", nil, false},
		{"not ready", &coldOriginStatus{Ready: false, Epoch: "e1", Pump: 1000, at: time.Now()}, false},
		{"other epoch", &coldOriginStatus{Ready: true, Epoch: "e0", Pump: 1000, at: time.Now()}, false},
		{"lagging", &coldOriginStatus{Ready: true, Epoch: "e1", Pump: 1000 - coldOriginMaxLag - 1, at: time.Now()}, false},
		{"stale sample", &coldOriginStatus{Ready: true, Epoch: "e1", Pump: 1000, at: time.Now().Add(-time.Second)}, false},
		{"ok", &coldOriginStatus{Ready: true, Epoch: "e1", Pump: 1000 - coldOriginMaxLag, at: time.Now()}, true},
	}
	for _, c := range cases {
		coldOriginSt.Store(c.st)
		if got := coldOriginUsable(); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
