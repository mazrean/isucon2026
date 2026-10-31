package isureal

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
)

// ISUREAL_FWD_PRIORITY_YIELD=N (既定 0 = off): fwd-priority の枠を取ったローカル書き込みが
// fsync (localimg の sync)・PG の書き戻し・peer の ack を待つ間は枠を返す (CPU の処理の間だけ握る)。
// 返した要求は「待ち中」として数え、その数を N で抑える (入場の総量 = 枠 + N を超えない)。
// N に達していれば枠は握ったまま待つ (今までどおり)。返した枠は取り直さない (待ちの後は応答を書くだけ)。
var fwdPriorityYieldMax = int64(envIntOrDefault("ISUREAL_FWD_PRIORITY_YIELD", 0))

type fwdSlotHold struct {
	once   sync.Once
	slots  chan struct{}
	parked *atomic.Int64
	max    int64
	state  atomic.Int32 // 0 = 枠を握っている, 1 = 返して待ち中, 2 = 終了
}

type fwdSlotHoldKey struct{}

func withFwdSlotHold(ctx context.Context, h *fwdSlotHold) context.Context {
	return context.WithValue(ctx, fwdSlotHoldKey{}, h)
}

// fwdSlotYield: 待ちに入る前に呼ぶ。枠を握っていて待ち中の数が上限未満なら枠を返す。何度呼んでもよい。
func fwdSlotYield(ctx context.Context) {
	h, _ := ctx.Value(fwdSlotHoldKey{}).(*fwdSlotHold)
	if h == nil || h.max <= 0 {
		return
	}
	if h.parked.Add(1) > h.max {
		h.parked.Add(-1)
		return
	}
	if !h.state.CompareAndSwap(0, 1) {
		h.parked.Add(-1)
		return
	}
	<-h.slots
	fwdYieldStats.yielded.Add(1)
}

// finish: 要求の終わりに middleware が呼ぶ。握っていれば枠を返し、待ち中なら数を戻す。
func (h *fwdSlotHold) finish() {
	switch h.state.Swap(2) {
	case 0:
		<-h.slots
	case 1:
		h.parked.Add(-1)
	}
}

var fwdYieldStats struct{ yielded atomic.Int64 }

// ISUREAL_STORE_PRIO=1 (既定 0): store の適用役の受付を 2 列に分け、投稿 (createPost) 以外
// (login・友達申請・承認など) の列を先に取り出す。適用役は 1 本のまま (原子性・排他は同じ)。
// 1 つの要求の中の順序は変わらない。同時に並んだ別々の要求どうしの順だけが変わる (元から順は決まっていない)。
var storePrioOn = os.Getenv("ISUREAL_STORE_PRIO") == "1"
