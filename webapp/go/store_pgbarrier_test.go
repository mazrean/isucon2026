package isureal

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 偽の WAL: 書き込みは seq を得て (メモリで確定) 応答し、PG への反映 (pgApplied) は遅れて進む。
type fakeWAL struct {
	mu        sync.Mutex
	seq       int64
	applied   atomic.Int64
	responded map[int64]int64 // seq -> 応答時刻
	ch        chan struct{}
}

func newFakeWAL() *fakeWAL { return &fakeWAL{responded: map[int64]int64{}, ch: make(chan struct{})} }

func (w *fakeWAL) write() {
	w.mu.Lock()
	w.seq++
	s := w.seq
	w.mu.Unlock()
	now := memNow().UnixNano() // 応答 (seq を得た後)
	w.mu.Lock()
	w.responded[s] = now
	w.mu.Unlock()
}

func (w *fakeWAL) current() int64 { w.mu.Lock(); defer w.mu.Unlock(); return w.seq }

func (w *fakeWAL) flushLoop(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		time.Sleep(time.Duration(rand.IntN(3000)) * time.Microsecond)
		w.mu.Lock()
		w.applied.Store(w.seq)
		close(w.ch)
		w.ch = make(chan struct{})
		w.mu.Unlock()
	}
}

func (w *fakeWAL) barrier(ctx context.Context, seq int64) error {
	for {
		w.mu.Lock()
		if w.applied.Load() >= seq {
			w.mu.Unlock()
			return nil
		}
		ch := w.ch
		w.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// 到着時刻より前に応答した書き込みの最大 seq。
func (w *fakeWAL) maxRespondedBefore(t int64) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var m int64
	for s, at := range w.responded {
		if at < t && s > m {
			m = s
		}
	}
	return m
}

func withStoreWAL(t *testing.T, w *fakeWAL) {
	oldOn, oldB, oldC, oldA, oldAwait := storeWALOn, storePGBarrierFn, storeCurrentSeqFn, storePGAppliedFn, storeAwaitPG
	storeWALOn, storeAwaitPG = true, false
	storePGBarrierFn, storeCurrentSeqFn, storePGAppliedFn = w.barrier, w.current, w.applied.Load
	storePGPeer = newPeerPGState()
	t.Cleanup(func() {
		storeWALOn, storePGBarrierFn, storeCurrentSeqFn, storePGAppliedFn, storeAwaitPG = oldOn, oldB, oldC, oldA, oldAwait
		storePGRolePeer.Store(false)
		storePGPeer = newPeerPGState()
	})
}

// asia1 と peer の両方で: PG の最初の文の前の待ちが終わった時点で、到着時刻より前に応答した書き込みは PG に入っている。
func TestStorePGBarrierRYWConcurrent(t *testing.T) {
	w := newFakeWAL()
	withStoreWAL(t, w)
	stop := make(chan struct{})
	stopCtx, stopCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				w.write()
				time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond)
			}
		})
	}
	wg.Go(func() { w.flushLoop(stop) })
	// asia1 → peer の標本 (push / wm 通知): 遅延・入れ替わりあり
	var sendWG sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			pt, ps, pa := storePGSample()
			sendWG.Go(func() {
				time.Sleep(time.Duration(rand.IntN(3000)) * time.Microsecond)
				storePGPeer.update(pt, ps, pa)
			})
			time.Sleep(time.Duration(rand.IntN(1000)) * time.Microsecond)
		}
	})
	var reads, vio atomic.Int64
	reader := func(peer bool) {
		for {
			select {
			case <-stop:
				return
			default:
			}
			arrive := memNow().UnixNano()
			var ctx context.Context
			if peer {
				ctx = context.WithValue(stopCtx, pgBarrierKey{}, &pgBarrierTarget{peer: true, at: arrive})
			} else {
				ctx = storePGReadCtx(stopCtx)
			}
			pgBarrierWait(ctx) // = PG の最初の文の直前 (QueryTracer)
			tg := ctx.Value(pgBarrierKey{}).(*pgBarrierTarget)
			if !tg.done.Load() {
				if stopCtx.Err() != nil {
					return // 終了 (標本の送信が止まった)
				}
				t.Errorf("barrier timed out (peer=%v)", peer)
				return
			}
			pgNow := w.applied.Load() // この後の PG の読みが見える範囲の下限
			if need := w.maxRespondedBefore(arrive); pgNow < need {
				vio.Add(1)
				t.Errorf("peer=%v: PG applied %d < seq %d responded before arrival", peer, pgNow, need)
			}
			reads.Add(1)
			pgBarrierWait(ctx) // 2 回目以降は素通り
			time.Sleep(time.Duration(rand.IntN(500)) * time.Microsecond)
		}
	}
	// peer の読み手は、標本が届き始めてから (届く前は「asia1 で無効」と同じで待たない)
	time.Sleep(10 * time.Millisecond)
	for i := range 8 {
		peer := i%2 == 1
		wg.Go(func() { reader(peer) })
	}
	time.Sleep(700 * time.Millisecond)
	close(stop)
	stopCancel()
	wg.Wait()
	sendWG.Wait()
	if reads.Load() == 0 {
		t.Fatal("no reads")
	}
	t.Logf("reads=%d violations=%d", reads.Load(), vio.Load())
}

// フラグ off・フックが nil なら何もしない (既定の挙動は不変)。
func TestStorePGBarrierOffIsNoop(t *testing.T) {
	ctx := context.Background()
	if storePGReadCtx(ctx) != ctx {
		t.Fatal("ctx changed with ISUREAL_STORE_WAL unset")
	}
	oldOn := storeWALOn
	storeWALOn = true
	t.Cleanup(func() { storeWALOn = oldOn })
	if storePGBarrierFn == nil && storePGReadCtx(ctx) != ctx {
		t.Fatal("asia1 without the WAL hooks must not set a target")
	}
	if pt, _, _ := storePGSample(); pt != 0 {
		t.Fatal("sample without hooks")
	}
	// peer: 標本を 1 つも受けていなければ待たない
	p := newPeerPGState()
	if _, err := p.wait(ctx, memNow().UnixNano()); err != nil {
		t.Fatal(err)
	}
}

// peer: 目標の時刻より後の標本が無い間は待つ。asia1 の再起動 (seq の系列が戻る) で古い PA を捨てる。
func TestPeerPGStateWaitAndRestart(t *testing.T) {
	p := newPeerPGState()
	p.update(100, 10, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := p.wait(ctx, 50); err == nil {
		t.Fatal("PA 5 < PS 10 must wait")
	}
	p.update(200, 12, 12)
	if s, err := p.wait(context.Background(), 150); err != nil || s != 12 {
		t.Fatalf("s=%d err=%v", s, err)
	}
	p.update(300, 3, 1) // 再起動: seq が戻った
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	if _, err := p.wait(ctx2, 250); err == nil {
		t.Fatal("stale PA from before the restart satisfied the barrier")
	}
}

// LAZY: 上限を超えても待ちをやめず、PG が追いついたら通す。要求の ctx が切れたら取り消し済みの ctx を返す。
func TestStorePGBarrierLazyTimeout(t *testing.T) {
	oldLazy, oldMax, oldFn := storeBarrierLazy, storeWALBarrierMax, storePGBarrierFn
	t.Cleanup(func() { storeBarrierLazy, storeWALBarrierMax, storePGBarrierFn = oldLazy, oldMax, oldFn })
	storeBarrierLazy, storeWALBarrierMax = true, 5*time.Millisecond
	var applied atomic.Int64
	storePGBarrierFn = func(ctx context.Context, seq int64) error {
		for applied.Load() < seq {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
		return nil
	}
	tg := &pgBarrierTarget{seq: 1}
	ctx := context.WithValue(context.Background(), pgBarrierKey{}, tg)
	go func() { time.Sleep(30 * time.Millisecond); applied.Store(1) }()
	if got := pgBarrierWait(ctx); got.Err() != nil || !tg.done.Load() {
		t.Fatalf("lazy: want pass after catch-up, err=%v done=%v", got.Err(), tg.done.Load())
	}
	if applied.Load() < 1 {
		t.Fatal("lazy: passed before PG applied")
	}
	tg2 := &pgBarrierTarget{seq: 2}
	rctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ctx2 := context.WithValue(rctx, pgBarrierKey{}, tg2)
	if got := pgBarrierWait(ctx2); got.Err() == nil || tg2.done.Load() {
		t.Fatalf("lazy: want cancelled ctx, err=%v done=%v", got.Err(), tg2.done.Load())
	}
}
