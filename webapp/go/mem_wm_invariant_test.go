package isureal

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// wm の不変条件 (並行): peer の読みが wm >= T_r (+ε, slack 0) を確認した時点で、
// 応答が T_r より前に返ったコミットは、すべて peer の適用済み seq 以前のバッチに入っている。
//
// pump の作り方をモデル化する: x = now を取ってから (DB の) スナップショットを取り、新しいコミットを 1 バッチにして
// 連番を振る。データ push は順に (遅延付きで) 届け、wm 通知は別経路で遅延・入れ替わりありで届ける。
// early=true は ISUREAL_MEM_WM_EARLY と同じく wm をデータ push にも同梱する。
func TestMemWMInvariantConcurrent(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(map[bool]string{false: "wmmsg", true: "early"}[early], func(t *testing.T) {
			runWMInvariant(t, early, false)
		})
	}
}

func runWMInvariant(t *testing.T, early, xAfterSnap bool) (violations int64) {
	m := wmTestStore()
	m.w.set(0)
	r := wmTestReceiver(m, "e1", 0)

	var (
		dbMu      sync.Mutex
		committed []int
		responded = map[int]int64{} // id -> 応答時刻 (コミットが可視になった後)
		seqOf     = map[int]int64{} // id -> 含まれたバッチの seq
		nextID    int
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// writers
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				dbMu.Lock()
				nextID++
				id := nextID
				committed = append(committed, id) // COMMIT (可視)
				dbMu.Unlock()
				now := memNow().UnixNano() // 応答はコミットが可視になった後
				dbMu.Lock()
				responded[id] = now
				dbMu.Unlock()
				time.Sleep(time.Duration(rand.IntN(300)) * time.Microsecond)
			}
		})
	}

	// pump
	pushes := make(chan memPushRequest, 1024)
	var wmWG sync.WaitGroup
	wg.Go(func() {
		defer close(pushes)
		var last int
		var seq int64
		for {
			select {
			case <-stop:
				return
			default:
			}
			x := memNow().UnixNano()
			time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond) // クエリが届くまで
			dbMu.Lock()
			n := len(committed)
			ids := append([]int(nil), committed[last:n]...)
			last = n
			seq++
			for _, id := range ids {
				seqOf[id] = seq
			}
			dbMu.Unlock()
			time.Sleep(time.Duration(rand.IntN(300)) * time.Microsecond) // 結果が返るまで
			if xAfterSnap { // 誤った作り方 (負のテスト用): スナップショットの後に時刻を取る
				x = memNow().UnixNano()
			}
			req := memPushRequest{Epoch: "e1", Seq: seq, Batches: []memBatch{{Seq: seq}}}
			if early {
				req.WM = x
			}
			pushes <- req
			msg := memWMMsg{Epoch: "e1", Seq: seq, WM: x}
			wmWG.Go(func() {
				time.Sleep(time.Duration(rand.IntN(3000)) * time.Microsecond)
				r.wmMsg(msg)
			})
			time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		}
	})
	// データ push (順に、遅延付き)
	wg.Go(func() {
		for req := range pushes {
			time.Sleep(time.Duration(rand.IntN(1500)) * time.Microsecond)
			if !r.push(req) {
				t.Errorf("push %d rejected", req.Seq)
			}
			r.wmAfterPush(req)
		}
	})

	// readers
	var reads, ok atomic.Int64
	var vio atomic.Int64
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := memNow()
				target := start.UnixNano() // ε = 0, slack = 0 (同じ時計)
				o := m.waitWM(target, start)
				reads.Add(1)
				if !o.ok {
					continue
				}
				ok.Add(1)
				r.mu.Lock()
				applied := r.seq
				wm := m.w.wm.Load()
				r.mu.Unlock()
				if wm < target {
					t.Errorf("wm %d < target %d after ok", wm, target)
				}
				dbMu.Lock()
				for id, at := range responded {
					if at < target {
						if s, in := seqOf[id]; !in || s > applied {
							vio.Add(1)
							if !xAfterSnap {
								t.Errorf("commit %d (responded %d < T_r %d) not visible: seq=%d in=%v applied=%d", id, at, target, s, in, applied)
							}
						}
					}
				}
				dbMu.Unlock()
				time.Sleep(time.Duration(rand.IntN(500)) * time.Microsecond)
			}
		})
	}

	time.Sleep(800 * time.Millisecond)
	close(stop)
	wg.Wait()
	wmWG.Wait()
	if !xAfterSnap && ok.Load() == 0 {
		t.Fatalf("no read reached the watermark (reads=%d)", reads.Load())
	}
	t.Logf("reads=%d ok=%d violations=%d", reads.Load(), ok.Load(), vio.Load())
	return vio.Load()
}

// 負のテスト: 時刻をスナップショットの後に取る (誤った wm) と、上のテストは違反を検出する (テストの感度の確認)。
func TestMemWMInvariantDetectsLateX(t *testing.T) {
	if v := runWMInvariant(t, false, true); v == 0 {
		t.Fatal("late-x watermark produced no violation: the invariant test is not sensitive")
	}
}
