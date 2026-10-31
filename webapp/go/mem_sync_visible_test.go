package isureal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

func withSyncVisible(t *testing.T) {
	ow, os := memWatermark, memSyncVisible
	memWatermark, memSyncVisible = true, true
	t.Cleanup(func() { memWatermark, memSyncVisible = ow, os })
}

func svHash(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

func svReq(tok string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/api/feeds/friends", nil)
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	return r
}

// svStore: eu の peer。住人 u-eu、asia の利用者 u-asia、退会済み u-gone。
func svStore() *memStore {
	m := wmTestStore()
	m.w.pumpSync.Store(true)
	m.w.caughtUp.Store(true)
	m.w.set(0) // wm は進まない (待つ読みは必ず DB に落ちる)
	add := func(id, region, status, tok string) {
		u := &memUser{}
		u.ID, u.Region, u.Status = id, region, status
		m.d.users[id] = u
		m.d.sessions[svHash(tok)] = memSession{UserID: id}
	}
	add("u-eu", "eu", "active", "tok-eu")
	add("u-asia", "asia", "active", "tok-asia")
	add("u-gone", "eu", "deleted", "tok-gone")
	return m
}

// 住人の判定: 判定できないものはすべて待つ側。
func TestMemSyncResident(t *testing.T) {
	withSyncVisible(t)
	m := svStore()
	cases := []struct {
		name string
		tok  string
		prep func()
		want bool
	}{
		{"resident", "tok-eu", nil, true},
		{"no token", "", nil, false},
		{"unknown token", "tok-x", nil, false},
		{"other region (user moved / asia user)", "tok-asia", nil, false},
		{"withdrawn", "tok-gone", nil, false},
		{"marked (local invalidation in flight)", "tok-eu", func() { m.marks.add("u-eu") }, false},
		{"not caught up", "tok-eu", func() { m.w.caughtUp.Store(false) }, false},
		{"pump not sync", "tok-eu", func() { m.w.pumpSync.Store(false) }, false},
		{"lease expired", "tok-eu", func() { m.leaseUntil.Store(0) }, false},
		{"not ready", "tok-eu", func() { m.ready.Store(false) }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m = svStore()
			if c.prep != nil {
				c.prep()
			}
			if got := m.syncResident(svReq(c.tok), "eu"); got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
	m = svStore()
	if m.syncResident(svReq("tok-eu"), "") {
		t.Fatal("unknown local region must wait")
	}
	old := memSyncVisible
	memSyncVisible = false
	if m.syncResident(svReq("tok-eu"), "eu") {
		t.Fatal("flag off must wait")
	}
	memSyncVisible = old
}

// middleware: 住人は wm を待たずにガードを通る。非住人は従来どおり待って (上限で) DB 経路。
// 処理中に lease が切れたら、入口で住人でもガードで落ちる。
func TestMemSyncReadWaitMiddleware(t *testing.T) {
	withSyncVisible(t)
	oldMax := memWMMaxWait
	memWMMaxWait = 20 * time.Millisecond
	t.Cleanup(func() { memWMMaxWait = oldMax })
	oldR := memLocalRegion
	memLocalRegion = "eu"
	t.Cleanup(func() { memLocalRegion = oldR })
	for _, tc := range []struct {
		tok      string
		expire   bool
		want     bool
		maxDelay time.Duration
	}{
		{"tok-eu", false, true, 10 * time.Millisecond},
		{"tok-asia", false, false, time.Second},
		{"tok-x", false, false, time.Second},
		{"", false, false, time.Second},
		{"tok-eu", true, false, time.Second}, // 入口で住人でも、処理中に lease が切れたらガードで落ちる
	} {
		m := svStore()
		var ok bool
		e := echo.New()
		e.Use(memReadWait(m))
		e.GET("/api/feeds/:feed", func(c *echo.Context) error {
			if tc.expire {
				m.leaseUntil.Store(0)
			}
			ok = m.onCtx(c.Request().Context())
			return c.NoContent(http.StatusOK)
		})
		start := time.Now()
		e.ServeHTTP(httptest.NewRecorder(), svReq(tc.tok))
		if ok != tc.want {
			t.Fatalf("%q expire=%v: guard=%v want %v", tc.tok, tc.expire, ok, tc.want)
		}
		if el := time.Since(start); el > tc.maxDelay {
			t.Fatalf("%q: took %v", tc.tok, el)
		}
	}
}

// svCtx: 住人と判定済みの読みの ctx (wm の目標は届かない値)。
func svCtx(tr int64) context.Context {
	ctx := context.WithValue(context.Background(), memWMTargetKey{}, tr+int64(time.Hour))
	return context.WithValue(ctx, memSyncSkipKey{}, true)
}

// waitAcked: フラグ on なら wm モードの peer の ack も待つ (同期)。lease 切れまで待ったら先へ進む。
func TestMemSyncWaitAckedWaitsWMPeers(t *testing.T) {
	withSyncVisible(t)
	wmPeer := &memPeer{addr: "127.0.0.1:1", lastGrantAt: time.Now(), wmMode: true}
	ps := &memPeers{epoch: "e1", peers: []*memPeer{wmPeer}, seq: 3}
	ps.cond = sync.NewCond(&ps.mu)
	go func() {
		time.Sleep(30 * time.Millisecond)
		ps.mu.Lock()
		wmPeer.acked = 3
		ps.cond.Broadcast()
		ps.mu.Unlock()
	}()
	start := time.Now()
	ps.waitAcked()
	if el := time.Since(start); el < 25*time.Millisecond || el > memLease {
		t.Fatalf("must wait for wm peer ack: %v", el)
	}
	// 負: フラグ off なら wm モードの peer は待たない (= 従来の wm 方式)
	memSyncVisible = false
	ps.seq = 4
	start = time.Now()
	ps.waitAcked()
	if el := time.Since(start); el > 20*time.Millisecond {
		t.Fatalf("flag off must not wait: %v", el)
	}
}

// 不変条件 (並行): 書き手 (asia1) 4、読み手 (eu の住人) 3、push の遅延・停止あり。
// 待たずに返した読みについて、到着前に 2xx を返した書き込み (投稿・再ログインによる旧セッション無効化) が全部見えていること。
//
// mode: "ok" = 実装どおり。"noack" = 書き手が ack を待たない (判定を誤って同期にしない)。
// "nogate" = 読み手が lease / caughtUp を見ない (lease 切れで先へ進んだ書き込みがある間も待たずに読む)。
func runSyncInvariant(t *testing.T, mode string, dur time.Duration) (violations, skipped int64) {
	withSyncVisible(t)
	m := svStore()
	m.extendLeaseFrom(time.Now().UnixNano())

	var (
		pumpMu    sync.Mutex
		seq       int64
		lastGrant = time.Now()
		pausedTil atomic.Int64 // push 停止 (この時刻まで peer に届かない)
		applied   atomic.Int64
		rmu       sync.Mutex // memReceiver.mu 相当

		respMu  sync.Mutex
		resp    []int64 // 応答時刻 (昇順)
		respMax []int64 // resp[:i+1] の seq の最大
		// 再ログイン: 旧トークンの無効化が応答した時刻
		tokMu   sync.Mutex
		curTok  = "tok-eu"
		oldToks = map[string]int64{}
	)
	type batch struct {
		seq            int64
		sentAt         int64
		oldTok, newTok string
	}
	q := make(chan batch, 1<<16)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// deliverer: 順に、遅延付きで適用。停止中は届かない。lease は送信時刻から (適用後に延長)。
	wg.Go(func() {
		for b := range q {
			for time.Now().UnixNano() < pausedTil.Load() {
				time.Sleep(time.Millisecond)
			}
			time.Sleep(time.Duration(rand.IntN(400)) * time.Microsecond)
			if b.newTok != "" {
				m.mu.Lock()
				delete(m.d.sessions, svHash(b.oldTok))
				m.d.sessions[svHash(b.newTok)] = memSession{UserID: "u-eu"}
				m.mu.Unlock()
			}
			applied.Store(b.seq)
			rmu.Lock()
			m.w.syncSeen(b.seq, b.seq)
			rmu.Unlock()
			m.extendLeaseFrom(b.sentAt)
		}
	})
	// wm 通知 (別経路、先行しうる): pump の seq を知らせる
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			time.Sleep(time.Duration(rand.IntN(500)) * time.Microsecond)
			pumpMu.Lock()
			s := seq
			pumpMu.Unlock()
			rmu.Lock()
			m.w.syncSeen(s, applied.Load())
			rmu.Unlock()
		}
	})

	var writers sync.WaitGroup
	for w := range 4 {
		writers.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				var b batch
				pumpMu.Lock()
				seq++
				b.seq = seq
				now := time.Now()
				b.sentAt = now.UnixNano()
				if now.UnixNano() >= pausedTil.Load() {
					lastGrant = now // 停止中の peer には lease を与える push は届かない
				}
				relogin := w == 0 && i%50 == 49
				if relogin {
					tokMu.Lock()
					b.oldTok, b.newTok = curTok, fmt.Sprintf("tok-eu-%d", b.seq)
					tokMu.Unlock()
				}
				q <- b
				pumpMu.Unlock()
				// waitAcked 相当: ack (適用) か lease 切れまで待つ
				if mode != "noack" {
					for applied.Load() < b.seq {
						pumpMu.Lock()
						leaseEnd := lastGrant.Add(memLease + memClockSkew)
						pumpMu.Unlock()
						if !time.Now().Before(leaseEnd) {
							break
						}
						time.Sleep(50 * time.Microsecond)
					}
				}
				respMu.Lock()
				at := time.Now().UnixNano()
				mx := b.seq
				if n := len(respMax); n > 0 && respMax[n-1] > mx {
					mx = respMax[n-1]
				}
				resp = append(resp, at)
				respMax = append(respMax, mx)
				respMu.Unlock()
				if relogin {
					tokMu.Lock()
					oldToks[b.oldTok] = at
					curTok = b.newTok
					tokMu.Unlock()
				}
				time.Sleep(time.Duration(rand.IntN(300)) * time.Microsecond)
			}
		})
	}

	// readers
	var readers sync.WaitGroup
	for range 3 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				tokMu.Lock()
				tok := curTok
				useOld := ""
				for o := range oldToks {
					useOld = o
					break
				}
				tokMu.Unlock()
				if useOld != "" && rand.IntN(2) == 0 {
					tok = useOld
				}
				tr := time.Now().UnixNano()
				var served bool
				var seen int64
				if mode == "nogate" {
					m.mu.RLock()
					_, served = m.d.sessions[svHash(tok)]
					seen = applied.Load()
					m.mu.RUnlock()
				} else if m.syncResident(svReq(tok), "eu") {
					ctx := svCtx(tr)
					m.mu.RLock()
					served = m.onCtx(ctx)
					seen = applied.Load()
					if served {
						_, served = m.d.sessions[svHash(tok)] // メモリで認証 (無ければ 401 / DB)
					}
					m.mu.RUnlock()
				}
				if !served {
					continue // 待つ読み / DB 経路 (このテストの対象外)
				}
				atomic.AddInt64(&skipped, 1)
				respMu.Lock()
				i := sort.Search(len(resp), func(i int) bool { return resp[i] >= tr })
				var need int64
				if i > 0 {
					need = respMax[i-1]
				}
				respMu.Unlock()
				if need > seen {
					atomic.AddInt64(&violations, 1) // 見えるべき書き込みが無い
				}
				tokMu.Lock()
				at, isOld := oldToks[tok]
				tokMu.Unlock()
				if isOld && at < tr {
					atomic.AddInt64(&violations, 1) // 無効化済みの旧トークンが通った (見えてはいけない)
				}
			}
		})
	}

	// 途中で push を止める (lease 切れで書き手が先へ進む場面)
	time.Sleep(dur / 4)
	pausedTil.Store(time.Now().Add(memLease + memClockSkew + 300*time.Millisecond).UnixNano())
	time.Sleep(dur)
	close(stop)
	writers.Wait()
	readers.Wait()
	close(q)
	wg.Wait()
	return violations, skipped
}

func TestMemSyncVisibleInvariant(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	v, s := runSyncInvariant(t, "ok", 2*time.Second)
	if v != 0 {
		t.Fatalf("violations=%d (skipped reads %d)", v, s)
	}
	if s < 100 {
		t.Fatalf("too few unwaited reads: %d", s)
	}
	t.Logf("unwaited reads=%d violations=0", s)
}

// 負のテスト: わざと判定を誤らせると違反を検出する。
func TestMemSyncVisibleInvariantDetectsNoAck(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	if v, _ := runSyncInvariant(t, "noack", 2*time.Second); v == 0 {
		t.Fatal("writers not waiting for ack must produce violations")
	}
}

func TestMemSyncVisibleInvariantDetectsNoGate(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	if v, _ := runSyncInvariant(t, "nogate", 2*time.Second); v == 0 {
		t.Fatal("readers ignoring lease/caughtUp must produce violations")
	}
}
