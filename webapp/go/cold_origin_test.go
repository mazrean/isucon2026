package isureal

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

func withColdOrigin(t *testing.T, addr string) {
	oldO, oldS, oldSeq, oldWM := coldOrigin, forwardSecret, coldPumpSeqFn, coldPumpWM
	coldOrigin, forwardSecret = addr, "s3cret"
	coldPumpWM = newPeerPGState()
	oldGate := coldOriginGate
	coldOriginGate = func() bool { return true } // 偽の origin は状態を答えない (状態の確認は mem_full_gate_test.go)
	t.Cleanup(func() {
		coldOrigin, forwardSecret, coldPumpSeqFn, coldPumpWM, coldOriginGate = oldO, oldS, oldSeq, oldWM, oldGate
	})
}

// handler: pg=true なら「PG の最初の文」(= QueryTracer の coldOnPG) を踏み、取り消された ctx のエラーを返す。
func coldTestHandler(runs *atomic.Int64, pg bool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		runs.Add(1)
		ctx := c.Request().Context()
		if pg {
			if coldOnPG(ctx) && ctx.Err() != nil {
				return ctx.Err() // pgx は取り消された ctx で失敗する
			}
			return c.JSON(http.StatusOK, map[string]string{"from": "local-pg"})
		}
		return c.JSON(http.StatusOK, map[string]string{"from": "memory"})
	}
}

func coldServe(t *testing.T, mw echo.MiddlewareFunc, h echo.HandlerFunc, target string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Cookie", "a=b")
	rec := httptest.NewRecorder()
	if err := mw(h)(e.NewContext(req, rec)); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	return rec
}

func TestColdOriginForwardsOnlyPGFallbacks(t *testing.T) {
	var got http.Header
	var gotURI string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, gotURI = r.Header.Clone(), r.URL.RequestURI()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Test", "1")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"from":"asia2"}`))
	}))
	defer origin.Close()
	withColdOrigin(t, strings.TrimPrefix(origin.URL, "http://"))
	coldPumpSeqFn = func() int64 { return 42 }
	mw := coldMiddleware(false)

	var runs atomic.Int64
	rec := coldServe(t, mw, coldTestHandler(&runs, false), "/api/feeds/friends?limit=3")
	if !strings.Contains(rec.Body.String(), "memory") || got != nil {
		t.Fatalf("memory answer must not be forwarded: body=%s", rec.Body.String())
	}
	rec = coldServe(t, mw, coldTestHandler(&runs, true), "/api/feeds/friends?limit=3")
	if !strings.Contains(rec.Body.String(), "asia2") || rec.Header().Get("X-Test") != "1" || rec.Code != http.StatusOK {
		t.Fatalf("PG fallback not forwarded: code=%d body=%s", rec.Code, rec.Body.String())
	}
	if gotURI != "/api/feeds/friends?limit=3" || got.Get("Authorization") != "Bearer tok" || got.Get("Cookie") != "a=b" ||
		got.Get(coldWaitHeader) != "42" || got.Get(coldSpaceHeader) != "pump" || got.Get(coldHopHeader) != "s3cret" {
		t.Fatalf("forwarded request: uri=%s hdr=%v", gotURI, got)
	}
	// 冷たい転送の要求そのもの (asia2 側) と画像は対象外
	req := httptest.NewRequest(http.MethodGet, "/api/images/x", nil)
	if coldTarget(req) {
		t.Fatal("images must not be cold-forwarded")
	}
	req = httptest.NewRequest(http.MethodGet, "/api/feeds/public", nil)
	req.Header.Set(coldHopHeader, "s3cret")
	if coldTarget(req) || !coldIsHop(req) {
		t.Fatal("hop requests must not loop")
	}
}

func TestColdOriginFallsBackOn5xx(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer origin.Close()
	withColdOrigin(t, strings.TrimPrefix(origin.URL, "http://"))
	coldPumpSeqFn = func() int64 { return 1 }
	var runs atomic.Int64
	rec := coldServe(t, coldMiddleware(false), coldTestHandler(&runs, true), "/api/feeds/public")
	if !strings.Contains(rec.Body.String(), "local-pg") || runs.Load() != 2 {
		t.Fatalf("5xx must re-run locally on PG: runs=%d body=%s", runs.Load(), rec.Body.String())
	}
	// asia2 が落ちている
	withColdOrigin(t, "127.0.0.1:1")
	coldPumpSeqFn = func() int64 { return 1 }
	runs.Store(0)
	rec = coldServe(t, coldMiddleware(false), coldTestHandler(&runs, true), "/api/feeds/public")
	if !strings.Contains(rec.Body.String(), "local-pg") || runs.Load() != 2 {
		t.Fatalf("unreachable origin must re-run locally: runs=%d body=%s", runs.Load(), rec.Body.String())
	}
}

// peer (並行): 転送に載る seq は、到着時刻より前に応答したコミットの batch の seq 以上 (WM と同じ意味論)。
func TestColdOriginPeerSeqCoversArrivalConcurrent(t *testing.T) {
	var mu sync.Mutex
	responded := map[int64]int64{} // batch seq -> そこに入ったコミットの応答時刻の最小
	var seq atomic.Int64
	var maxSeen atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.Header.Get(coldWaitHeader)))
	}))
	defer origin.Close()
	withColdOrigin(t, strings.TrimPrefix(origin.URL, "http://"))
	mw := coldMiddleware(true)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// pump: コミット (seq++) → 応答。wm の組 (W = 時刻を先に取る, seq) を遅延・入れ替わり付きで届ける
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			s := seq.Add(1)
			now := memNow().UnixNano()
			mu.Lock()
			responded[s] = now
			mu.Unlock()
			w := memNow().UnixNano()
			cur := seq.Load()
			go func() {
				time.Sleep(time.Duration(cur%3) * time.Millisecond)
				coldPumpWM.update(w, cur, 0)
			}()
			time.Sleep(300 * time.Microsecond)
		}
	})
	var vio, n atomic.Int64
	for range 6 {
		wg.Go(func() {
			e := echo.New()
			var runs atomic.Int64
			h := coldTestHandler(&runs, true)
			for {
				select {
				case <-stop:
					return
				default:
				}
				arrive := memNow().UnixNano()
				req := httptest.NewRequest(http.MethodGet, "/api/feeds/friends", nil)
				rec := httptest.NewRecorder()
				if err := mw(h)(e.NewContext(req, rec)); err != nil {
					t.Errorf("err %v", err)
					return
				}
				got, err := strconv.ParseInt(rec.Body.String(), 10, 64)
				if err != nil {
					continue // local-pg (標本が来なかった)
				}
				n.Add(1)
				mu.Lock()
				var need int64
				for s, at := range responded {
					if at < arrive && s > need {
						need = s
					}
				}
				mu.Unlock()
				if got < need {
					vio.Add(1)
					t.Errorf("forwarded seq %d < %d (responded before arrival)", got, need)
				}
				if got > maxSeen.Load() {
					maxSeen.Store(got)
				}
			}
		})
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	if n.Load() == 0 {
		t.Fatal("nothing forwarded")
	}
	t.Logf("forwarded=%d violations=%d", n.Load(), vio.Load())
}
