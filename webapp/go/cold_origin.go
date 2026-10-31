package isureal

import (
	"context"
	"crypto/subtle"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_COLD_ORIGIN=host:port (asia1 / eu / am、既定は空 = off): 公開 API の GET が自分のメモリで答えられず
// PG に落ちる時点で、PG を読む代わりに元の要求をそのまま (パス・クエリ・全ヘッダ) 冷たい読みのノード (asia2,
// ISUREAL_MEM_FULL=1) に転送する。
//
// 落ちる箇所の検出 (全部の分岐を手で洗わない): 対象の要求に印を付けた ctx で handler を走らせ、
// pgx の QueryTracer (traceDSN が全プールに付ける。書き戻しの "store" プールは除く) が最初の PG の文を見た時点で
// その ctx を取り消す。PG の文は失敗し、handler の応答は捨てて asia2 に転送する。これで mem_read.go の
// fallback、Memories / export、セッションの DB 確認、ホット外の読みのすべてが同じ形で拾える。
// GET の handler は読みだけなので、捨てて転送しても副作用は無い。
//
// 待ちの目印 X-Isureal-Cold-Wait: <seq> と X-Isureal-Cold-Space: store|pump。asia2 は自分の適用済みの seq が
// それ以上になってからメモリで答える (上限 2000ms、超えたら asia2 の PG)。
//   - asia1: 到着時の storeCurrentSeq (ISUREAL_STORE_WAL のフックがあれば、space=store)。無ければ pump の seq
//     (書き込みの応答はサイクル完了 = publish 後なので、到着前に応答した書き込みの batch は到着時の seq 以下)。
//   - eu/am: WM と同じ意味論。到着時刻 T_r + ε 以降の asia1 の標本の seq。store の標本 (store_pgbarrier.go) があれば store、
//     無ければ wm (W, seq) の組 (W より前に応答したコミットは seq 以下)。標本が上限まで来なければ転送しない。
//
// asia2 が応答しない / 5xx / 標本が取れないときは、今までどおり自分の PG の経路 (barrier 付き) で handler を走らせ直す。
// 対象外: 画像 (/api/images/、バイト列は今までどおり)、SSE の /api/notifications、/api/initialize、
// asia2 からの転送・キャッシュ origin の要求 (ループ防止)。
var (
	coldOrigin       = os.Getenv("ISUREAL_COLD_ORIGIN")
	coldOriginClient = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxIdleConns: 512, IdleConnTimeout: 90 * time.Second}}
	coldSampleWait   = time.Duration(envIntOrDefault("ISUREAL_COLD_SAMPLE_WAIT_MS", 300)) * time.Millisecond
	coldPumpWM       = newPeerPGState() // peer: wm の組 (W, seq)
	coldPumpSeqFn    func() int64       // asia1: pump の seq (setupMem が設定)
	coldStats        struct{ fell, forwarded, fallback, noSample, waitNs atomic.Int64 }
	coldStatsStarted atomic.Bool
)

const (
	coldWaitHeader  = "X-Isureal-Cold-Wait"
	coldSpaceHeader = "X-Isureal-Cold-Space"
	coldHopHeader   = "X-Isureal-Cold-Hop" // = fwd secret。asia2 で「冷たい転送」を見分ける
)

type coldKey struct{}

type coldState struct {
	cancel context.CancelFunc
	fell   atomic.Bool
	peer   bool
	arrive int64 // unix ns
	seq    int64 // asia1: 到着時の seq (-1 = 未取得)
	space  string
}

// coldOnPG: QueryTracer から。印の付いた要求の最初の PG の文で ctx を取り消す。true なら barrier は不要。
func coldOnPG(ctx context.Context) bool {
	st, ok := ctx.Value(coldKey{}).(*coldState)
	if !ok {
		return false
	}
	if st.fell.CompareAndSwap(false, true) {
		st.cancel()
	}
	return true
}

func coldTarget(req *http.Request) bool {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return false
	}
	p := req.URL.Path
	if !strings.HasPrefix(p, "/api/") || strings.HasPrefix(p, "/api/images/") || p == "/api/notifications" || p == "/api/initialize" {
		return false
	}
	if v := req.Header.Get(coldHopHeader); v != "" {
		return false
	}
	if v := req.Header.Get(memRespOriginHeader); v != "" {
		return false
	}
	return true
}

// coldArrival: 到着時の目印を作る。
func coldArrival(peer bool) *coldState {
	st := &coldState{peer: peer, arrive: memNow().UnixNano(), seq: -1}
	if peer {
		return st
	}
	if storeWALOn && storeCurrentSeqFn != nil {
		st.seq, st.space = storeCurrentSeqFn(), "store"
	} else if coldPumpSeqFn != nil {
		st.seq, st.space = coldPumpSeqFn(), "pump"
	}
	return st
}

// coldResolve: 転送に載せる seq を決める (peer は標本を待つ。上限 coldSampleWait)。
func coldResolve(ctx context.Context, st *coldState) (int64, string, bool) {
	if st.seq >= 0 {
		return st.seq, st.space, true
	}
	if !st.peer {
		return 0, "", false
	}
	t := st.arrive + int64(memWMEps)
	wctx, cancel := context.WithTimeout(ctx, coldSampleWait)
	defer cancel()
	if storeWALOn && storePGPeer.has() {
		if s, ok := storePGPeer.first(wctx, t); ok {
			return s, "store", true
		}
		return 0, "", false
	}
	if s, ok := coldPumpWM.first(wctx, t); ok {
		return s, "pump", true
	}
	return 0, "", false
}

// has / first: peerPGState を「時刻 t 以降の最初の標本の seq」の検索に使う (PA は見ない)。
func (p *peerPGState) has() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.samples) > 0
}

func (p *peerPGState) first(ctx context.Context, t int64) (int64, bool) {
	for {
		p.mu.Lock()
		for _, s := range p.samples {
			if s.t >= t {
				p.mu.Unlock()
				return s.s, true
			}
		}
		if p.ch == nil {
			p.ch = make(chan struct{})
		}
		ch := p.ch
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, false
		}
	}
}

// coldForward: asia2 へ転送して応答を書く。false なら呼び出し側がローカルで走らせ直す。
func coldForward(c *echo.Context, seq int64, space string) bool {
	in := c.Request()
	fctx, fcancel := context.WithTimeout(in.Context(), coldForwardMax) // 上限を超えたら自分の経路で走らせ直す
	defer fcancel()
	req, err := http.NewRequestWithContext(fctx, in.Method, "http://"+coldOrigin+in.URL.RequestURI(), nil)
	if err != nil {
		return false
	}
	for k, vs := range in.Header {
		switch http.CanonicalHeaderKey(k) {
		case "Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "Upgrade", "Te", "Trailer", "Content-Length":
			continue
		}
		req.Header[k] = append([]string(nil), vs...)
	}
	req.Header.Set(coldHopHeader, forwardSecret)
	req.Header.Set(coldWaitHeader, strconv.FormatInt(seq, 10))
	req.Header.Set(coldSpaceHeader, space)
	if region := os.Getenv("ISUREAL_REGION"); region != "" && region != "asia" && in.Header.Get(forwardedRegionHeader) == "" {
		req.Header.Set(forwardedRegionHeader, region)
		req.Header.Set(forwardedSecretHeader, forwardSecret)
	}
	start := time.Now()
	resp, err := coldOriginClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode >= 500 {
		return false
	}
	coldStats.waitNs.Add(int64(time.Since(start)))
	out := c.Response().Header()
	for k, vs := range resp.Header {
		switch http.CanonicalHeaderKey(k) {
		case "Connection", "Keep-Alive", "Transfer-Encoding", "Content-Length", "Date":
			continue
		}
		out[k] = append([]string(nil), vs...)
	}
	c.Response().WriteHeader(resp.StatusCode)
	if in.Method != http.MethodHead {
		_, _ = c.Response().Write(body)
	}
	return true
}

// coldMiddleware: setupMem から (pump / peer)。
func coldMiddleware(peer bool) echo.MiddlewareFunc {
	if coldStatsStarted.CompareAndSwap(false, true) {
		slog.Info("cold-origin: on", "origin", coldOrigin)
		go coldStatsLoop()
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			in := c.Request()
			if !coldTarget(in) || forwardSecret == "" {
				return next(c)
			}
			if !coldOriginGate() {
				// 冷たい読みのノードが未準備 / 遅れている: 取り消し付きの試し走りも二度走りもせず、COLD_ORIGIN 無しと同じ経路 (mem_full.go)
				return next(c)
			}
			st := coldArrival(peer)
			ctx, cancel := context.WithCancel(in.Context())
			defer cancel()
			st.cancel = cancel
			orig := c.Response()
			bw := &memBufferedWriter{header: http.Header{}}
			c.SetRequest(in.WithContext(context.WithValue(ctx, coldKey{}, st)))
			c.SetResponse(bw)
			err := next(c)
			c.SetResponse(orig)
			c.SetRequest(in)
			if !st.fell.Load() {
				for k, vs := range bw.header {
					orig.Header()[k] = vs
				}
				if bw.code != 0 {
					orig.WriteHeader(bw.code)
					_, _ = orig.Write(bw.buf.Bytes())
				}
				return err
			}
			coldStats.fell.Add(1)
			if !coldOriginGate() { // 冷たい読みのノードが未準備 / 遅れている: 待たず・送らず自分の経路へ (mem_full.go)
				coldStats.fallback.Add(1)
				return next(c)
			}
			seq, space, ok := coldResolve(in.Context(), st)
			if !ok {
				coldStats.noSample.Add(1)
			} else if coldForward(c, seq, space) {
				coldStats.forwarded.Add(1)
				return nil
			}
			coldStats.fallback.Add(1)
			// 今までどおり自分の PG の経路 (ISUREAL_STORE_WAL の barrier 付き) で走らせ直す
			return next(c)
		}
	}
}

// coldIsHop: asia2 側で使う (この要求が冷たい転送か)。
func coldIsHop(req *http.Request) bool {
	v := req.Header.Get(coldHopHeader)
	return v != "" && forwardSecret != "" && subtle.ConstantTimeCompare([]byte(v), []byte(forwardSecret)) == 1
}

func coldStatsLoop() {
	for range time.Tick(10 * time.Second) {
		fell := coldStats.fell.Swap(0)
		if fell == 0 {
			continue
		}
		fw := coldStats.forwarded.Swap(0)
		var avg time.Duration
		if fw > 0 {
			avg = time.Duration(coldStats.waitNs.Swap(0) / fw)
		}
		slog.Info("cold-origin", "fell", fell, "forwarded", fw, "avg_forward", avg,
			"fallback", coldStats.fallback.Swap(0), "no_sample", coldStats.noSample.Swap(0), "origin_not_usable", coldOriginSkipped.Swap(0))
	}
}
