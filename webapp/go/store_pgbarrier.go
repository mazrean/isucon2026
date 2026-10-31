package isureal

import (
	"context"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_STORE_WAL (空でないときだけ有効。既定は挙動を変えない): STORE=1 の PG が純粋な write-behind
// (AWAIT_PG=0) になったときの RYW の穴を塞ぐ。
//
// 規則 (slack 0): PG を読む要求は、到着時刻 T_r より前に応答した書き込みが PG に入るまで、最初の PG の文を待つ。
//   - asia1 (store の正本): 到着時に S_r = storeCurrentSeq() を取り、PG の最初の文の前に storePGBarrier(ctx, S_r)。
//     T_r より前に応答した書き込みは、応答の前に seq を得ているので S_r 以下。
//   - eu/am (peer の DB フォールバック): asia1 は push / wm 通知に標本 (PT, PS, PA) を載せる。
//     PT = asia1 の時刻、PS = PT の後に読んだ storeCurrentSeq()、PA = storePGApplied。
//     peer は PT >= T_r + ε の最初の標本の PS を目標にし (WM と同じ意味論: T_r より前に応答した書き込みの seq は PS 以下)、
//     PA >= PS を見てから PG を読む。時計のずれは ε (ISUREAL_MEM_WM_EPS_MS) が吸収する (WM と同じ)。
//   - 待つ場所は pgx の QueryTracer (traceDSN が全プールに付ける)。store の書き戻しのプール ("store") は除く。
//     メモリで答える要求は PG を読まないので待たない。待ちは要求ごとに 1 回 (成功したら以後の文は素通り)。
//   - バックグラウンドの読み (通知 hub の poll) は storePGReadCtx で、開始時点を到着時刻として同じ待ちをする。
//   - 上限 ISUREAL_STORE_WAL_BARRIER_MAX_MS (既定 3000) を超えたら待たずに読む (止めない。統計に数える)。
//
// store_engine.go の WAL 担当が公開する storePGBarrier / storeCurrentSeq / storePGApplied は、下のフックに繋ぐ
// (このファイルは名前を直接参照しない。nil の間は何もしない)。
var (
	storeWALOn          = os.Getenv("ISUREAL_STORE_WAL") != ""
	storeWALBarrierMax  = time.Duration(envIntOrDefault("ISUREAL_STORE_WAL_BARRIER_MAX_MS", 3000)) * time.Millisecond
	storePGBarrierFn    func(ctx context.Context, seq int64) error
	storeCurrentSeqFn   func() int64
	storePGAppliedFn    func() int64
	storePGRolePeer     atomic.Bool // この process が peer (eu/am) か
	storePGPeer         = newPeerPGState()
	storePGBarrierStats struct{ waits, timeouts atomic.Int64 }
)

type pgBarrierKey struct{}

type pgBarrierTarget struct {
	peer bool
	seq  int64 // asia1: 到着時の storeCurrentSeq()
	at   int64 // peer: 到着時刻 (unix ns)
	done atomic.Bool
}

// storePGActiveAsia: asia1 で待ちが有効か。
func storePGActiveAsia() bool {
	return storeWALOn && storePGBarrierFn != nil && storeCurrentSeqFn != nil
}

// storePGReadCtx: 今を到着時刻として、PG の読みの目標を ctx に付ける。無効なら ctx をそのまま返す。
func storePGReadCtx(ctx context.Context) context.Context {
	if !storeWALOn {
		return ctx
	}
	if storePGRolePeer.Load() {
		return context.WithValue(ctx, pgBarrierKey{}, &pgBarrierTarget{peer: true, at: memNow().UnixNano()})
	}
	if !storePGActiveAsia() {
		return ctx
	}
	return context.WithValue(ctx, pgBarrierKey{}, &pgBarrierTarget{seq: storeCurrentSeqFn()})
}

// storePGReadMiddleware: /api/* の要求に到着時の目標を付ける (GET 以外も: フォールバックの書き込みが PG を読む)。
func storePGReadMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		req := c.Request()
		if strings.HasPrefix(req.URL.Path, "/api/") && req.URL.Path != "/api/initialize" {
			c.SetRequest(req.WithContext(storePGReadCtx(req.Context())))
		}
		return next(c)
	}
}

// pgBarrierWait: QueryTracer から呼ぶ。目標があれば、PG がそこまで反映するのを待つ (要求ごとに 1 回)。
func pgBarrierWait(ctx context.Context) {
	t, ok := ctx.Value(pgBarrierKey{}).(*pgBarrierTarget)
	if !ok || t.done.Load() {
		return
	}
	wctx, cancel := context.WithTimeout(ctx, storeWALBarrierMax)
	defer cancel()
	storePGBarrierStats.waits.Add(1)
	var err error
	if t.peer {
		_, err = storePGPeer.wait(wctx, t.at+int64(memWMEps))
	} else if fn := storePGBarrierFn; fn != nil {
		err = fn(wctx, t.seq)
	}
	if err != nil {
		storePGBarrierStats.timeouts.Add(1)
		return
	}
	t.done.Store(true)
}

// ---- asia1 → peer の標本 ----

// storePGSample: push / wm 通知に載せる (PT, PS, PA)。無効なら 0。AWAIT_PG=1 (PG は応答前に反映済み) なら送らない。
func storePGSample() (pt, ps, pa int64) {
	if !storePGActiveAsia() || storePGAppliedFn == nil || storeAwaitPG {
		return 0, 0, 0
	}
	pt = memNow().UnixNano() // 時刻を先に取り、その後で seq を読む (PS は PT までに応答した書き込みを含む)
	ps = storeCurrentSeqFn()
	pa = storePGAppliedFn()
	return
}

type peerPGSample struct{ t, s int64 }

type peerPGState struct {
	mu      sync.Mutex
	samples []peerPGSample // t の昇順 (s も単調)
	pa      int64
	ch      chan struct{}
}

func newPeerPGState() *peerPGState { return &peerPGState{} }

// update: asia1 から届いた標本を入れる (到着順は入れ替わりうる)。
func (p *peerPGState) update(pt, ps, pa int64) {
	if pt <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.samples); n > 0 && pt > p.samples[n-1].t && ps < p.samples[n-1].s {
		p.samples, p.pa = nil, 0 // asia1 の seq の系列が変わった (再起動): 古い PA を信じない
	}
	if pa > p.pa {
		p.pa = pa
	}
	i, found := slices.BinarySearchFunc(p.samples, pt, func(s peerPGSample, t int64) int { return cmpInt64(s.t, t) })
	if !found {
		p.samples = slices.Insert(p.samples, i, peerPGSample{t: pt, s: ps})
	}
	if len(p.samples) > 512 {
		p.samples = append([]peerPGSample(nil), p.samples[len(p.samples)-256:]...)
	}
	if p.ch != nil {
		close(p.ch)
		p.ch = nil
	}
}

// reset: epoch 切り替え・resync で asia1 の seq の系列が変わりうるので捨てる。
func (p *peerPGState) reset() {
	p.mu.Lock()
	p.samples, p.pa = nil, 0
	p.mu.Unlock()
}

// wait: t 以降の標本の PS を目標に、PA >= PS まで待つ。標本を 1 つも受けていなければ (asia1 で無効) 待たない。
func (p *peerPGState) wait(ctx context.Context, t int64) (int64, error) {
	for {
		p.mu.Lock()
		if len(p.samples) == 0 {
			p.mu.Unlock()
			return 0, nil
		}
		target := int64(-1)
		if i, _ := slices.BinarySearchFunc(p.samples, t, func(s peerPGSample, t int64) int { return cmpInt64(s.t, t) }); i < len(p.samples) {
			target = p.samples[i].s
		}
		if target >= 0 && p.pa >= target {
			p.mu.Unlock()
			return target, nil
		}
		if p.ch == nil {
			p.ch = make(chan struct{})
		}
		ch := p.ch
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// setupStorePGBarrier: setupMem から呼ぶ。
func setupStorePGBarrier(e *echo.Echo, peer bool) {
	if !storeWALOn {
		return
	}
	storePGRolePeer.Store(peer)
	e.Use(storePGReadMiddleware)
}
