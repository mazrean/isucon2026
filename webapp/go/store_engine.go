package isureal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

// ISUREAL_STORE=1 (asia1 = pump のときだけ有効): 書き込みをメモリ (memData) で検証・反映し、
// peer へ直接 push、PG へは束ねて書き戻す。設計は NOTES-store.md。

var (
	storeEnabled = os.Getenv("ISUREAL_STORE") == "1"
	storeAwaitPG = os.Getenv("ISUREAL_STORE_AWAIT_PG") != "0"
)

// errStoreFallback: store では判定できない (ホット外 / 未ロード)。元の DB 経路で処理する。
var errStoreFallback = errors.New("store: fallback")

const storeHandledKey = "storeHandled"

type storeOp struct {
	Lane int
	SQL  string
	Args []any
}

type storeEngine struct {
	p  *memPump
	m  *memStore
	db *sqlx.DB // write-behind 専用

	mu sync.Mutex // 書き込みの直列化 (initialize もこれを取る)

	// ヒント用の索引 (memData が正。使う前に memData で確認する)
	idxD      *memData
	comments  map[string]string // comment id -> post id
	realmojis map[string]string // realmoji id -> post id
	requests  map[string]*memReq
	finalized map[string]memFriendRequestRow // store が確定させた申請 (403/404 をメモリで返す)

	stats sync.Map // name -> *[2]atomic.Int64

	// PG 書き戻しのレーン: 0 = 画像以外すべて (1 本で順序を保つ)、1..N = image_objects (画像 ID でシャード、各 1 本)。
	// 画像の INSERT/DELETE は同じ ID なら同じレーンなので順序が保たれる。大きい画像が小さい書き込みを待たせない。
	lanes []*storeLane
}

type storeLane struct {
	mu         sync.Mutex
	cond       *sync.Cond
	queue      []storeOp
	queuedSeq  uint64
	flushedSeq uint64
}

// storeSeqs[i] = レーン i で待つべき seq (0 = 待たない)
type storeSeqs []uint64

var storeImageLanes = func() int {
	if n, err := strconv.Atoi(os.Getenv("ISUREAL_STORE_IMG_LANES")); err == nil && n > 0 {
		return n
	}
	return 4
}()

var storeCurrent *storeEngine // テスト用

func setupStore(h *HandlerContext) {
	if !storeEnabled || h.memPump == nil {
		return
	}
	db, err := sqlx.Open("pgx", h.Config.DatabaseDSN())
	if err != nil {
		slog.Error("store: open db failed; store disabled", "error", err)
		return
	}
	configurePool(db, 2+storeImageLanes)
	e := &storeEngine{p: h.memPump, m: h.memPump.m, db: db}
	for i := 0; i <= storeImageLanes; i++ {
		l := &storeLane{}
		l.cond = sync.NewCond(&l.mu)
		e.lanes = append(e.lanes, l)
		go e.flusher(l)
	}
	h.Store = e
	h.memPump.preReload = e.syncPG
	storeCurrent = e
	slog.Info("store: enabled", "await_pg", storeAwaitPG, "image_lanes", storeImageLanes)
}

type storeTx struct {
	e       *storeEngine
	d       *memData
	now     time.Time
	changes []memChange
	ops     []storeOp
	after   []func() // コミット後 (ロック外・バリア待ちの前) に実行
}

func storeNow() time.Time { return time.Now().Truncate(time.Microsecond).In(time.Local) }

// emit はメモリに即時反映し、peer へ送る変更として記録する (row はトリガーの to_jsonb と同じ形)。
func (t *storeTx) emit(tbl, op string, row any) {
	b, err := json.Marshal(row)
	if err != nil {
		panic(err)
	}
	ch := memChange{Tbl: tbl, Op: op, Row: b}
	if err := t.d.applyChange(ch); err != nil {
		panic(err)
	}
	t.changes = append(t.changes, ch)
	switch r := row.(type) {
	case memCommentRow:
		if r.DeletedAt == nil {
			t.e.comments[r.ID] = r.PostID
		} else {
			delete(t.e.comments, r.ID)
		}
	case memRealMojiRow:
		if r.DeletedAt == nil && r.ReplacedAt == nil {
			t.e.realmojis[r.ID] = r.PostID
		} else {
			delete(t.e.realmojis, r.ID)
		}
	case memFriendRequestRow:
		if r.Status == "pending" {
			t.e.requests[r.ID] = t.d.users[r.RequesterID].outgoing[r.ID]
		} else {
			delete(t.e.requests, r.ID)
			t.e.finalized[r.ID] = r
		}
	}
}

func (t *storeTx) exec(sql string, args ...any) {
	t.ops = append(t.ops, storeOp{SQL: sql, Args: args})
}

// execImage は image_objects の文 (画像 ID のレーンへ)。
func (t *storeTx) execImage(imageID, sql string, args ...any) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(imageID))
	t.ops = append(t.ops, storeOp{Lane: 1 + int(h.Sum32()%uint32(storeImageLanes)), SQL: sql, Args: args})
}

func (e *storeEngine) ensureIndex(d *memData) {
	if e.idxD == d {
		return
	}
	e.idxD = d
	e.comments = map[string]string{}
	e.realmojis = map[string]string{}
	e.requests = map[string]*memReq{}
	e.finalized = map[string]memFriendRequestRow{}
	for _, p := range d.posts {
		for _, c := range p.comments {
			e.comments[c.ID] = p.ID
		}
		for _, r := range p.realmojis {
			e.realmojis[r.ID] = p.ID
		}
	}
	for _, u := range d.users {
		for id, r := range u.outgoing {
			e.requests[id] = r
		}
	}
}

// write は fn を排他の中で実行する。fn はエラーを返すなら変更を emit する前に返すこと。
// 戻り値の seq を commitWait に渡す。errStoreFallback なら元の DB 経路へ。
func (e *storeEngine) write(fn func(t *storeTx) error) (storeSeqs, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.p
	p.cycleMu.Lock()
	defer p.cycleMu.Unlock()
	if !p.m.ready.Load() || p.loadedGen != p.wantGen.Load() {
		return nil, errStoreFallback
	}
	m := p.m
	m.mu.Lock()
	e.ensureIndex(m.d)
	t := &storeTx{e: e, d: m.d, now: storeNow()}
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("store: panic: %v", r)
			}
		}()
		return fn(t)
	}()
	if t.d.sortedDirty {
		t.d.sortedUsers()
	}
	m.mu.Unlock()
	if err != nil && len(t.changes) > 0 {
		// 途中まで反映してしまった: メモリを PG から作り直す
		slog.Error("store: partial write; reloading", "error", err)
		e.enqueue(nil)
		p.invalidate()
		return nil, err
	}
	if len(t.changes) == 0 && len(t.ops) == 0 {
		return nil, err
	}
	if p.peers != nil && len(t.changes) > 0 {
		p.peers.publish(t.changes)
	}
	return e.enqueue(t.ops), err
}

// enqueue は e.mu 保持中に呼ばれる (各レーン内の順序 = 書き込み順)。レーン 0 には常に 1 つ積む。
func (e *storeEngine) enqueue(ops []storeOp) storeSeqs {
	seqs := make(storeSeqs, len(e.lanes))
	for i, l := range e.lanes {
		var mine []storeOp
		for _, op := range ops {
			if op.Lane == i {
				mine = append(mine, op)
			}
		}
		if i != 0 && len(mine) == 0 {
			continue
		}
		l.mu.Lock()
		l.queue = append(l.queue, mine...)
		l.queuedSeq++
		seqs[i] = l.queuedSeq
		l.cond.Broadcast()
		l.mu.Unlock()
	}
	return seqs
}

func (e *storeEngine) waitFlushed(seqs storeSeqs) {
	for i, seq := range seqs {
		if seq == 0 {
			continue
		}
		l := e.lanes[i]
		l.mu.Lock()
		for l.flushedSeq < seq {
			l.cond.Wait()
		}
		l.mu.Unlock()
	}
}

// syncPG は現在までの書き込みが PG に入るのを待つ (DB 経路へのフォールバック前)。
func (e *storeEngine) syncPG() {
	seqs := make(storeSeqs, len(e.lanes))
	for i, l := range e.lanes {
		l.mu.Lock()
		seqs[i] = l.queuedSeq
		l.mu.Unlock()
	}
	e.waitFlushed(seqs)
}

// commitWait: 書き込み seq が全ホストで読めるようになるまで待つ。memWriteBarrier の pump バリアは省略させる。
func (e *storeEngine) commitWait(c *echo.Context, seqs storeSeqs) {
	c.Set(storeHandledKey, true)
	if seqs == nil {
		return
	}
	if storeAwaitPG {
		e.waitFlushed(seqs)
	}
	if e.p.peers != nil && !localOnlyWrite(c.Request()) {
		e.p.peers.waitAcked()
	}
}

func (e *storeEngine) flusher(l *storeLane) {
	for {
		l.mu.Lock()
		for l.flushedSeq == l.queuedSeq {
			l.cond.Wait()
		}
		ops := l.queue
		l.queue = nil
		upto := l.queuedSeq
		l.mu.Unlock()

		if len(ops) > 0 {
			e.flushOps(ops)
		}

		l.mu.Lock()
		l.flushedSeq = upto
		l.cond.Broadcast()
		l.mu.Unlock()
	}
}

// flushOps: 接続系の失敗 (PG に届かない) は成功するまで再試行する (その間 書き込みの応答は止まる = 元実装と同じく
// PG なしでは応答しない)。文そのものの失敗 (PgError) だけ 1 文ずつに分け、失敗した文はログを出して捨てる。
func (e *storeEngine) flushOps(ops []storeOp) {
	backoff := 50 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := e.runBatch(ops)
		if err == nil {
			return
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			slog.Error("store: write-behind batch rejected; retrying op by op", "error", err, "ops", len(ops))
			break
		}
		slog.Error("store: write-behind batch failed; retrying", "error", err, "ops", len(ops), "attempt", attempt)
		time.Sleep(backoff)
		backoff = min(backoff*2, time.Second)
	}
	for _, op := range ops {
		backoff := 50 * time.Millisecond
		for {
			err := e.runBatch([]storeOp{op})
			if err == nil {
				break
			}
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				slog.Error("store: write-behind op dropped", "error", err, "sql", op.SQL)
				break
			}
			time.Sleep(backoff)
			backoff = min(backoff*2, time.Second)
		}
	}
}

func (e *storeEngine) runBatch(ops []storeOp) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(dc any) error {
		pc := dc.(*stdlib.Conn).Conn()
		b := &pgx.Batch{}
		b.Queue("BEGIN")
		// トリガー (mem_changelog) と FK を止める: 検証は store 側で済んでおり、pump に二重適用させない
		b.Queue("SET LOCAL session_replication_role = replica")
		for _, op := range ops {
			b.Queue(op.SQL, op.Args...)
		}
		b.Queue("COMMIT")
		err := pc.SendBatch(ctx, b).Close()
		if err != nil {
			_, _ = pc.Exec(ctx, "ROLLBACK")
		}
		return err
	})
}

// ---- helpers for handlers ----

func strPtr(s string) *string { return &s }

func timePtr(t time.Time) *time.Time { return &t }
