package isureal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
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

	wbMu       sync.Mutex
	wbCond     *sync.Cond
	queue      []storeOp
	queuedSeq  uint64
	flushedSeq uint64
}

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
	configurePool(db, 2)
	e := &storeEngine{p: h.memPump, m: h.memPump.m, db: db}
	e.wbCond = sync.NewCond(&e.wbMu)
	h.Store = e
	storeCurrent = e
	go e.flusher()
	slog.Info("store: enabled", "await_pg", storeAwaitPG)
}

type storeTx struct {
	e       *storeEngine
	d       *memData
	now     time.Time
	changes []memChange
	ops     []storeOp
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
		}
	}
}

func (t *storeTx) exec(sql string, args ...any) {
	t.ops = append(t.ops, storeOp{SQL: sql, Args: args})
}

func (e *storeEngine) ensureIndex(d *memData) {
	if e.idxD == d {
		return
	}
	e.idxD = d
	e.comments = map[string]string{}
	e.realmojis = map[string]string{}
	e.requests = map[string]*memReq{}
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
func (e *storeEngine) write(fn func(t *storeTx) error) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.p
	p.cycleMu.Lock()
	defer p.cycleMu.Unlock()
	if !p.m.ready.Load() || p.loadedGen != p.wantGen.Load() {
		return 0, errStoreFallback
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
		return 0, err
	}
	if len(t.changes) == 0 && len(t.ops) == 0 {
		return 0, err
	}
	if p.peers != nil && len(t.changes) > 0 {
		p.peers.publish(t.changes)
	}
	return e.enqueue(t.ops), err
}

func (e *storeEngine) enqueue(ops []storeOp) uint64 {
	e.wbMu.Lock()
	defer e.wbMu.Unlock()
	e.queue = append(e.queue, ops...)
	e.queuedSeq++
	e.wbCond.Broadcast()
	return e.queuedSeq
}

func (e *storeEngine) waitFlushed(seq uint64) {
	e.wbMu.Lock()
	for e.flushedSeq < seq {
		e.wbCond.Wait()
	}
	e.wbMu.Unlock()
}

// syncPG は現在までの書き込みが PG に入るのを待つ (DB 経路へのフォールバック前)。
func (e *storeEngine) syncPG() {
	e.wbMu.Lock()
	seq := e.queuedSeq
	e.wbMu.Unlock()
	e.waitFlushed(seq)
}

// commitWait: 書き込み seq が全ホストで読めるようになるまで待つ。memWriteBarrier の pump バリアは省略させる。
func (e *storeEngine) commitWait(c *echo.Context, seq uint64) {
	c.Set(storeHandledKey, true)
	if seq == 0 {
		return
	}
	if storeAwaitPG {
		e.waitFlushed(seq)
	}
	if e.p.peers != nil {
		e.p.peers.waitAcked()
	}
}

func (e *storeEngine) flusher() {
	for {
		e.wbMu.Lock()
		for e.flushedSeq == e.queuedSeq {
			e.wbCond.Wait()
		}
		ops := e.queue
		e.queue = nil
		upto := e.queuedSeq
		e.wbMu.Unlock()

		if len(ops) > 0 {
			e.flushOps(ops)
		}

		e.wbMu.Lock()
		e.flushedSeq = upto
		e.wbCond.Broadcast()
		e.wbMu.Unlock()
	}
}

func (e *storeEngine) flushOps(ops []storeOp) {
	for attempt := 0; attempt < 3; attempt++ {
		err := e.runBatch(ops)
		if err == nil {
			return
		}
		slog.Error("store: write-behind batch failed", "error", err, "ops", len(ops), "attempt", attempt)
		time.Sleep(time.Duration(attempt+1) * 20 * time.Millisecond)
	}
	// 1 文ずつ (失敗した文は捨てる)
	for _, op := range ops {
		if err := e.runBatch([]storeOp{op}); err != nil {
			slog.Error("store: write-behind op dropped", "error", err, "sql", op.SQL)
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
