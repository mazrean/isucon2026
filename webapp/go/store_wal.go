package isureal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
)

// ISUREAL_STORE_WAL=/dir (既定 空 = 無効): store の書き込みバッチの PG 文をローカル WAL に追記し、
// group sync (ASIA_LOCALIMG と同じ groupSyncer) の後に書き手を解放する。有効なときだけ AWAIT_PG=0 を許す
// (応答 2xx = WAL fsync 済み ∧ 全 peer ack。PG への書き戻しは純粋な write-behind)。
// 画像レーン (image_objects、バイト列を含む) は WAL に入れず、従来どおり PG の flush を待つ。
// 回復: 起動時 (pump のロード前) に WAL を読み、レーンごとに store_wal_applied.seq より後の文を PG に流し直す。
// 設計は NOTES-store.md。

var storeWALDir = os.Getenv("ISUREAL_STORE_WAL")

// storePGApplied: この seq までの store の書き込みは PG に入っている (AWAIT_PG=0 のときだけ進む)。
var storePGApplied atomic.Int64

var storeSeqNow atomic.Int64

// storeCurrentSeq: これまでに受け付けた store の書き込みバッチの seq。
func storeCurrentSeq() int64 { return storeSeqNow.Load() }

// storePGBarrier: seq までの書き込みが PG に入るのを待つ。AWAIT_PG=1 (応答前に PG 済み) なら即座に返す。
func storePGBarrier(ctx context.Context, seq int64) error {
	e := storeCurrent
	if storeAwaitPG || e == nil {
		return nil
	}
	for {
		if storePGApplied.Load() >= seq {
			return nil
		}
		e.pendMu.Lock()
		ch := e.pgCh
		e.pendMu.Unlock()
		if storePGApplied.Load() >= seq {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type storePend struct {
	g    int64
	seqs storeSeqs
}

// trackPG は enqueue (e.mu 保持中) の後に呼ぶ。
func (e *storeEngine) trackPG(g int64, seqs storeSeqs) {
	if storeAwaitPG {
		return
	}
	e.pendMu.Lock()
	e.pend = append(e.pend, storePend{g: g, seqs: seqs})
	e.pendMu.Unlock()
	e.advancePG()
}

func (e *storeEngine) advancePG() {
	if storeAwaitPG {
		return
	}
	e.pendMu.Lock()
	defer e.pendMu.Unlock()
	moved := false
	for len(e.pend) > 0 {
		p := e.pend[0]
		ok := true
		for i, s := range p.seqs {
			if s == 0 {
				continue
			}
			l := e.lanes[i]
			l.mu.Lock()
			f := l.flushedSeq
			l.mu.Unlock()
			if f < s {
				ok = false
				break
			}
		}
		if !ok {
			break
		}
		storePGApplied.Store(p.g)
		e.pend[0] = storePend{}
		e.pend = e.pend[1:]
		moved = true
	}
	if len(e.pend) == 0 {
		e.pend = nil
	}
	if moved {
		close(e.pgCh)
		e.pgCh = make(chan struct{})
	}
}

func isImageLane(i int) bool { return i >= 1 && i <= storeImageLanes }

// ---- WAL file ----

type storeWAL struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	syncer *groupSyncer
}

type walOp struct {
	L int    `json:"l"`
	S string `json:"s"`
	A []any  `json:"a"`
}

type walRec struct {
	G int64   `json:"g"`
	O []walOp `json:"o"`
}

func walPath(dir string) string { return filepath.Join(dir, "store.wal") }

func openStoreWAL(dir string) (*storeWAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := walPath(dir)
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &storeWAL{f: f, path: p, syncer: &groupSyncer{dir: func() string { return dir }}}, nil
}

// append は e.mu 保持中 (= 書き込み順) に呼ぶ。fsync はしない (wait で group sync)。
// 戻り値 false = WAL に入れる文がない。
func (w *storeWAL) append(g int64, ops []storeOp) (bool, error) {
	rec := walRec{G: g}
	for _, op := range ops {
		if isImageLane(op.Lane) {
			continue
		}
		rec.O = append(rec.O, walOp{L: op.Lane, S: op.SQL, A: op.Args})
	}
	if len(rec.O) == 0 {
		return false, nil
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return false, err
	}
	b = append(b, '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err = w.f.Write(b)
	return true, err
}

func (w *storeWAL) wait() error { return w.syncer.wait(w.path) }

// truncate: WAL を空にして永続化する (initialize / 回復の後)。
func (w *storeWAL) truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	return w.f.Sync()
}

const storeWALTableDDL = `CREATE TABLE IF NOT EXISTS store_wal_applied (lane int PRIMARY KEY, seq bigint NOT NULL)`

func walDecodeArg(v any) any {
	switch x := v.(type) {
	case json.Number:
		return string(x)
	case []any:
		out := make([]string, 0, len(x))
		for _, y := range x {
			s, ok := y.(string)
			if !ok {
				if n, ok := y.(json.Number); ok {
					s = string(n)
				} else {
					b, _ := json.Marshal(y)
					s = string(b)
				}
			}
			out = append(out, s)
		}
		return out
	default:
		return v
	}
}

// replaySQL: 再送しても結果が変わらないよう INSERT は重複を無視する (UPDATE/DELETE は ID 指定)。
func replaySQL(s string) string {
	t := strings.TrimSpace(s)
	if strings.HasPrefix(strings.ToUpper(t), "INSERT") && !strings.Contains(strings.ToUpper(t), "ON CONFLICT") {
		return t + " ON CONFLICT DO NOTHING"
	}
	return s
}

// storeWALRecover は pump のロード前に呼ぶ: WAL の未適用分を PG に流し、WAL と適用位置を空にする。
func storeWALRecover(dsn string) {
	if storeWALDir == "" {
		return
	}
	start := time.Now()
	db, err := sqlx.Open("pgx", dsn)
	if err != nil {
		slog.Error("store wal: open db failed", "error", err)
		return
	}
	defer db.Close()
	configurePool(db, 2)
	ctx := context.Background()
	for {
		if _, err = db.ExecContext(ctx, storeWALTableDDL); err == nil {
			break
		}
		slog.Error("store wal: create table failed; retrying", "error", err)
		time.Sleep(time.Second)
	}
	applied := map[int]int64{}
	rows, err := db.QueryContext(ctx, `SELECT lane, seq FROM store_wal_applied`)
	if err == nil {
		for rows.Next() {
			var l int
			var s int64
			if rows.Scan(&l, &s) == nil {
				applied[l] = s
			}
		}
		rows.Close()
	}
	e := &storeEngine{db: db}
	f, err := os.Open(walPath(storeWALDir))
	replayed, records := 0, 0
	if err == nil {
		buf := map[int][]storeOp{}
		bufG := map[int]int64{}
		n := 0
		flush := func() {
			for l, ops := range buf {
				e.flushOps(l, bufG[l], ops)
			}
			buf = map[int][]storeOp{}
			n = 0
		}
		dec := json.NewDecoder(f)
		dec.UseNumber()
		for {
			var rec walRec
			if err := dec.Decode(&rec); err != nil {
				if !errors.Is(err, io.EOF) {
					slog.Warn("store wal: torn tail ignored", "error", err)
				}
				break
			}
			records++
			for _, op := range rec.O {
				if rec.G <= applied[op.L] {
					continue
				}
				args := make([]any, len(op.A))
				for i, a := range op.A {
					args[i] = walDecodeArg(a)
				}
				buf[op.L] = append(buf[op.L], storeOp{Lane: op.L, SQL: replaySQL(op.S), Args: args})
				bufG[op.L] = rec.G
				n++
				replayed++
			}
			if n >= 2000 {
				flush()
			}
		}
		flush()
		f.Close()
	}
	// WAL を空にしてから適用位置を消す (逆順だと、途中で落ちたとき古い WAL を全部流し直してしまう)
	if w, err := openStoreWAL(storeWALDir); err == nil {
		if err := w.truncate(); err != nil {
			slog.Error("store wal: truncate failed", "error", err)
		}
		w.f.Close()
	} else {
		slog.Error("store wal: open failed", "error", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM store_wal_applied`); err != nil {
		slog.Error("store wal: reset applied failed", "error", err)
	}
	slog.Info("store wal: recovered", "records", records, "replayed_ops", replayed, "elapsed", time.Since(start))
}

// storeWALAfterInit: initialize の init.sh の後 (表を作り直し、適用位置を空にする)。
func (e *storeEngine) walAfterInit(ctx context.Context) {
	if e.wal == nil {
		return
	}
	if _, err := e.db.ExecContext(ctx, storeWALTableDDL); err != nil {
		slog.Error("store wal: create table failed", "error", err)
	}
	if _, err := e.db.ExecContext(ctx, `DELETE FROM store_wal_applied`); err != nil {
		slog.Error("store wal: reset applied failed", "error", err)
	}
}

// walBeforeInit: initialize の init.sh の前 (書き戻しは出し切ってある)。
func (e *storeEngine) walBeforeInit() {
	if e.wal == nil {
		return
	}
	if err := e.wal.truncate(); err != nil {
		slog.Error("store wal: truncate failed", "error", err)
	}
}
