package isureal

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

// 論理レプリケーション版メモリモデル (ISUREAL_MEM_LOGREPL=1, 既定 off)。
//
// 各 app が自分用の論理スロット (= application_name = ノード名) から pgoutput で
// publication isureal_mem (memTriggerTables の 10 表) を購読し、memData に反映する。
//   - 初回: CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT のスナップショットで DB からロード。
//   - 再起動: ローカルのスナップショットファイル (dump + LSN) から復元し、スロットの続きを受け取る。
//     flush 位置 (= confirmed_flush_lsn) はファイルに保存済みの LSN しか返さないので、kill -9 でも欠けない。
//   - initialize: reset.sql の TRUNCATE / DELETE もストリームで届く (session_replication_role は無関係)。
//   - RYW: apply 位置 = メモリに反映済みの LSN。書き込みリクエストの最後に
//     synchronous_commit=remote_apply の空トランザクション (marker) を commit し、同期対象の全 app の反映を待つ。
//   - 同期対象の管理は asia2 の watchdog (mem_logrepl_watchdog.go)。外すときは
//     mem_logrepl_members.blocked=true → walsender を terminate → lease 以上待つ → synchronous_standby_names から外す。
//     app は「walsender が sync/quorum かつ blocked=false」を確認してから ready になり、
//     ready 中も受信が lrLease 途絶えたら DB 経路に戻る (terminate 後に古いメモリで答えない)。

var memLogReplEnabled = os.Getenv("ISUREAL_MEM_LOGREPL") == "1"

// ISUREAL_MEM_LOGREPL_SNAPSHOT=1: ローカルのスナップショットから再起動する (flush = 保存済み LSN)。
// flush を遅らせるので、synchronous_commit=on (remote flush 待ち) の commit は保存まで止まる。
// asia2 の既定 synchronous_commit が off/local のときだけ使うこと。既定 off (flush = apply、再起動時は COPY し直す)。
var memLogReplSnapshot = os.Getenv("ISUREAL_MEM_LOGREPL_SNAPSHOT") == "1"

const (
	lrPublication  = "isureal_mem"
	lrLease        = 1 * time.Second
	lrStatusEvery  = 100 * time.Millisecond
	lrCheckEvery   = 200 * time.Millisecond
	lrSaveEvery    = 60 * time.Second
	lrMembersTable = "mem_logrepl_members"
)

var lrSlotSanitize = regexp.MustCompile(`[^a-z0-9_]`)

func lrSlotName(config *Config) string {
	name := config.NodeName
	if v := os.Getenv("ISUREAL_MEM_LOGREPL_SLOT"); v != "" {
		name = v
	}
	if name == "" {
		name, _ = os.Hostname()
	}
	name = lrSlotSanitize.ReplaceAllString(strings.ToLower(name), "_")
	if name == "" {
		name = "isureal"
	}
	return name
}

type memLogRepl struct {
	m    *memStore
	db   *sqlx.DB // 通常接続 (BG プール): 初期ロード・同期状態確認
	cfg  *Config
	slot string
	dir  string

	applied atomic.Uint64 // メモリに反映済みの LSN
	saved   atomic.Uint64 // ローカルファイルに保存済みの LSN (= flush として返す)
	loaded  bool          // メモリが applied 時点の状態を持っている (再接続時はそこから続ける)

	pid       atomic.Int32 // 現在の walsender の pid
	connGen   atomic.Uint64
	insync    atomic.Bool // 直近の確認で sync/quorum かつ blocked=false
	target    atomic.Uint64
	saving    atomic.Bool
	lastSave  time.Time
	rels      map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
	saveMu    sync.Mutex
	stopped   atomic.Bool
	dsn       string
	needSave  bool
	lastFlush uint64
}

var memCurrentLogRepl []*memLogRepl // テスト用

func setupMemLogRepl(h *HandlerContext, e *echo.Echo) {
	m := &memStore{d: newMemData(), useLease: true}
	r := &memLogRepl{
		m: m, db: h.BG, cfg: h.Config, slot: lrSlotName(h.Config),
		dir:     envOrDefault("ISUREAL_MEM_LOGREPL_DIR", "/home/isucon/mem-logrepl"),
		rels:    map[uint32]*pglogrepl.RelationMessage{},
		typeMap: pgtype.NewMap(),
	}
	u, _ := url.Parse(h.Config.DatabaseDSN())
	q := u.Query()
	q.Set("replication", "database")
	q.Set("application_name", r.slot)
	u.RawQuery = q.Encode()
	r.dsn = u.String()
	h.Mem = m
	memCurrentLogRepl = append(memCurrentLogRepl, r)
	if err := ensureMemLogReplSchema(h.DB); err != nil {
		slog.Error("mem-logrepl: schema setup failed; mem disabled", "error", err)
		h.Mem = nil
		return
	}
	e.Use(memLogReplBarrier(h.DB))
	go r.run()
	go r.checkLoop()
}

// ensureMemLogReplSchema: publication / REPLICA IDENTITY FULL / members 表を冪等に用意する。
func ensureMemLogReplSchema(db *sqlx.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var ok bool
	err := db.GetContext(ctx, &ok, `
SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)
   AND (SELECT count(*) FROM pg_class WHERE relname = ANY($2) AND relkind = 'r' AND relreplident = 'f') = $3
   AND to_regclass($4) IS NOT NULL`, lrPublication, memTriggerTables, len(memTriggerTables), lrMembersTable)
	if err == nil && ok {
		return nil
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmts := []string{
		`SET LOCAL synchronous_commit = local`,
		`SELECT pg_advisory_xact_lock(727275)`,
		`CREATE TABLE IF NOT EXISTS ` + lrMembersTable + ` (name text PRIMARY KEY, blocked bool NOT NULL DEFAULT true)`,
	}
	for _, t := range memTriggerTables {
		stmts = append(stmts, `ALTER TABLE `+t+` REPLICA IDENTITY FULL`)
	}
	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	var n int
	if err := tx.GetContext(ctx, &n, `SELECT count(*) FROM pg_publication WHERE pubname = $1`, lrPublication); err != nil {
		return err
	}
	if n == 0 {
		if _, err := tx.ExecContext(ctx, `CREATE PUBLICATION `+lrPublication+` FOR TABLE `+strings.Join(memTriggerTables, ", ")); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ---- write barrier ----

// memLogReplBarrier: 非 GET の /api/* (initialize 含む) の後に remote_apply の marker を commit し、
// それまでの全コミットが同期対象の全 app のメモリに反映されるのを待ってから返す。
func memLogReplBarrier(db *sqlx.DB) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			if req.Method == http.MethodGet || req.Method == http.MethodHead || !strings.HasPrefix(req.URL.Path, "/api/") {
				return next(c)
			}
			orig := c.Response()
			bw := &memBufferedWriter{header: orig.Header()}
			c.SetResponse(bw)
			err := next(c)
			c.SetResponse(orig)
			var apiErr *APIError
			clientErr := (bw.code >= 400 && bw.code < 500) ||
				(err != nil && errors.As(err, &apiErr) && apiErr.Status >= 400 && apiErr.Status < 500)
			// login も待つ: 再ログインは古いセッションを無効化する (権限を減らす書き込み)。job10890 はこれで FAIL。
			if !clientErr {
				if werr := lrWaitApplied(req.Context(), db); werr != nil {
					slog.Warn("mem-logrepl: barrier failed", "error", werr)
				}
			}
			if bw.code != 0 {
				orig.WriteHeader(bw.code)
				_, _ = orig.Write(bw.buf.Bytes())
			}
			return err
		}
	}
}

// lrWaitApplied: WAL を書く (transactional な論理メッセージ) だけのトランザクションを remote_apply で commit する。
// WAL を書かないトランザクション (xid だけ) は commit が同期待ちにならないので不可。
// 行ロックを取らないので並行する marker 同士は待ち合わない。pgoutput は messages オプション無しなら送らない (空 tx → keepalive)。
func lrWaitApplied(ctx context.Context, db *sqlx.DB) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `BEGIN; SET LOCAL synchronous_commit = remote_apply; SELECT pg_logical_emit_message(true, 'isureal_mem', ''); COMMIT`)
	return err
}

// ---- subscriber ----

func (r *memLogRepl) run() {
	for !r.stopped.Load() {
		err := r.session()
		r.m.ready.Store(false)
		r.insync.Store(false)
		if r.stopped.Load() {
			return
		}
		slog.Warn("mem-logrepl: session ended; reconnecting", "slot", r.slot, "error", err)
		time.Sleep(500 * time.Millisecond)
	}
}

type lrSlotInfo struct {
	Exists  bool   `db:"exists"`
	Active  bool   `db:"active"`
	Flushed string `db:"flushed"`
}

func (r *memLogRepl) session() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := pgconn.Connect(ctx, r.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	res, err := conn.Exec(ctx, `SELECT pg_backend_pid()`).ReadAll()
	if err != nil {
		return err
	}
	if len(res) == 0 || len(res[0].Rows) == 0 {
		return fmt.Errorf("no pid")
	}
	pid, _ := strconv.Atoi(string(res[0].Rows[0][0]))

	var start pglogrepl.LSN
	if r.loaded {
		start = pglogrepl.LSN(r.applied.Load())
	} else {
		start, err = r.restoreOrCreate(ctx, conn)
		if err != nil {
			return err
		}
	}
	r.rels = map[uint32]*pglogrepl.RelationMessage{}
	if err := pglogrepl.StartReplication(ctx, conn, r.slot, start, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + lrPublication + "'"},
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	r.pid.Store(int32(pid))
	r.connGen.Add(1)
	r.target.Store(^uint64(0))
	slog.Info("mem-logrepl: streaming", "slot", r.slot, "start", start, "pid", pid)
	return r.stream(ctx, conn)
}

// restoreOrCreate はローカルファイルとスロットが揃っていればファイルから復元し、
// そうでなければスロットを作り直してエクスポートしたスナップショットで DB からロードする。
func (r *memLogRepl) restoreOrCreate(ctx context.Context, conn *pgconn.PgConn) (pglogrepl.LSN, error) {
	var si lrSlotInfo
	if err := r.db.GetContext(ctx, &si, `
SELECT true AS exists, active, coalesce(confirmed_flush_lsn::text, '0/0') AS flushed
FROM pg_replication_slots WHERE slot_name = $1 AND plugin = 'pgoutput' AND database = current_database()`, r.slot); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if si.Exists && memLogReplSnapshot {
		flushed, _ := pglogrepl.ParseLSN(si.Flushed)
		start := time.Now()
		d, lsn, err := lrReadSnapshot(r.snapPath())
		if err == nil && lsn >= uint64(flushed) && lsn > 0 {
			r.installData(d, lsn)
			r.saved.Store(lsn)
			slog.Info("mem-logrepl: restored from local snapshot", "lsn", pglogrepl.LSN(lsn), "slot_flushed", flushed, "elapsed", time.Since(start))
			return pglogrepl.LSN(lsn), nil
		}
		slog.Warn("mem-logrepl: local snapshot unusable; recreating slot", "error", err, "lsn", pglogrepl.LSN(lsn), "slot_flushed", flushed)
	}
	if si.Exists {
		if si.Active {
			// 前プロセスの walsender がまだ残っている
			_, _ = r.db.ExecContext(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL`, r.slot)
			time.Sleep(300 * time.Millisecond)
		}
		if _, err := conn.Exec(ctx, `DROP_REPLICATION_SLOT `+r.slot+` WAIT`).ReadAll(); err != nil {
			return 0, fmt.Errorf("drop slot: %w", err)
		}
	}
	start := time.Now()
	cr, err := pglogrepl.CreateReplicationSlot(ctx, conn, r.slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		return 0, fmt.Errorf("create slot: %w", err)
	}
	lsn, err := pglogrepl.ParseLSN(cr.ConsistentPoint)
	if err != nil {
		return 0, err
	}
	d, err := r.loadWithSnapshot(ctx, cr.SnapshotName)
	if err != nil {
		return 0, fmt.Errorf("initial load: %w", err)
	}
	r.installData(d, uint64(lsn))
	r.saved.Store(uint64(lsn)) // = confirmed_flush_lsn of the new slot
	r.needSave = true
	slog.Info("mem-logrepl: loaded via exported snapshot", "lsn", lsn, "users", len(d.users), "posts", len(d.posts), "elapsed", time.Since(start))
	return lsn, nil
}

func (r *memLogRepl) installData(d *memData, lsn uint64) {
	r.m.mu.Lock()
	r.m.d = d
	r.m.mu.Unlock()
	r.applied.Store(lsn)
	r.loaded = true
}

func (r *memLogRepl) loadWithSnapshot(ctx context.Context, snapshot string) (*memData, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `SET TRANSACTION SNAPSHOT '`+snapshot+`'`); err != nil {
		return nil, err
	}
	return lrLoad(ctx, tx)
}

// stream は受信ループ。pgconn は単一 goroutine でのみ使う。
func (r *memLogRepl) stream(ctx context.Context, conn *pgconn.PgConn) error {
	var txn []lrChange
	inTxn := false
	nextStatus := time.Now()
	for {
		if r.stopped.Load() {
			return errors.New("stopped")
		}
		now := time.Now()
		if !now.Before(nextStatus) {
			if err := r.sendStatus(ctx, conn); err != nil {
				return err
			}
			nextStatus = now.Add(lrStatusEvery)
			r.maybeSave()
		}
		rctx, cancel := context.WithDeadline(ctx, nextStatus)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("server error: %s", msg.Message)
		case *pgproto3.CopyData:
			// どのメッセージも walsender が生きている証拠 → lease を延ばす
			r.m.leaseUntil.Store(time.Now().Add(lrLease).UnixNano())
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				if !inTxn && uint64(ka.ServerWALEnd) > r.applied.Load() {
					r.applied.Store(uint64(ka.ServerWALEnd))
					r.updateReady()
				}
				if ka.ReplyRequested {
					nextStatus = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}
				lm, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return err
				}
				switch v := lm.(type) {
				case *pglogrepl.RelationMessage:
					r.rels[v.RelationID] = v
				case *pglogrepl.BeginMessage:
					inTxn, txn = true, txn[:0]
				case *pglogrepl.InsertMessage:
					ch, err := r.change(v.RelationID, "I", v.Tuple)
					if err != nil {
						return err
					}
					txn = append(txn, ch)
				case *pglogrepl.UpdateMessage:
					ch, err := r.change(v.RelationID, "U", v.NewTuple)
					if err != nil {
						return err
					}
					txn = append(txn, ch)
				case *pglogrepl.DeleteMessage:
					if v.OldTupleType != pglogrepl.UpdateMessageTupleTypeOld {
						return fmt.Errorf("delete without full old tuple (REPLICA IDENTITY FULL required)")
					}
					ch, err := r.change(v.RelationID, "D", v.OldTuple)
					if err != nil {
						return err
					}
					txn = append(txn, ch)
				case *pglogrepl.TruncateMessage:
					for _, id := range v.RelationIDs {
						rel := r.rels[id]
						if rel == nil {
							return fmt.Errorf("truncate: unknown relation %d", id)
						}
						txn = append(txn, lrChange{tbl: rel.RelationName, op: "T"})
					}
				case *pglogrepl.CommitMessage:
					if err := r.applyTxn(txn); err != nil {
						// メモリが壊れた可能性 → ファイルもスロットも信用せず作り直す
						r.loaded = false
						_ = os.Remove(r.snapPath())
						return err
					}
					inTxn, txn = false, txn[:0]
					r.applied.Store(uint64(v.TransactionEndLSN))
					r.updateReady()
				}
			}
		}
	}
}

func (r *memLogRepl) sendStatus(ctx context.Context, conn *pgconn.PgConn) error {
	applied := pglogrepl.LSN(r.applied.Load())
	flush := pglogrepl.LSN(r.saved.Load())
	if !memLogReplSnapshot || flush == 0 || flush > applied {
		flush = applied
	}
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: applied, WALFlushPosition: flush, WALApplyPosition: applied,
		ClientTime: time.Now(), ReplyRequested: true,
	})
}

type lrChange struct {
	tbl string
	op  string // I U D T
	row json.RawMessage
}

// change は pgoutput のタプルを to_jsonb 互換の JSON 行にする (applyChange の行型でそのまま読める)。
func (r *memLogRepl) change(relID uint32, op string, tuple *pglogrepl.TupleData) (lrChange, error) {
	rel := r.rels[relID]
	if rel == nil {
		return lrChange{}, fmt.Errorf("unknown relation %d", relID)
	}
	if tuple == nil {
		return lrChange{}, fmt.Errorf("%s %s: no tuple", rel.RelationName, op)
	}
	row := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[name] = nil
		case pglogrepl.TupleDataTypeToast:
			return lrChange{}, fmt.Errorf("%s.%s: unchanged toast value", rel.RelationName, name)
		case pglogrepl.TupleDataTypeText:
			v, err := r.decodeText(rel.Columns[i].DataType, col.Data)
			if err != nil {
				return lrChange{}, fmt.Errorf("%s.%s: %w", rel.RelationName, name, err)
			}
			row[name] = v
		default:
			return lrChange{}, fmt.Errorf("%s.%s: unexpected tuple type %c", rel.RelationName, name, col.DataType)
		}
	}
	b, err := json.Marshal(row)
	if err != nil {
		return lrChange{}, err
	}
	return lrChange{tbl: rel.RelationName, op: op, row: b}, nil
}

func (r *memLogRepl) decodeText(oid uint32, data []byte) (any, error) {
	switch oid {
	case pgtype.BoolOID:
		return len(data) > 0 && data[0] == 't', nil
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return strconv.ParseInt(string(data), 10, 64)
	case pgtype.Float4OID, pgtype.Float8OID, pgtype.NumericOID:
		return strconv.ParseFloat(string(data), 64)
	case pgtype.TimestamptzOID, pgtype.TimestampOID:
		var t time.Time
		if err := r.typeMap.Scan(oid, pgtype.TextFormatCode, data, &t); err != nil {
			return nil, err
		}
		return t, nil
	default:
		// text / varchar / uuid / date (YYYY-MM-DD) / enum
		return string(data), nil
	}
}

func (r *memLogRepl) applyTxn(txn []lrChange) error {
	if len(txn) == 0 {
		return nil
	}
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	d := r.m.d
	for _, ch := range txn {
		if err := lrApply(d, ch); err != nil {
			return err
		}
		if ch.op == "T" {
			r.needSave = true // initialize 直後の小さい状態を保存しておく
		}
	}
	if d.sortedDirty {
		d.sortedUsers()
	}
	return nil
}

// lrApply は 1 変更を反映する。INSERT/UPDATE と関係表の DELETE は applyChange をそのまま使い、
// 行そのものが消える DELETE (users/posts/posting_windows) と TRUNCATE だけここで扱う。
func lrApply(d *memData, ch lrChange) error {
	if ch.op == "T" {
		return d.lrTruncate(ch.tbl)
	}
	if ch.op == "D" {
		switch ch.tbl {
		case "users":
			var row memUserRow
			if err := json.Unmarshal(ch.row, &row); err != nil {
				return err
			}
			if _, ok := d.users[row.ID]; ok {
				delete(d.users, row.ID)
				d.sortedDirty = true
			}
			return nil
		case "posts":
			var row memPostRow
			if err := json.Unmarshal(ch.row, &row); err != nil {
				return err
			}
			d.lrRemovePost(row.ID)
			return nil
		case "posting_windows":
			var row memWindowRow
			if err := json.Unmarshal(ch.row, &row); err != nil {
				return err
			}
			if w := d.windows[row.ID]; w != nil {
				if d.open[w.Region] == w {
					delete(d.open, w.Region)
				}
				delete(d.windows, row.ID)
			}
			return nil
		}
	}
	return d.applyChange(memChange{Tbl: ch.tbl, Op: ch.op, Row: ch.row})
}

func lrRemoveFrom(list []*memPost, p *memPost) []*memPost {
	for i, x := range list {
		if x == p {
			return append(list[:i:i], list[i+1:]...)
		}
	}
	return list
}

func (d *memData) lrRemovePost(id string) {
	if memRespCacheOn {
		d.rc.clearAll()
	}
	p := d.posts[id]
	if p == nil {
		return
	}
	delete(d.posts, id)
	if p.window != nil {
		p.window.posts = lrRemoveFrom(p.window.posts, p)
	}
	if p.author != nil {
		p.author.posts = lrRemoveFrom(p.author.posts, p)
	}
}

func (d *memData) lrTruncate(tbl string) error {
	switch tbl {
	case "login_sessions":
		d.sessions = map[string]memSession{}
	case "friendships":
		for _, u := range d.users {
			u.friends = nil
		}
	case "follows":
		for _, u := range d.users {
			u.following, u.followers = nil, nil
		}
	case "block_relations":
		for _, u := range d.users {
			u.blocking, u.blockedBy = nil, nil
		}
	case "friend_requests":
		for _, u := range d.users {
			u.incoming, u.outgoing = nil, nil
		}
	default:
		return fmt.Errorf("mem-logrepl: truncate of %s not supported", tbl)
	}
	return nil
}

// ---- readiness ----

// updateReady: 同期対象に入っていることを確認済みで、確認時点の WAL 位置まで反映済みなら ready。
func (r *memLogRepl) updateReady() {
	if r.insync.Load() && r.applied.Load() >= r.target.Load() {
		if !r.m.ready.Load() {
			r.m.ready.Store(true)
			slog.Info("mem-logrepl: ready", "slot", r.slot, "lsn", pglogrepl.LSN(r.applied.Load()))
		}
	}
}

type lrSyncRow struct {
	SyncState string `db:"sync_state"`
	Blocked   bool   `db:"blocked"`
	LSN       string `db:"lsn"`
}

// checkLoop は自分の walsender が同期対象 (sync/quorum) で、watchdog に blocked にされていないかを確認し続ける。
func (r *memLogRepl) checkLoop() {
	t := time.NewTicker(lrCheckEvery)
	defer t.Stop()
	for range t.C {
		if r.stopped.Load() {
			return
		}
		pid, gen := r.pid.Load(), r.connGen.Load()
		if pid == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var row lrSyncRow
		err := r.db.GetContext(ctx, &row, `
SELECT coalesce((SELECT sync_state FROM pg_stat_replication WHERE pid = $1), 'none') AS sync_state,
       coalesce((SELECT blocked FROM `+lrMembersTable+` WHERE name = $2), true) AS blocked,
       pg_current_wal_lsn()::text AS lsn`, pid, r.slot)
		cancel()
		ok := err == nil && !row.Blocked && (row.SyncState == "sync" || row.SyncState == "quorum") && gen == r.connGen.Load()
		if !ok {
			if r.m.ready.Load() {
				slog.Warn("mem-logrepl: not in sync; memory reads off", "slot", r.slot, "state", row.SyncState, "blocked", row.Blocked, "error", err)
			}
			r.insync.Store(false)
			r.m.ready.Store(false)
			r.target.Store(^uint64(0))
			continue
		}
		if !r.insync.Load() {
			lsn, _ := pglogrepl.ParseLSN(row.LSN)
			r.target.Store(uint64(lsn))
			r.insync.Store(true)
		}
		r.updateReady()
	}
}

// ---- local snapshot ----

func (r *memLogRepl) snapPath() string { return filepath.Join(r.dir, r.slot+".snap.gz") }

type lrSnapHeader struct {
	Slot string `json:"slot"`
	LSN  uint64 `json:"lsn"`
}

// maybeSave は受信ループから呼ばれ、必要なら非同期でスナップショットを保存する。
func (r *memLogRepl) maybeSave() {
	if !memLogReplSnapshot {
		return
	}
	if !r.needSave && time.Since(r.lastSave) < lrSaveEvery {
		return
	}
	if r.applied.Load() == r.saved.Load() && !r.needSave {
		r.lastSave = time.Now()
		return
	}
	if !r.saving.CompareAndSwap(false, true) {
		return
	}
	r.needSave = false
	r.lastSave = time.Now()
	r.m.mu.RLock()
	dump := r.m.d.dump()
	lsn := r.applied.Load()
	r.m.mu.RUnlock()
	go func() {
		defer r.saving.Store(false)
		start := time.Now()
		if err := lrWriteSnapshot(r.snapPath(), r.slot, lsn, dump); err != nil {
			slog.Warn("mem-logrepl: save snapshot failed", "error", err)
			return
		}
		if lsn > r.saved.Load() {
			r.saved.Store(lsn)
		}
		slog.Info("mem-logrepl: snapshot saved", "lsn", pglogrepl.LSN(lsn), "elapsed", time.Since(start))
	}()
}

func lrWriteSnapshot(path, slot string, lsn uint64, dump *memDump) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	zw, _ := gzip.NewWriterLevel(bw, gzip.BestSpeed)
	enc := json.NewEncoder(zw)
	if err := enc.Encode(lrSnapHeader{Slot: slot, LSN: lsn}); err != nil {
		f.Close()
		return err
	}
	if err := enc.Encode(dump); err != nil {
		f.Close()
		return err
	}
	if err := zw.Close(); err != nil {
		f.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

func lrReadSnapshot(path string) (*memData, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(bufio.NewReaderSize(f, 1<<20))
	if err != nil {
		return nil, 0, err
	}
	dec := json.NewDecoder(zr)
	var h lrSnapHeader
	if err := dec.Decode(&h); err != nil {
		return nil, 0, err
	}
	var dump memDump
	if err := dec.Decode(&dump); err != nil {
		return nil, 0, err
	}
	if _, err := io.Copy(io.Discard, zr); err != nil {
		return nil, 0, err
	}
	d, err := memDataFromDump(&dump)
	if err != nil {
		return nil, 0, err
	}
	return d, h.LSN, nil
}

// ---- initial load (loadMemFromDB と同じ内容; 任意の querier = エクスポートしたスナップショットの tx で読む) ----

func lrLoad(ctx context.Context, q sqlx.QueryerContext) (*memData, error) {
	d := newMemData()
	var windows []memWindowRow
	if err := sqlx.SelectContext(ctx, q, &windows, `SELECT id, region, local_date::text AS local_date, starts_at, on_time_deadline, ended_at FROM posting_windows`); err != nil {
		return nil, err
	}
	for _, r := range windows {
		if err := d.applyWindow(r); err != nil {
			return nil, err
		}
	}
	var users []memUserRow
	if err := sqlx.SelectContext(ctx, q, &users, `SELECT `+strings.Join(userColumns, ", ")+` FROM users`); err != nil {
		return nil, err
	}
	for _, r := range users {
		d.applyUser(r)
	}
	var sessions []memSessionRow
	if err := sqlx.SelectContext(ctx, q, &sessions, `SELECT token_hash, user_id, active, created_at FROM login_sessions WHERE active`); err != nil {
		return nil, err
	}
	for _, r := range sessions {
		d.applySession(false, r)
	}
	var friendships []memFriendshipRow
	if err := sqlx.SelectContext(ctx, q, &friendships, `SELECT id, user_low_id, user_high_id, updated_at FROM friendships`); err != nil {
		return nil, err
	}
	for _, r := range friendships {
		d.applyFriendship(false, r)
	}
	var follows []memFollowRow
	if err := sqlx.SelectContext(ctx, q, &follows, `SELECT id, follower_id, followee_id, updated_at FROM follows`); err != nil {
		return nil, err
	}
	for _, r := range follows {
		d.applyFollow(false, r)
	}
	var blocks []memBlockRow
	if err := sqlx.SelectContext(ctx, q, &blocks, `SELECT blocker_id, blocked_id FROM block_relations`); err != nil {
		return nil, err
	}
	for _, r := range blocks {
		d.applyBlock(false, r)
	}
	var requests []memFriendRequestRow
	if err := sqlx.SelectContext(ctx, q, &requests, `SELECT id, requester_id, recipient_id, status, updated_at FROM friend_requests WHERE status = 'pending'`); err != nil {
		return nil, err
	}
	for _, r := range requests {
		d.applyFriendRequest(false, r)
	}
	var minDate *time.Time
	for _, w := range d.open {
		if minDate == nil || w.LocalDate.Before(*minDate) {
			v := w.LocalDate
			minDate = &v
		}
	}
	d.setHotFrom(minDate)
	if minDate != nil {
		var posts []memPostRow
		if err := sqlx.SelectContext(ctx, q, &posts, `
   SELECT p.id, p.author_id, p.posting_window_id, p.image_id, p.caption, p.visibility,
          p.location_kind, p.latitude, p.longitude, p.locality, p.posted_at, p.deleted_at
   FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date >= $1::date`, minDate.Format(time.DateOnly)); err != nil {
			return nil, err
		}
		for _, r := range posts {
			d.applyPost(r)
		}
		var comments []memCommentRow
		if err := sqlx.SelectContext(ctx, q, &comments, `
   SELECT c.id, c.post_id, c.author_id, c.body, c.created_at, c.deleted_at
   FROM comments c JOIN posts p ON p.id = c.post_id JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date >= $1::date AND c.deleted_at IS NULL`, minDate.Format(time.DateOnly)); err != nil {
			return nil, err
		}
		for _, r := range comments {
			d.applyComment(r)
		}
		var realmojis []memRealMojiRow
		if err := sqlx.SelectContext(ctx, q, &realmojis, `
   SELECT r.id, r.post_id, r.reactor_id, r.image_id, r.reaction_type, r.created_at, r.replaced_at, r.deleted_at
   FROM realmojis r JOIN posts p ON p.id = r.post_id JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date >= $1::date AND r.replaced_at IS NULL AND r.deleted_at IS NULL`, minDate.Format(time.DateOnly)); err != nil {
			return nil, err
		}
		for _, r := range realmojis {
			d.applyRealMoji(r)
		}
	}
	d.sortedUsers()
	return d, nil
}

// stop はテスト用 (kill -9 相当: 保存せずに止める)。
func (r *memLogRepl) stop() { r.stopped.Store(true); r.m.ready.Store(false) }
