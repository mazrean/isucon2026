package isureal

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

// memPump は primary の mem_changelog を xid スナップショット差分で tail し、メモリに反映する (asia1 = 全書き込み担当)。
// 書き込みリクエストは「コミット後に開始したサイクル」が完了するまでレスポンスを保留する (memWriteBarrier)。
// これでレスポンス後の読み取りは必ずメモリに反映済み。
type memPump struct {
	m  *memStore
	db *sqlx.DB

	snap string // 最後に反映済みのスナップショット (pg_snapshot text)

	pollFailures int // 連続した poll 失敗 (cycleMu 下)

	pmu       sync.Mutex
	cond      *sync.Cond
	started   uint64
	done      uint64
	requested uint64
	running   bool

	cycleMu sync.Mutex // サイクル (poll / reload) を直列化

	// invalidate のたびに増える。reload はロード開始時の値を loadedGen に記録し、
	// ロード中に無効化されていたら ready にしない (次のサイクルで再ロード)。
	wantGen   atomic.Uint64
	loadedGen uint64

	peers *memPeers // 分散モード (nil なら集中モード)

	// ISUREAL_STORE=1: PG から読み直す前に store の書き戻しを出し切る (cycleMu 保持中に呼ばれる)
	preReload func()
}

var (
	memCurrentPump      *memPump  // テスト用
	memCurrentPeerStore *memStore // テスト用
)

func memRole(config *Config) string {
	if !memEnabled {
		return ""
	}
	if role := os.Getenv("ISUREAL_MEM_ROLE"); role != "" {
		return role
	}
	if config.Region == "asia" {
		return "pump"
	}
	if os.Getenv("ISUREAL_MEM_PUMP") != "" {
		return "peer"
	}
	return ""
}

// setupMem は ISUREAL_MEM=1 のときだけメモリモデルを準備する。失敗しても DB 経路で動く。
func setupMem(h *HandlerContext, e *echo.Echo) {
	if memLogReplEnabled { // mem_logrepl.go
		setupMemLogRepl(h, e)
		return
	}
	switch memRole(h.Config) {
	case "pump":
		m := &memStore{d: newMemData()}
		p := &memPump{m: m, db: h.BG}
		p.cond = sync.NewCond(&p.pmu)
		p.peers = newMemPeersFromEnv(p)
		h.Mem = m
		h.memPump = p
		memCurrentPump = p
		gpMem.Store(m)
		if err := ensureMemSchema(h.DB); err != nil {
			slog.Error("mem: schema setup failed; mem disabled", "error", err)
			h.Mem, h.memPump = nil, nil
			return
		}
		e.Use(memWriteBarrier(p))
		registerMemPumpRoutes(e, p)
		registerSessMarkRoute(e, h, p)
		go p.ticker()
		go memBarrierStatsLoop()
		if p.peers != nil {
			go p.peers.heartbeatLoop()
		}
		go p.barrier()
	case "peer":
		m := &memStore{d: newMemData(), useLease: true}
		h.Mem = m
		memCurrentPeerStore = m
		if memWatermark {
			e.Use(memReadWait(m))
		}
		registerMemPeerRoutes(e, newMemReceiver(m))
	}
}

func ensureMemSchema(db *sqlx.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.GetContext(ctx, &n, `SELECT count(*) FROM pg_trigger WHERE tgname = 'mem_changelog' AND NOT tgisinternal`); err == nil && n == len(memTriggerTables) {
		return nil
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(727274)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, memSchemaSQL); err != nil {
		return err
	}
	for _, t := range memTriggerTables {
		if _, err := tx.ExecContext(ctx, `CREATE OR REPLACE TRIGGER mem_changelog AFTER INSERT OR UPDATE OR DELETE ON `+t+` FOR EACH ROW EXECUTE FUNCTION mem_changelog_trigger()`); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// barrier は、呼び出し後に開始したサイクルの完了を待つ。
func (p *memPump) barrier() { p.barrierScope(true) }

// barrierScope(false) は自ホストのメモリへの反映だけを待つ (peer の ack は待たない)。
// 後続の全体バリアは現在の連番までの ack を待つので、取りこぼしは起きない。
func (p *memPump) barrierScope(peers bool) {
	p.pmu.Lock()
	want := p.started + 1
	if p.requested < want {
		p.requested = want
	}
	if !p.running {
		p.running = true
		go p.loop()
	}
	for p.done < want {
		p.cond.Wait()
	}
	p.pmu.Unlock()
	if peers && p.peers != nil {
		p.peers.waitAcked()
	}
}

func (p *memPump) loop() {
	for {
		p.pmu.Lock()
		if p.requested <= p.done {
			p.running = false
			p.pmu.Unlock()
			return
		}
		p.started++
		g := p.started
		p.pmu.Unlock()

		if !p.cycle() {
			// nothing was applied or published: keep barrier waiters blocked and retry
			time.Sleep(10 * time.Millisecond)
			continue
		}

		p.pmu.Lock()
		p.done = g
		p.cond.Broadcast()
		p.pmu.Unlock()
	}
}

func (p *memPump) ticker() {
	every := 200 * time.Millisecond
	if memWatermark && p.peers != nil {
		every = memWMTick // watermark の鮮度 = サイクル周期 (ISUREAL_MEM_WM_HB=1 なら既定 10ms)
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for range t.C {
		p.barrier()
	}
}

// cycle reports whether every change committed before it started is now applied
// locally and handed to the peers; barrier waiters are released only then.
func (p *memPump) cycle() bool {
	p.cycleMu.Lock()
	defer p.cycleMu.Unlock()
	if !p.m.ready.Load() || p.loadedGen != p.wantGen.Load() {
		if err := p.reload(); err != nil {
			slog.Error("mem: reload failed", "error", err)
			return false
		}
		return true
	}
	// a failed poll leaves p.snap unchanged, so retrying is safe; only reload
	// (seconds, blocks every write) when polling keeps failing.
	if err := p.poll(); err != nil {
		p.pollFailures++
		if p.pollFailures < 3 {
			slog.Warn("mem: poll failed; retrying", "error", err, "failures", p.pollFailures)
			return false
		}
		slog.Error("mem: poll failed; reloading", "error", err)
		p.pollFailures = 0
		p.invalidate()
		return false
	}
	p.pollFailures = 0
	return true
}

type memPollRow struct {
	Cur string         `db:"cur"`
	Seq sql.NullInt64  `db:"seq"`
	Tbl sql.NullString `db:"tbl"`
	Op  sql.NullString `db:"op"`
	Row []byte         `db:"row"`
}

// fetchChanges は prev スナップショット以降にコミットされた変更と、新しいスナップショットを返す (単一文 = 単一スナップショット)。
func fetchChanges(ctx context.Context, db *sqlx.DB, prev string) ([]memChange, string, error) {
	var rows []memPollRow
	if err := db.SelectContext(ctx, &rows, `
  SELECT s.cur::text AS cur, c.seq, c.tbl, c.op, c.row
  FROM (SELECT pg_current_snapshot() AS cur) s
  LEFT JOIN mem_changelog c ON c.xid >= pg_snapshot_xmin($1::pg_snapshot) AND NOT pg_visible_in_snapshot(c.xid, $1::pg_snapshot)
  ORDER BY c.seq
 `, prev); err != nil {
		return nil, "", err
	}
	if len(rows) == 0 {
		return nil, "", fmt.Errorf("mem: empty poll result")
	}
	changes := make([]memChange, 0, len(rows))
	for _, r := range rows {
		if !r.Seq.Valid {
			continue
		}
		changes = append(changes, memChange{Seq: r.Seq.Int64, Tbl: r.Tbl.String, Op: r.Op.String, Row: r.Row})
	}
	return changes, rows[0].Cur, nil
}

func (p *memPump) poll() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// watermark: クエリを送る直前の時刻 (スナップショット取得より前)。mem_watermark.go 参照。
	x := memNow().UnixNano()
	changes, cur, err := fetchChanges(ctx, p.db, p.snap)
	if err != nil {
		return err
	}
	if err := p.m.applyBatch(changes); err != nil {
		return err
	}
	p.snap = cur
	if p.peers != nil && len(changes) > 0 {
		if memWMHB {
			p.peers.publishWM(changes, x) // wm をデータ push に同梱 (wm 単独メッセージとの突き合わせ待ちを省く)
		} else {
			p.peers.publish(changes)
		}
	}
	if memWatermark && p.peers != nil {
		p.peers.setWM(x)
	}
	return nil
}

func (m *memStore) applyBatch(changes []memChange) error {
	if len(changes) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range changes {
		if err := m.d.applyChange(ch); err != nil {
			return err
		}
	}
	if m.d.sortedDirty {
		m.d.sortedUsers()
	}
	return nil
}

// invalidate はメモリを not-ready にし、次のサイクルで再ロードさせる。
func (p *memPump) invalidate() {
	p.wantGen.Add(1)
	p.m.ready.Store(false)
	if p.peers != nil {
		p.peers.invalidate()
	}
}

func (p *memPump) reload() error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	start := time.Now()
	gen := p.wantGen.Load()
	p.m.ready.Store(false)
	if p.preReload != nil {
		p.preReload()
	}
	d, snap, err := loadMemFromDB(ctx, p.db)
	if err != nil {
		return err
	}
	p.m.mu.Lock()
	p.m.d = d
	p.m.mu.Unlock()
	p.snap = snap
	if p.peers != nil {
		p.peers.newEpoch()
	}
	// ロード中にコミットされた分を取り込んでから公開する
	if err := p.poll(); err != nil {
		return err
	}
	p.loadedGen = gen
	if gen != p.wantGen.Load() {
		return fmt.Errorf("mem: invalidated during reload")
	}
	p.m.ready.Store(true)
	go memPrebuildLoaded(p.m)
	slog.Info("mem: loaded", "users", len(d.users), "posts", len(d.posts), "windows", len(d.windows), "elapsed", time.Since(start))
	return nil
}

// resetAfterInitialize は initialize (DB リセット) 後に呼ぶ。全ホストのメモリを作り直して完了を待つ。
func (p *memPump) resetAfterInitialize(ctx context.Context) {
	if _, err := p.db.ExecContext(ctx, `TRUNCATE mem_changelog`); err != nil {
		slog.Error("mem: truncate changelog failed", "error", err)
	}
	p.invalidate()
	p.barrier()
	if p.peers != nil && p.m.ready.Load() {
		p.peers.resetAll(ctx)
	}
}

func loadMemFromDB(ctx context.Context, db *sqlx.DB) (*memData, string, error) {
	d := newMemData()
	var snap string
	if err := db.GetContext(ctx, &snap, `SELECT pg_current_snapshot()::text`); err != nil {
		return nil, "", err
	}
	var windows []memWindowRow
	if err := db.SelectContext(ctx, &windows, `SELECT id, region, local_date::text AS local_date, starts_at, on_time_deadline, ended_at FROM posting_windows`); err != nil {
		return nil, "", err
	}
	for _, r := range windows {
		if err := d.applyWindow(r); err != nil {
			return nil, "", err
		}
	}
	var users []memUserRow
	if err := db.SelectContext(ctx, &users, `SELECT `+strings.Join(userColumns, ", ")+` FROM users`); err != nil {
		return nil, "", err
	}
	for _, r := range users {
		d.applyUser(r)
	}
	var sessions []memSessionRow
	if err := db.SelectContext(ctx, &sessions, `SELECT token_hash, user_id, active, created_at FROM login_sessions WHERE active`); err != nil {
		return nil, "", err
	}
	for _, r := range sessions {
		d.applySession(false, r)
	}
	var friendships []memFriendshipRow
	if err := db.SelectContext(ctx, &friendships, `SELECT id, user_low_id, user_high_id, updated_at FROM friendships`); err != nil {
		return nil, "", err
	}
	for _, r := range friendships {
		d.applyFriendship(false, r)
	}
	var follows []memFollowRow
	if err := db.SelectContext(ctx, &follows, `SELECT id, follower_id, followee_id, updated_at FROM follows`); err != nil {
		return nil, "", err
	}
	for _, r := range follows {
		d.applyFollow(false, r)
	}
	var blocks []memBlockRow
	if err := db.SelectContext(ctx, &blocks, `SELECT blocker_id, blocked_id FROM block_relations`); err != nil {
		return nil, "", err
	}
	for _, r := range blocks {
		d.applyBlock(false, r)
	}
	var requests []memFriendRequestRow
	if err := db.SelectContext(ctx, &requests, `SELECT id, requester_id, recipient_id, status, updated_at FROM friend_requests WHERE status = 'pending'`); err != nil {
		return nil, "", err
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
		if err := db.SelectContext(ctx, &posts, `
   SELECT p.id, p.author_id, p.posting_window_id, p.image_id, p.caption, p.visibility,
          p.location_kind, p.latitude, p.longitude, p.locality, p.posted_at, p.deleted_at
   FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date >= $1::date`, minDate.Format(time.DateOnly)); err != nil {
			return nil, "", err
		}
		for _, r := range posts {
			d.applyPost(r)
		}
		var comments []memCommentRow
		if err := db.SelectContext(ctx, &comments, `
   SELECT c.id, c.post_id, c.author_id, c.body, c.created_at, c.deleted_at
   FROM comments c JOIN posts p ON p.id = c.post_id JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date >= $1::date AND c.deleted_at IS NULL`, minDate.Format(time.DateOnly)); err != nil {
			return nil, "", err
		}
		for _, r := range comments {
			d.applyComment(r)
		}
		var realmojis []memRealMojiRow
		if err := db.SelectContext(ctx, &realmojis, `
   SELECT r.id, r.post_id, r.reactor_id, r.image_id, r.reaction_type, r.created_at, r.replaced_at, r.deleted_at
   FROM realmojis r JOIN posts p ON p.id = r.post_id JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date >= $1::date AND r.replaced_at IS NULL AND r.deleted_at IS NULL`, minDate.Format(time.DateOnly)); err != nil {
			return nil, "", err
		}
		for _, r := range realmojis {
			d.applyRealMoji(r)
		}
	}
	d.sortedUsers()
	return d, snap, nil
}

// ---- write barrier ----

type memBufferedWriter struct {
	header http.Header
	code   int
	buf    bytes.Buffer
}

func (w *memBufferedWriter) Header() http.Header { return w.header }
func (w *memBufferedWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
func (w *memBufferedWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.buf.Write(b)
}

// memWriteBarrier: 非 GET の /api/* はレスポンスをバッファし、メモリ反映 (+ 分散モードでは全ホストの ack) を待ってから返す。
func memWriteBarrier(p *memPump) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			if req.Method == http.MethodGet || req.Method == http.MethodHead ||
				!strings.HasPrefix(req.URL.Path, "/api/") || req.URL.Path == "/api/initialize" {
				return next(c)
			}
			orig := c.Response()
			bw := &memBufferedWriter{header: orig.Header()}
			c.SetResponse(bw)
			err := next(c)
			c.SetResponse(orig)
			// a 4xx means the handler rolled back (no handler commits and then
			// returns 4xx), so there is nothing new for later reads to observe.
			var apiErr *APIError
			clientErr := (bw.code >= 400 && bw.code < 500) ||
				(err != nil && errors.As(err, &apiErr) && apiErr.Status >= 400 && apiErr.Status < 500)
			if handled, _ := c.Get(storeHandledKey).(bool); !clientErr && !handled {
				bs := time.Now()
				p.barrierScope(!localOnlyWrite(req))
				memRecordBarrier(time.Since(bs))
			}
			if bw.code != 0 {
				orig.WriteHeader(bw.code)
				_, _ = orig.Write(bw.buf.Bytes())
			}
			return err
		}
	}
}

// localOnlyWrite: login/logout of a user of this host's own region (not forwarded
// by a peer). Only this host reads that user's sessions: other regions reject the
// session (RegionMiddleware), and moving region is itself a write whose full
// barrier first waits for every earlier change, these sessions included.
func localOnlyWrite(req *http.Request) bool {
	if !localOnlyWriteOn || req.Header.Get(forwardedRegionHeader) != "" {
		return false
	}
	switch req.URL.Path {
	case "/api/auth/login", "/api/auth/logout":
		return true
	}
	return false
}

// ISUREAL_LOCAL_ONLY_WRITE=0 makes local login/logout wait for every peer too (safety switch).
var localOnlyWriteOn = os.Getenv("ISUREAL_LOCAL_ONLY_WRITE") != "0"
