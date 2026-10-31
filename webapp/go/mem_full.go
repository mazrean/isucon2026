package isureal

import (
	"context"

	"encoding/json"
	"github.com/jackc/pgx/v5"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

// ISUREAL_MEM_FULL=1 (asia2 だけ。既定 0 = 挙動は変えない): 全期間をメモリに持つ冷たい読みのノード。
//   - ロード: pump / logrepl の loader は hotFrom を最古の投稿期間にする (全投稿・コメント・RealMoji)。
//     peer (asia2) は pump のダンプ (ホット集合) を取った後、ホット外 (local_date < dump.hotFrom) の投稿と
//     その有効なコメント/RealMoji を自分の PG (同じホスト) から足す。ホット外の行の変更は DB 経路の書き込み
//     (トリガー → pump) なので、ダンプの後に読んだ PG はダンプの seq 以前の変更をすべて含む。以後の変更は push
//     (applyPost/Comment/RealMoji は ID で冪等) で上書きされる。hotFrom はゼロ時刻 (= 全期間がメモリ)。
//   - ダンプは wm=1 で取る (pump はこの peer の ack を待たない)。RYW は下の冷たい待ちで取る。
//   - 冷たい読みの受け口 (cold_origin.go の送り手との約束): X-Isureal-Cold-Hop = fwd secret の要求は、
//     X-Isureal-Cold-Space の系列で適用済みの seq が X-Isureal-Cold-Wait 以上になるまで待つ (上限 2000ms)。
//       space=pump : 適用済みの pump の batch seq (memReceiver.seq) と比べる。
//       space=store: push の SS (asia1 が ps.mu の下で読んだ storeCurrentSeq。Seq までのバッチを適用した時点で有効)
//                    の適用済みの最大値と比べる。
//     届いたらメモリで答える。上限を超えた / lease 切れのときは自分の PG で答える (memForceDBKey)。
//     作り直し中 (not-ready) は待たずに 503 (送り手が自分の PG で答える)。
//   - キャッシュ origin の要求 (X-Isureal-Cache-Origin) は待ちの目印を持たないので、従来どおり PG で答える。

var memFull = os.Getenv("ISUREAL_MEM_FULL") == "1"

// memFullLoaders: pump / logrepl の loader を全期間にするか (テストで peer だけ全期間にするときに false)
var memFullLoaders = memFull

// memColdMaxWait: 受け手が seq を待つ上限 (超えたら 503 = 送り手が自分の経路へ)。coldForwardMax: 送り手の転送全体の上限。
var (
	memColdMaxWait = time.Duration(envIntOrDefault("ISUREAL_COLD_WAIT_MS", 300)) * time.Millisecond
	coldForwardMax = time.Duration(envIntOrDefault("ISUREAL_COLD_FORWARD_MAX_MS", 1000)) * time.Millisecond
)

var (
	memFullDB    *sqlx.DB // peer の setupMem が設定 (ホット外のロード元 = 自分の PG)
	memFullDSN   string
	memColdStats struct{ served, timeout, notOn, forceDB atomic.Int64 }
)

// memForceDBKey: この要求はメモリで答えない (onCtx が false)。
type memForceDBKey struct{}

func memForcedDB(ctx context.Context) bool { return ctx.Value(memForceDBKey{}) != nil }

// memNotify: 値が進んだら待ち手を起こす (close する chan を差し替える)。
type memNotify struct {
	mu sync.Mutex
	ch chan struct{}
}

func (n *memNotify) wait() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	return n.ch
}

func (n *memNotify) wake() {
	n.mu.Lock()
	if n.ch != nil {
		close(n.ch)
		n.ch = nil
	}
	n.mu.Unlock()
}

// setApplied: r.mu の下、r.seq を進めた後に呼ぶ。
func (r *memReceiver) setApplied() {
	r.appliedPump.Store(r.seq)
	r.coldNotify.wake()
}

func (r *memReceiver) coldApplied(space string) int64 {
	if space == "store" {
		return r.appliedStore.Load()
	}
	return r.appliedPump.Load()
}

// coldWait: space の適用済み seq >= target かつメモリが使えるまで待つ。true ならメモリで答えてよい。
func (r *memReceiver) coldWait(ctx context.Context, space string, target int64) bool {
	deadline := time.NewTimer(memColdMaxWait)
	defer deadline.Stop()
	for {
		ch := r.coldNotify.wait()
		if r.m.on() && r.coldApplied(space) >= target {
			return true
		}
		if !r.m.ready.Load() {
			return false // 再同期中 (ダンプ + ホット外のロードで数十秒): 待たずに PG
		}
		select {
		case <-ch:
		case <-time.After(5 * time.Millisecond): // lease の回復など通知の無い変化
		case <-deadline.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// memColdWaitMiddleware: MEM_FULL の peer で冷たい転送の要求を待たせる。
func memColdWaitMiddleware(r *memReceiver) echo.MiddlewareFunc {
	go memColdStatsLoop()
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			if req.Header.Get(memRespOriginHeader) != "" {
				memColdStats.forceDB.Add(1)
				c.SetRequest(req.WithContext(context.WithValue(req.Context(), memForceDBKey{}, true)))
				return next(c)
			}
			if !coldIsHop(req) {
				return next(c)
			}
			target, err := strconv.ParseInt(req.Header.Get(coldWaitHeader), 10, 64)
			if err != nil {
				target = 0
			}
			space := req.Header.Get(coldSpaceHeader)
			if !r.m.ready.Load() {
				// 作り直し中 (initialize の後のダンプ + ホット外のロード、18〜21 秒): 即座に断る。
				// 送り手 (cold_origin.go の coldForward) は 5xx で自分の PG の経路に落ちる。読みを待たせない。
				memColdStats.notOn.Add(1)
				return c.NoContent(http.StatusServiceUnavailable)
			}
			if r.coldWait(req.Context(), space, target) {
				memColdStats.served.Add(1)
				return next(c)
			}
			if r.m.on() {
				memColdStats.timeout.Add(1)
			} else {
				memColdStats.notOn.Add(1)
			}
			// 待ちの上限を超えた: 自分の PG で答えず断る (送り手は 5xx で自分の経路へ。asia2 の PG に重い読みを足さない)
			return c.NoContent(http.StatusServiceUnavailable)
		}
	}
}

func memColdStatsLoop() {
	for range time.Tick(10 * time.Second) {
		s, t, n, f := memColdStats.served.Swap(0), memColdStats.timeout.Swap(0), memColdStats.notOn.Swap(0), memColdStats.forceDB.Swap(0)
		if s+t+n+f > 0 {
			slog.Info("mem-full: cold reads", "mem", s, "timeout_pg", t, "not_ready_503", n, "origin_pg", f)
		}
	}
}

// memFullMinDate: loader の minDate (posts を読む下限) を、MEM_FULL なら最古の投稿期間にする。
func memFullMinDate(d *memData, minDate *time.Time) *time.Time {
	if !memFullLoaders {
		return minDate
	}
	var first *time.Time
	for _, w := range d.windows {
		if first == nil || w.LocalDate.Before(*first) {
			v := w.LocalDate
			first = &v
		}
	}
	if first == nil {
		return minDate
	}
	return first
}

// memLoadCold: peer のダンプにホット外の投稿/コメント/RealMoji を自分の PG から足す。
// 行はストリームで読み (一括スライスを作らない。StructScan の反射は 1 行 ~8µs かかるので列を直接 Scan)、新しく足した投稿の索引 (投稿期間ごとの投稿、投稿ごとのコメント/RealMoji)
// には append だけして、最後に一度だけ整列する。ctx は世代ごと (invalidate で取り消される)。
func memLoadCold(ctx context.Context, d *memData) error {
	if memFullDB == nil {
		return nil
	}
	start := time.Now()
	var gc0 runtime.MemStats
	runtime.ReadMemStats(&gc0)
	var tSQL, tScan, tApply time.Duration
	// pgx を直接使う (database/sql の行の変換は 1 行 ~7µs で、全履歴 2.2M 行の律速だった)。専用の接続で jit も切る。
	conn, err := pgx.Connect(ctx, memFullDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "SET jit = off"); err != nil {
		return err
	}
	beforeT := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	if d.hotKnown {
		beforeT = d.hotFrom
	}
	stream := func(q string, scan func(pgx.Rows) error) (int, error) {
		t0 := time.Now()
		rows, err := conn.Query(ctx, q, beforeT)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		n := 0
		first := true
		for rows.Next() {
			if first {
				tSQL += time.Since(t0) // 最初の行まで = SQL の実行
				first = false
			}
			if err := scan(rows); err != nil {
				return n, err
			}
			n++
		}
		if first {
			tSQL += time.Since(t0)
		}
		return n, rows.Err()
	}
	fresh := map[*memPost]struct{}{}     // このロードで足した投稿 (コメント/RealMoji は append だけ)
	touched := map[*memWindow]struct{}{} // 投稿を append した投稿期間
	nPosts, err := stream(`
   SELECT p.id, p.author_id, p.posting_window_id, p.image_id, p.caption, p.visibility,
          p.location_kind, p.latitude, p.longitude, p.locality, p.posted_at, p.deleted_at
   FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date < $1::date`, func(rows pgx.Rows) error {
		var r memPostRow
		t0 := time.Now()
		if err := rows.Scan(&r.ID, &r.AuthorID, &r.PostingWindowID, &r.ImageID, &r.Caption, &r.Visibility, &r.LocationKind, &r.Latitude, &r.Longitude, &r.Locality, &r.PostedAt, &r.DeletedAt); err != nil {
			return err
		}
		t1 := time.Now()
		tScan += t1.Sub(t0)
		if d.posts[r.ID] == nil { // ダンプ (pump のメモリ) にあるものはそちらが新しい
			w := d.windows[r.PostingWindowID]
			if w == nil {
				d.applyPost(r)
			} else {
				author := d.user(r.AuthorID)
				p := &memPost{Post: Post{
					ID: r.ID, AuthorID: r.AuthorID, PostingWindowID: r.PostingWindowID, ImageID: r.ImageID,
					Caption: r.Caption, Visibility: r.Visibility,
					PostLocation: PostLocation{Kind: r.LocationKind, Latitude: r.Latitude, Longitude: r.Longitude, Locality: r.Locality},
					PostedAt:     memLocal(r.PostedAt),
				}, deleted: r.DeletedAt != nil, author: author, window: w}
				d.posts[r.ID] = p
				w.posts = append(w.posts, p)
				author.posts = append(author.posts, p)
				touched[w] = struct{}{}
				fresh[p] = struct{}{}
			}
		}
		tApply += time.Since(t1)
		return nil
	})
	if err != nil {
		return err
	}
	nComments, err := stream(`
   SELECT c.id, c.post_id, c.author_id, c.body, c.created_at, c.deleted_at
   FROM comments c JOIN posts p ON p.id = c.post_id JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date < $1::date AND c.deleted_at IS NULL`, func(rows pgx.Rows) error {
		var r memCommentRow
		t0 := time.Now()
		if err := rows.Scan(&r.ID, &r.PostID, &r.AuthorID, &r.Body, &r.CreatedAt, &r.DeletedAt); err != nil {
			return err
		}
		t1 := time.Now()
		tScan += t1.Sub(t0)
		if p := d.posts[r.PostID]; p != nil {
			if _, ok := fresh[p]; ok {
				p.comments = append(p.comments, &memComment{ID: r.ID, PostID: r.PostID, author: d.user(r.AuthorID), Body: r.Body, CreatedAt: memLocal(r.CreatedAt)})
			} else {
				d.applyComment(r)
			}
		}
		tApply += time.Since(t1)
		return nil
	})
	if err != nil {
		return err
	}
	nRealMojis, err := stream(`
   SELECT r.id, r.post_id, r.reactor_id, r.image_id, r.reaction_type, r.created_at, r.replaced_at, r.deleted_at
   FROM realmojis r JOIN posts p ON p.id = r.post_id JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date < $1::date AND r.replaced_at IS NULL AND r.deleted_at IS NULL`, func(rows pgx.Rows) error {
		var r memRealMojiRow
		t0 := time.Now()
		if err := rows.Scan(&r.ID, &r.PostID, &r.ReactorID, &r.ImageID, &r.ReactionType, &r.CreatedAt, &r.ReplacedAt, &r.DeletedAt); err != nil {
			return err
		}
		t1 := time.Now()
		tScan += t1.Sub(t0)
		if p := d.posts[r.PostID]; p != nil {
			if _, ok := fresh[p]; ok {
				p.realmojis = append(p.realmojis, &memRealMoji{ID: r.ID, PostID: r.PostID, reactor: d.user(r.ReactorID), ImageID: r.ImageID, ReactionType: r.ReactionType, CreatedAt: memLocal(r.CreatedAt)})
			} else {
				d.applyRealMoji(r)
			}
		}
		tApply += time.Since(t1)
		return nil
	})
	if err != nil {
		return err
	}
	t0 := time.Now()
	for w := range touched {
		sort.Slice(w.posts, func(i, j int) bool { return memPostLess(w.posts[i], w.posts[j]) })
	}
	for p := range fresh {
		if len(p.comments) > 1 {
			sort.Slice(p.comments, func(i, j int) bool {
				return memTimeIDLess(p.comments[i].CreatedAt, p.comments[i].ID, p.comments[j].CreatedAt, p.comments[j].ID)
			})
		}
		if len(p.realmojis) > 1 {
			sort.Slice(p.realmojis, func(i, j int) bool {
				return memTimeIDLess(p.realmojis[i].CreatedAt, p.realmojis[i].ID, p.realmojis[j].CreatedAt, p.realmojis[j].ID)
			})
		}
	}
	tSort := time.Since(t0)
	d.hotFrom, d.hotKnown = time.Time{}, true
	var gc1 runtime.MemStats
	runtime.ReadMemStats(&gc1)
	slog.Info("mem-full: cold rows loaded", "posts", nPosts, "comments", nComments, "realmojis", nRealMojis,
		"elapsed", time.Since(start), "sql", tSQL, "scan", tScan, "apply", tApply, "sort", tSort,
		"gc_pause", time.Duration(gc1.PauseTotalNs-gc0.PauseTotalNs), "gc_n", gc1.NumGC-gc0.NumGC, "gc_cpu", gc1.GCCPUFraction,
		"heap_mib", gc1.HeapInuse>>20, "gomaxprocs", runtime.GOMAXPROCS(0))
	return nil
}

// --- 送り手側の確認 (ISUREAL_COLD_ORIGIN): 冷たい読みのノードが ready・同じ epoch・追いついているときだけ転送する ---

type coldOriginStatus struct {
	Ready bool   `json:"ready"`
	Epoch string `json:"epoch"`
	Pump  int64  `json:"pump"`
	Store int64  `json:"store"`
	at    time.Time
}

var (
	coldOriginSt        atomic.Pointer[coldOriginStatus]
	coldPumpEpochFn     func() string // asia1: pump の epoch (setupMem が設定)
	coldOriginMaxLag    = int64(envIntOrDefault("ISUREAL_COLD_ORIGIN_MAX_LAG", 256))
	coldOriginPollEvery = 100 * time.Millisecond
	coldOriginPollOnce  sync.Once
	coldOriginSkipped   atomic.Int64
)

// registerMemFullStatus: MEM_FULL の peer (asia2) が状態を答える。
func registerMemFullStatus(e *echo.Echo, r *memReceiver) {
	e.GET("/internal/mem/full-status", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		r.mu.Lock()
		epoch := r.epoch
		r.mu.Unlock()
		return c.JSON(http.StatusOK, coldOriginStatus{Ready: r.m.on(), Epoch: epoch, Pump: r.appliedPump.Load(), Store: r.appliedStore.Load()})
	})
}

func coldOriginPollLoop() {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for {
		var st coldOriginStatus
		req, _ := http.NewRequest(http.MethodGet, "http://"+coldOrigin+"/internal/mem/full-status", nil)
		req.Header.Set(forwardedSecretHeader, forwardSecret)
		if resp, err := client.Do(req); err == nil {
			if resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&st) == nil {
				st.at = time.Now()
				coldOriginSt.Store(&st)
			}
			resp.Body.Close()
		}
		time.Sleep(coldOriginPollEvery)
	}
}

// coldOriginGate: cold_origin.go が転送の前に呼ぶ (テストで差し替える)。
var coldOriginGate = coldOriginUsable

// coldOriginUsable: 転送してよいか (標本が新しく、ready、asia1 なら同じ epoch で遅れが MAX_LAG 以下)。
func coldOriginUsable() bool {
	coldOriginPollOnce.Do(func() { go coldOriginPollLoop() })
	st := coldOriginSt.Load()
	ok := st != nil && st.Ready && time.Since(st.at) < 5*coldOriginPollEvery
	if ok && coldPumpEpochFn != nil && coldPumpSeqFn != nil {
		ok = st.Epoch == coldPumpEpochFn() && coldPumpSeqFn()-st.Pump <= coldOriginMaxLag
	}
	if !ok {
		coldOriginSkipped.Add(1)
	}
	return ok
}
