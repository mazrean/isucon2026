package isureal

import (
	"context"
	"log/slog"
	"net/http"
	"os"
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

const memColdMaxWait = 2000 * time.Millisecond

var (
	memFullDB    *sqlx.DB // peer の setupMem が設定 (ホット外のロード元 = 自分の PG)
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
			c.SetRequest(req.WithContext(context.WithValue(req.Context(), memForceDBKey{}, true)))
			return next(c)
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
func memLoadCold(d *memData) error {
	if memFullDB == nil {
		return nil
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	before := "infinity"
	if d.hotKnown {
		before = d.hotFrom.Format(time.DateOnly)
	}
	var posts []memPostRow
	if err := memFullDB.SelectContext(ctx, &posts, `
   SELECT p.id, p.author_id, p.posting_window_id, p.image_id, p.caption, p.visibility,
          p.location_kind, p.latitude, p.longitude, p.locality, p.posted_at, p.deleted_at
   FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date < $1::date`, before); err != nil {
		return err
	}
	for _, r := range posts {
		if d.posts[r.ID] != nil {
			continue // ダンプ (pump のメモリ) の方が新しい
		}
		d.applyPost(r)
	}
	nPosts := len(posts)
	posts = nil
	var comments []memCommentRow
	if err := memFullDB.SelectContext(ctx, &comments, `
   SELECT c.id, c.post_id, c.author_id, c.body, c.created_at, c.deleted_at
   FROM comments c JOIN posts p ON p.id = c.post_id JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date < $1::date AND c.deleted_at IS NULL`, before); err != nil {
		return err
	}
	for _, r := range comments {
		d.applyComment(r)
	}
	nComments := len(comments)
	comments = nil
	var realmojis []memRealMojiRow
	if err := memFullDB.SelectContext(ctx, &realmojis, `
   SELECT r.id, r.post_id, r.reactor_id, r.image_id, r.reaction_type, r.created_at, r.replaced_at, r.deleted_at
   FROM realmojis r JOIN posts p ON p.id = r.post_id JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date < $1::date AND r.replaced_at IS NULL AND r.deleted_at IS NULL`, before); err != nil {
		return err
	}
	for _, r := range realmojis {
		d.applyRealMoji(r)
	}
	d.hotFrom, d.hotKnown = time.Time{}, true
	slog.Info("mem-full: cold rows loaded", "posts", nPosts, "comments", nComments, "realmojis", len(realmojis), "elapsed", time.Since(start))
	return nil
}
