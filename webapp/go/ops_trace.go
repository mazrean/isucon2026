package isureal

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_OPS_TRACE=1 (既定 0、deploy の OPS_TRACE で全ホスト): 一連の操作を匿名の相関 ID で追う性能の観測。
// Pre に登録 (prevalidate より前) するので Pre で転送される要求も見る。相関 ID の鍵は forwardSecret から HMAC で導き、全ホストで同じ。
// 過去月 = 要求の month / date がアプリの投稿期間の最新の月 (DB の max(local_date)) より前。待ちは段ごと (slot / fwdprio / imgsync / apply / pg / ack)。
// 応答は変えない (読むだけ)。認証情報・生のトークンは保存しない: 相関 ID = HMAC-SHA256(起動ごとの乱数鍵, token) の
// hex 先頭 8 文字。login のトークンは応答本文から HMAC にしてすぐ捨てる。10 秒ごとに slog "ops-trace" で:
//   - login → 過去月の Memories (month / date が今月より前) の 2xx までの時間 p50 / p90
//   - 投稿 (POST /api/posts 2xx) の後に同じ ID で完了した (2xx) 閲覧 (投稿の詳細 / feed)、RealMoji、コメントの数
//   - 書き込みの待ちの地域ごとの p50: 枠待ち (GP fair / post cap の slot)、永続化 (store の PG 書き戻し待ち)、
//     複製待ち (store の peer ack 待ち)

var opsTraceOn = os.Getenv("ISUREAL_OPS_TRACE") == "1"

// opsKey: 全ホストで同じ匿名 ID になるよう forwardSecret から導く (無ければ起動ごとの乱数)。
var opsKey = func() []byte {
	if forwardSecret != "" {
		m := hmac.New(sha256.New, []byte(forwardSecret))
		m.Write([]byte("isureal-ops-trace-cid"))
		return m.Sum(nil)
	}
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return b
}()

// opsCur: アプリの投稿期間の最新の月 (YYYY-MM)。opsLoop が DB から更新する。空 = 不明 (過去月に数えない)
var opsCur atomic.Value

func opsCID(token string) string {
	m := hmac.New(sha256.New, opsKey)
	m.Write([]byte(token))
	return hex.EncodeToString(m.Sum(nil))[:8]
}

type opsSpanKey struct{}

// opsSpan: 1 要求の待ちの内訳 (ns)。
type opsSpan struct {
	mu sync.Mutex
	d  [opsN]time.Duration
}

const (
	opsSlot    = iota // GP fair / post cap の枠待ち
	opsPersist        // store の PG 待ち
	opsRepl           // store の peer ACK 待ち
	opsFwdPrio        // FwdPriority の枠待ち
	opsImgSync        // 画像の sync 待ち (asia localimg)
	opsApply          // store の apply (writePrio の往復)
	opsN
)

var opsKindName = [opsN]string{"slot", "pg", "ack", "fwdprio", "imgsync", "apply"}

// opsAdd: ctx に span があれば待ち時間を足す (OPS_TRACE off なら span は無い)。
func opsAdd(ctx context.Context, kind int, d time.Duration) {
	sp, ok := ctx.Value(opsSpanKey{}).(*opsSpan)
	if !ok {
		return
	}
	if kind < 0 || kind >= opsN {
		return
	}
	sp.mu.Lock()
	sp.d[kind] += d
	sp.mu.Unlock()
}

type opsUser struct {
	loginAt      time.Time // login の応答
	loginStartAt time.Time // login の要求の開始
	posted       bool
	seen         time.Time
}

var ops struct {
	mu        sync.Mutex
	users     map[string]*opsUser
	loginMem  []time.Duration // login の応答 → 過去月 Memories
	loginMemS []time.Duration // login の開始 → 過去月 Memories
	loginDur  []time.Duration // login の開始 → 応答
	views     int
	feeds     int
	realmojis int
	comments  int
	waits     map[string]*[opsN][]time.Duration // region -> 段ごと
	started   bool
}

// opsTee: login の応答本文を (最大 4KB) 写す。書き込みはそのまま通す。
type opsTee struct {
	http.ResponseWriter
	code int
	buf  []byte
}

func (t *opsTee) WriteHeader(code int) {
	if t.code == 0 {
		t.code = code
	}
	t.ResponseWriter.WriteHeader(code)
}

func (t *opsTee) Write(b []byte) (int, error) {
	if t.code == 0 {
		t.code = http.StatusOK
	}
	if room := 4096 - len(t.buf); room > 0 {
		t.buf = append(t.buf, b[:min(room, len(b))]...)
	}
	return t.ResponseWriter.Write(b)
}

func (t *opsTee) Unwrap() http.ResponseWriter { return t.ResponseWriter }

// opsPastMonth: 要求のパラメータ (month / date) がアプリの投稿期間の最新の月 (cur) より前か。
func opsPastMonth(req *http.Request, cur string) bool {
	if len(cur) != 7 {
		return false
	}
	p := req.URL.Path
	if p == "/api/memories" {
		m := req.URL.Query().Get("month")
		return len(m) == 7 && m < cur
	}
	if d, ok := strings.CutPrefix(p, "/api/memories/"); ok && len(d) >= 7 {
		return d[:7] < cur
	}
	return false
}

func opsTraceMiddleware(h *HandlerContext) echo.MiddlewareFunc {
	return opsTraceMiddlewareFn(func() *HandlerContext { return h })
}

// opsTraceMiddlewareFn: h は要求の時点で引く (Pre に先に登録するため)。nil の間は何もしない。
func opsTraceMiddlewareFn(getH func() *HandlerContext) echo.MiddlewareFunc {
	ops.mu.Lock()
	if !ops.started {
		ops.started = true
		ops.users = map[string]*opsUser{}
		ops.waits = map[string]*[opsN][]time.Duration{}
		go opsLoop(getH)
	}
	ops.mu.Unlock()
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			h := getH()
			if h == nil {
				return next(c)
			}
			start := time.Now()
			req := c.Request()
			p := req.URL.Path
			if !strings.HasPrefix(p, "/api/") || strings.HasPrefix(p, "/api/images/") || p == "/api/notifications" || strings.HasPrefix(p, "/api/admin/") {
				return next(c)
			}
			isLogin := req.Method == http.MethodPost && p == "/api/auth/login"
			write := req.Method != http.MethodGet && req.Method != http.MethodHead
			var sp *opsSpan
			if write {
				sp = &opsSpan{}
				c.SetRequest(req.WithContext(context.WithValue(req.Context(), opsSpanKey{}, sp)))
			}
			var tee *opsTee
			if isLogin {
				tee = &opsTee{ResponseWriter: c.Response()}
				c.SetResponse(tee)
			}
			err := next(c)
			if tee != nil {
				c.SetResponse(tee.ResponseWriter)
			}
			c.SetRequest(req)
			now := time.Now()
			var status int
			if tee != nil {
				status = tee.code
			} else if r, uerr := echo.UnwrapResponse(c.Response()); uerr == nil {
				status = r.Status
			}
			// エラーを返した要求は応答がまだ書かれていない (Status は既定値のまま): 完了に数えない
			ok := err == nil && status >= 200 && status < 300
			if sp != nil {
				region := requestRegion(h, c)
				sp.mu.Lock()
				w := sp.d
				sp.mu.Unlock()
				ops.mu.Lock()
				ws := ops.waits[region]
				if ws == nil {
					ws = &[opsN][]time.Duration{}
					ops.waits[region] = ws
				}
				for i := range w {
					if w[i] > 0 && len(ws[i]) < 100000 {
						ws[i] = append(ws[i], w[i])
					}
				}
				ops.mu.Unlock()
			}
			if !ok {
				return err
			}
			var cid string
			if isLogin {
				var lr struct {
					Token string `json:"token"`
				}
				if json.Unmarshal(tee.buf, &lr) == nil && lr.Token != "" {
					cid = opsCID(lr.Token)
				}
			} else if tok, found := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer "); found && tok != "" {
				cid = opsCID(tok)
			}
			if cid == "" {
				return err
			}
			cur, _ := opsCur.Load().(string)
			ops.mu.Lock()
			defer ops.mu.Unlock()
			u := ops.users[cid]
			if isLogin {
				if u == nil {
					if len(ops.users) > 200000 {
						ops.users = map[string]*opsUser{}
					}
					u = &opsUser{}
					ops.users[cid] = u
				}
				u.loginAt, u.loginStartAt, u.posted, u.seen = now, start, false, now
				if len(ops.loginDur) < 100000 {
					ops.loginDur = append(ops.loginDur, now.Sub(start))
				}
				return err
			}
			if u == nil {
				return err
			}
			u.seen = now
			switch {
			case req.Method == http.MethodGet && opsPastMonth(req, cur):
				if !u.loginAt.IsZero() {
					if len(ops.loginMem) < 100000 {
						ops.loginMem = append(ops.loginMem, now.Sub(u.loginAt))
						ops.loginMemS = append(ops.loginMemS, now.Sub(u.loginStartAt))
					}
					u.loginAt, u.loginStartAt = time.Time{}, time.Time{}
				}
			case req.Method == http.MethodPost && p == "/api/posts":
				u.posted = true
			case !u.posted:
			case req.Method == http.MethodGet && strings.HasPrefix(p, "/api/feeds/"):
				ops.feeds++
			case req.Method == http.MethodGet && strings.HasPrefix(p, "/api/posts/") && strings.Count(p, "/") == 3 && p != "/api/posts/me":
				ops.views++
			case req.Method == http.MethodPost && strings.HasSuffix(p, "/realmojis"):
				ops.realmojis++
			case req.Method == http.MethodPost && strings.HasSuffix(p, "/comments"):
				ops.comments++
			}
			return err
		}
	}
}

func opsPct(xs []time.Duration, q float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	return xs[int(float64(len(xs)-1)*q)]
}

// opsRefreshCur: アプリの投稿期間の最新の月を DB から読む (実時間の月は使わない)。
func opsRefreshCur(h *HandlerContext) {
	if h == nil || h.reads == nil {
		return
	}
	db := h.Reader()
	if db == nil {
		return
	}
	var m *string
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.GetContext(ctx, &m, `SELECT to_char(max(local_date), 'YYYY-MM') FROM posting_windows`); err == nil && m != nil {
		opsCur.Store(*m)
	}
}

func opsLoop(getH func() *HandlerContext) {
	for range time.Tick(10 * time.Second) {
		opsRefreshCur(getH())
		ops.mu.Lock()
		lm, lms, ld := ops.loginMem, ops.loginMemS, ops.loginDur
		ops.loginMemS, ops.loginDur = nil, nil
		v, f, r, cm := ops.views, ops.feeds, ops.realmojis, ops.comments
		waits := ops.waits
		ops.loginMem, ops.views, ops.feeds, ops.realmojis, ops.comments = nil, 0, 0, 0, 0
		ops.waits = map[string]*[opsN][]time.Duration{}
		cut := time.Now().Add(-10 * time.Minute)
		for k, u := range ops.users {
			if u.seen.Before(cut) {
				delete(ops.users, k)
			}
		}
		nUsers := len(ops.users)
		ops.mu.Unlock()
		attrs := []any{"ids", nUsers, "login_memories_n", len(lm), "login_memories_p50", opsPct(lm, 0.5), "login_memories_p90", opsPct(lm, 0.9),
			"login_start_memories_p50", opsPct(lms, 0.5), "login_start_memories_p90", opsPct(lms, 0.9),
			"login_n", len(ld), "login_p50", opsPct(ld, 0.5), "login_p90", opsPct(ld, 0.9), "cur_month", opsCurString(),
			"after_post_views", v, "after_post_feeds", f, "after_post_realmojis", r, "after_post_comments", cm}
		regions := make([]string, 0, len(waits))
		for k := range waits {
			regions = append(regions, k)
		}
		sort.Strings(regions)
		for _, rg := range regions {
			ws := waits[rg]
			for i := range opsN {
				if len(ws[i]) > 0 {
					attrs = append(attrs, rg+"_"+opsKindName[i]+"_p50", opsPct(ws[i], 0.5), rg+"_"+opsKindName[i]+"_p90", opsPct(ws[i], 0.9), rg+"_"+opsKindName[i]+"_n", len(ws[i]))
				}
			}
		}
		slog.Info("ops-trace", attrs...)
	}
}

func opsCurString() string { s, _ := opsCur.Load().(string); return s }
