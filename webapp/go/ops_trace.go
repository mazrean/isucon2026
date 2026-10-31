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
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_OPS_TRACE=1 (既定 0、deploy の OPS_TRACE で asia1 だけ): 一連の操作を匿名の相関 ID で追う性能の観測。
// 応答は変えない (読むだけ)。認証情報・生のトークンは保存しない: 相関 ID = HMAC-SHA256(起動ごとの乱数鍵, token) の
// hex 先頭 8 文字。login のトークンは応答本文から HMAC にしてすぐ捨てる。10 秒ごとに slog "ops-trace" で:
//   - login → 過去月の Memories (month / date が今月より前) の 2xx までの時間 p50 / p90
//   - 投稿 (POST /api/posts 2xx) の後に同じ ID で完了した (2xx) 閲覧 (投稿の詳細 / feed)、RealMoji、コメントの数
//   - 書き込みの待ちの地域ごとの p50: 枠待ち (GP fair / post cap の slot)、永続化 (store の PG 書き戻し待ち)、
//     複製待ち (store の peer ack 待ち)

var opsTraceOn = os.Getenv("ISUREAL_OPS_TRACE") == "1"

var opsKey = func() []byte {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return b
}()

func opsCID(token string) string {
	m := hmac.New(sha256.New, opsKey)
	m.Write([]byte(token))
	return hex.EncodeToString(m.Sum(nil))[:8]
}

type opsSpanKey struct{}

// opsSpan: 1 要求の待ちの内訳 (ns)。
type opsSpan struct {
	mu                  sync.Mutex
	slot, persist, repl time.Duration
}

const (
	opsSlot = iota
	opsPersist
	opsRepl
)

// opsAdd: ctx に span があれば待ち時間を足す (OPS_TRACE off なら span は無い)。
func opsAdd(ctx context.Context, kind int, d time.Duration) {
	sp, ok := ctx.Value(opsSpanKey{}).(*opsSpan)
	if !ok {
		return
	}
	sp.mu.Lock()
	switch kind {
	case opsSlot:
		sp.slot += d
	case opsPersist:
		sp.persist += d
	case opsRepl:
		sp.repl += d
	}
	sp.mu.Unlock()
}

type opsUser struct {
	loginAt time.Time
	posted  bool
	seen    time.Time
}

var ops struct {
	mu        sync.Mutex
	users     map[string]*opsUser
	loginMem  []time.Duration
	views     int
	feeds     int
	realmojis int
	comments  int
	waits     map[string]*[3][]time.Duration // region -> slot/persist/repl
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

func opsPastMonth(req *http.Request, now time.Time) bool {
	cur := now.UTC().Format("2006-01")
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
	ops.mu.Lock()
	if !ops.started {
		ops.started = true
		ops.users = map[string]*opsUser{}
		ops.waits = map[string]*[3][]time.Duration{}
		go opsLoop()
	}
	ops.mu.Unlock()
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
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
				w := [3]time.Duration{sp.slot, sp.persist, sp.repl}
				sp.mu.Unlock()
				ops.mu.Lock()
				ws := ops.waits[region]
				if ws == nil {
					ws = &[3][]time.Duration{}
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
				u.loginAt, u.posted, u.seen = now, false, now
				return err
			}
			if u == nil {
				return err
			}
			u.seen = now
			switch {
			case req.Method == http.MethodGet && opsPastMonth(req, now):
				if !u.loginAt.IsZero() {
					if len(ops.loginMem) < 100000 {
						ops.loginMem = append(ops.loginMem, now.Sub(u.loginAt))
					}
					u.loginAt = time.Time{}
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

func opsLoop() {
	for range time.Tick(10 * time.Second) {
		ops.mu.Lock()
		lm := ops.loginMem
		v, f, r, cm := ops.views, ops.feeds, ops.realmojis, ops.comments
		waits := ops.waits
		ops.loginMem, ops.views, ops.feeds, ops.realmojis, ops.comments = nil, 0, 0, 0, 0
		ops.waits = map[string]*[3][]time.Duration{}
		cut := time.Now().Add(-10 * time.Minute)
		for k, u := range ops.users {
			if u.seen.Before(cut) {
				delete(ops.users, k)
			}
		}
		nUsers := len(ops.users)
		ops.mu.Unlock()
		attrs := []any{"ids", nUsers, "login_memories_n", len(lm), "login_memories_p50", opsPct(lm, 0.5), "login_memories_p90", opsPct(lm, 0.9),
			"after_post_views", v, "after_post_feeds", f, "after_post_realmojis", r, "after_post_comments", cm}
		regions := make([]string, 0, len(waits))
		for k := range waits {
			regions = append(regions, k)
		}
		sort.Strings(regions)
		for _, rg := range regions {
			ws := waits[rg]
			attrs = append(attrs, rg+"_slot_p50", opsPct(ws[0], 0.5), rg+"_persist_p50", opsPct(ws[1], 0.5), rg+"_repl_p50", opsPct(ws[2], 0.5),
				rg+"_n", [3]int{len(ws[0]), len(ws[1]), len(ws[2])})
		}
		slog.Info("ops-trace", attrs...)
	}
}
