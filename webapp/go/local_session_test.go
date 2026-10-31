package isureal

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

func lsTestStore(t *testing.T) (*memStore, string) {
	t.Helper()
	m := &memStore{d: newMemData()}
	sum := sha256.Sum256([]byte("secret-1"))
	m.d.users["u1"] = &memUser{User: User{ID: "u1", Username: "alice", SecretKeyHash: hex.EncodeToString(sum[:]), Status: "active", Region: "europe"}}
	m.d.sortedDirty = true
	th := sha256.Sum256([]byte("tok-1"))
	m.d.sessions[hex.EncodeToString(th[:])] = memSession{UserID: "u1", CreatedAt: time.Now()}
	m.ready.Store(true)
	return m, "tok-1"
}

func lsSession(m *memStore, token string) (*Session, bool) {
	req := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	c := echo.New().NewContext(req, httptest.NewRecorder())
	return m.sessionFromHeader(c)
}

// 印の有無で、メモリが「有効」と答えるか DB に回すかが切り替わる (有効・無効の差分)。
func TestLocalSessionMarkDiff(t *testing.T) {
	m, tok := lsTestStore(t)
	if s, ok := lsSession(m, tok); !ok || s == nil || s.UserID != "u1" || s.Region != "europe" {
		t.Fatalf("unmarked: want valid from memory, got %+v %v", s, ok)
	}
	if _, ok := lsSession(m, "unknown"); ok {
		t.Fatal("unknown token must fall back to DB")
	}
	m.marks.add("u1")
	m.marks.add("u1")
	if _, ok := lsSession(m, tok); ok {
		t.Fatal("marked user: memory must not answer valid (DB decides)")
	}
	m.marks.add("u2")
	m.marks.done("u2")
	m.marks.done("u1")
	if _, ok := lsSession(m, tok); ok {
		t.Fatal("one of two marks still pending: must stay on DB path")
	}
	m.marks.done("u1")
	if _, ok := lsSession(m, tok); !ok {
		t.Fatal("all marks done: memory answers again")
	}
	m.marks.done("u1") // 余分な done は無害
	if m.marks.size.Load() != 0 {
		t.Fatal("marks not empty")
	}

	// done が届かなくても TTL で外れる
	base := time.Now()
	memNow = func() time.Time { return base }
	defer func() { memNow = time.Now }()
	m.marks.add("u1")
	if _, ok := lsSession(m, tok); ok {
		t.Fatal("marked")
	}
	memNow = func() time.Time { return base.Add(sessMarkTTL + time.Second) }
	if _, ok := lsSession(m, tok); !ok {
		t.Fatal("mark must expire after TTL")
	}
}

func TestLocalSessionSQL(t *testing.T) {
	for _, bad := range []string{"", "a'b", "a;b", "a b", `a\b`, strings.Repeat("a", 129)} {
		if _, ok := pgLit(bad); ok {
			t.Fatalf("pgLit(%q) accepted", bad)
		}
	}
	if _, ok := localLoginSQL("u'1", "h", "s", "t"); ok {
		t.Fatal("quote accepted")
	}
	q, ok := localLoginSQL("0b3c-uuid", "abcdef", "sid", "thash")
	if !ok || strings.Count(q, ";") != 2 || !strings.Contains(q, "FOR NO KEY UPDATE") ||
		!strings.Contains(q, "reason = 'replaced'") || !strings.Contains(q, "status = 'active'") {
		t.Fatalf("login sql: %s", q)
	}
	q, ok = localLogoutSQL("u1", "thash")
	if !ok || strings.Count(q, ";") != 1 || !strings.Contains(q, "reason = 'logout'") || !strings.Contains(q, "token_hash = 'thash'") {
		t.Fatalf("logout sql: %s", q)
	}
}

// nginx の印 → 地域内で処理 / 転送 の振り分け。
func TestLocalSessionGating(t *testing.T) {
	var fwd atomic.Int32
	var fwdRegion atomic.Value
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fwd.Add(1)
		fwdRegion.Store(r.Header.Get(forwardedRegionHeader))
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusTeapot)
	}))
	defer target.Close()

	oldSecret, oldTarget, oldOn, oldWM, oldLR := forwardSecret, prevalidateTarget, localSessionOn, memWatermark, memLogReplEnabled
	defer func() {
		forwardSecret, prevalidateTarget, localSessionOn, memWatermark, memLogReplEnabled = oldSecret, oldTarget, oldOn, oldWM, oldLR
	}()
	forwardSecret = "s3cret"
	prevalidateTarget = strings.TrimPrefix(target.URL, "http://")
	localSessionOn, memWatermark, memLogReplEnabled = true, true, false
	t.Setenv("ISUREAL_MEM_PUMP", "127.0.0.1:1")

	m, _ := lsTestStore(t)
	m.useLease = true
	m.leaseUntil.Store(time.Now().Add(time.Hour).UnixNano())
	m.w.pumpWM.Store(true)
	h := &HandlerContext{Config: &Config{Region: "europe"}, Mem: m}
	e := echo.New()
	e.HTTPErrorHandler = APIErrorHandler
	e.Use(middleware.Recover()) // 秘密が違う → 通常の DB 経路 (テストでは DB が無い)
	registerLocalSession(e, h)
	e.POST("/api/auth/login", loginHandler(h))

	do := func(body, secret string) int {
		req := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = "127.0.0.1:5555"
		if secret != "" {
			req.Header.Set(localSessionHeader, secret)
		}
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec.Code
	}

	// 地域内: 誤ったパスワードは DB も転送も使わずに 401
	if c := do(`{"username":"alice","secretKey":"wrong"}`, "s3cret"); c != http.StatusUnauthorized || fwd.Load() != 0 {
		t.Fatalf("wrong password: code=%d fwd=%d", c, fwd.Load())
	}
	// メモリに居ないユーザーは転送 (本文はそのまま、地域ヘッダー付き)
	if c := do(`{"username":"bob","secretKey":"x"}`, "s3cret"); c != http.StatusTeapot || fwd.Load() != 1 || fwdRegion.Load() != "europe" {
		t.Fatalf("unknown user: code=%d fwd=%d region=%v", c, fwd.Load(), fwdRegion.Load())
	}
	// pump が wm モードでない → 地域内では確定できない → 転送
	m.w.pumpWM.Store(false)
	if c := do(`{"username":"alice","secretKey":"wrong"}`, "s3cret"); c != http.StatusTeapot || fwd.Load() != 2 {
		t.Fatalf("ack mode: code=%d fwd=%d", c, fwd.Load())
	}
	m.w.pumpWM.Store(true)
	// フラグ off → 転送
	localSessionOn = false
	if c := do(`{"username":"alice","secretKey":"wrong"}`, "s3cret"); c != http.StatusTeapot || fwd.Load() != 3 {
		t.Fatalf("flag off: code=%d fwd=%d", c, fwd.Load())
	}
	localSessionOn = true
	// 秘密が違う印は無視 (通常の経路)
	if c := do(`{"username":"bob","secretKey":"x"}`, "nope"); c == http.StatusTeapot || fwd.Load() != 3 {
		t.Fatalf("bad secret must not forward: code=%d fwd=%d", c, fwd.Load())
	}
}

// asia1 側の sess-mark: 秘密の無い要求は拒否、pre で印が付く、store が有効なら拒否。
func TestLocalSessionMarkRoute(t *testing.T) {
	oldSecret := forwardSecret
	defer func() { forwardSecret = oldSecret }()
	forwardSecret = "s3cret"
	m, tok := lsTestStore(t)
	p := &memPump{m: m}
	h := &HandlerContext{Config: &Config{Region: "asia"}}
	e := echo.New()
	registerSessMarkRoute(e, h, p)
	post := func(body, secret string) int {
		req := httptest.NewRequest(http.MethodPost, "/internal/mem/sess-mark", strings.NewReader(body))
		req.RemoteAddr = "127.0.0.1:5555"
		req.Header.Set(forwardedSecretHeader, secret)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec.Code
	}
	if c := post(`{"u":"u1"}`, "nope"); c != http.StatusForbidden {
		t.Fatalf("no secret: %d", c)
	}
	if _, ok := lsSession(m, tok); !ok {
		t.Fatal("rejected request must not mark")
	}
	if c := post(`{"u":"u1"}`, "s3cret"); c != http.StatusOK {
		t.Fatalf("pre: %d", c)
	}
	if _, ok := lsSession(m, tok); ok {
		t.Fatal("after pre the old token must go to DB on asia1")
	}
	h.Store = &storeEngine{}
	if c := post(`{"u":"u1"}`, "s3cret"); c != http.StatusConflict {
		t.Fatalf("store on: %d", c)
	}
}
