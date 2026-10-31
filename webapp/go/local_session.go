package isureal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

// ISUREAL_LOCAL_SESSION=1 (europe1/america1): POST /api/auth/login と /api/auth/logout を
// asia1 に転送せず、その地域で確定する。
//
// nginx は eu/am の login/logout を、X-Isureal-Local-Session=<secret> を付けてローカルの app に送る
// (prevalidate と同じ形)。app はできるときだけ地域内で処理し、できないときは今までどおり
// FWD_TARGET へ転送する (X-Isureal-Fwd-* を付ける。newPrevalidateProxy を流用)。
//
// 確定の手順 (1 往復ぶん):
//  1. login はユーザー名 → user_id とパスワードのハッシュを mem で引き、違えば 401 (ハッシュは不変)。
//     mem に居ないユーザーは転送する。
//  2. 次の 2 つを並行に送り、両方を待つ:
//     a. PG primary (asia2) に multi-statement の simple query を 1 本 (暗黙のトランザクション、
//        文ごとに新しいスナップショット): ユーザー行を FOR NO KEY UPDATE → 古いセッションを無効化
//        (login: replaced / logout: 対象トークン) → login なら INSERT。status は DB で確かめる。
//     b. asia1 の pump に「このユーザーのセッションは DB で確かめよ」の印 (sess-mark) を付ける。
//  3. 両方成功したら 2xx。そのあと asia1 に done を送る (待たない)。asia1 は done を受けてから
//     始まったサイクル (= このコミットを含む) が終わったら印を外す。
//
// 権限を減らす書き込み (logout、login の replaced) の保証:
//   - asia1 (pump のメモリ。転送された eu/am の書き込みもここで認証する): 2xx の前に印が付いているので、
//     メモリに古いトークンが残っていても DB で確かめる → 401。印は pump がコミットを取り込むまで残る。
//   - europe1/america1 の GET: watermark (mem_watermark.go)。W は「W より前に COMMIT の応答が返った
//     トランザクションの変更は seq(W) までに含まれる」で、コミットした主体を問わない。2xx < T_r <= W なので、
//     メモリで答える時点で無効化は反映済み (届かなければ DB 経路)。pump が wm モードのときだけ地域内で処理する。
//   - eu/am のこの経路自身 (logout の認証): middleware はメモリを見るが、無効化の可否は UPDATE の行数で決める
//     (0 行 = 既に無効 → 401)。
// 新しいセッション: asia1 / peer のメモリに無ければ sessionFromHeader が DB を引く (既存の安全側の経路)。

const localSessionHeader = "X-Isureal-Local-Session"

var (
	localSessionOn   = os.Getenv("ISUREAL_LOCAL_SESSION") == "1"
	localSessionHits atomic.Int64
	localSessionFwd  atomic.Int64
)

type localSessionKey struct{}

type localSessionReq struct {
	proxy *httputil.ReverseProxy
	body  []byte
	pump  string
}

func localSessionPath(req *http.Request) bool {
	return req.Method == http.MethodPost && (req.URL.Path == "/api/auth/login" || req.URL.Path == "/api/auth/logout")
}

// localSessionReady: 地域内で確定してよい構成か (peer、wm モードの pump、メモリが使える)。
func localSessionReady(h *HandlerContext) bool {
	m := h.Mem
	return localSessionOn && m != nil && m.useLease && memWatermark && !memLogReplEnabled &&
		h.Store == nil && m.on() && m.w.pumpWM.Load()
}

// registerLocalSession は Pre に入れる (nginx が付けた印を見て、地域内で処理するか転送するかを決める)。
func registerLocalSession(e *echo.Echo, h *HandlerContext) {
	if forwardSecret == "" {
		return
	}
	var proxy *httputil.ReverseProxy
	if prevalidateTarget != "" {
		proxy = newPrevalidateProxy(prevalidateTarget, h.Config.Region)
	}
	pump := os.Getenv("ISUREAL_MEM_PUMP")
	if localSessionOn {
		go func() {
			for range time.Tick(10 * time.Second) {
				hits, fwd := localSessionHits.Swap(0), localSessionFwd.Swap(0)
				if hits > 0 || fwd > 0 {
					slog.Info("local session", "local", hits, "forwarded", fwd)
				}
			}
		}()
	}
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			v := req.Header.Get(localSessionHeader)
			if v == "" {
				return next(c)
			}
			req.Header.Del(localSessionHeader)
			if subtle.ConstantTimeCompare([]byte(v), []byte(forwardSecret)) != 1 || !prevalidatePeer(req.RemoteAddr) || !localSessionPath(req) {
				return next(c)
			}
			if proxy == nil {
				// nginx は転送の代わりにここへ送ってきた。転送先が無いのは設定の誤り。
				return c.NoContent(http.StatusBadGateway)
			}
			var buf bytes.Buffer
			if _, err := buf.ReadFrom(io.LimitReader(req.Body, 1<<20)); err != nil {
				return err
			}
			req.Body.Close()
			ls := &localSessionReq{proxy: proxy, body: buf.Bytes(), pump: pump}
			if pump == "" || !localSessionReady(h) {
				return ls.forward(c)
			}
			ls.resetBody(req)
			c.Set("localSession", ls)
			return next(c)
		}
	})
}

func getLocalSession(c *echo.Context) *localSessionReq {
	ls, _ := c.Get("localSession").(*localSessionReq)
	return ls
}

func (ls *localSessionReq) resetBody(req *http.Request) {
	body := ls.body
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	req.TransferEncoding = nil
}

// forward: 今までどおり asia1 (FWD_TARGET) へ転送する。
func (ls *localSessionReq) forward(c *echo.Context) error {
	localSessionFwd.Add(1)
	req := c.Request()
	ls.resetBody(req)
	ls.proxy.ServeHTTP(c.Response(), req)
	return nil
}

// ---- 印 (asia1 の pump 側) ----

const sessMarkTTL = 30 * time.Second // done が届かなかったときの漏れ止め (安全側に倒れるだけ)

type sessMark struct {
	n     int
	until int64
}

// sessMarks: user_id → 「このユーザーのセッションはメモリで有効と答えず DB で確かめる」。
type sessMarks struct {
	size atomic.Int32
	mu   sync.Mutex
	m    map[string]sessMark
}

func (s *sessMarks) add(userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string]sessMark{}
	}
	e := s.m[userID]
	e.n++
	e.until = memNow().Add(sessMarkTTL).UnixNano()
	s.m[userID] = e
	s.size.Store(int32(len(s.m)))
}

func (s *sessMarks) done(userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[userID]
	if !ok {
		return
	}
	e.n--
	if e.n <= 0 {
		delete(s.m, userID)
	} else {
		s.m[userID] = e
	}
	s.size.Store(int32(len(s.m)))
}

func (s *sessMarks) marked(userID string) bool {
	if s.size.Load() == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[userID]
	if !ok {
		return false
	}
	if memNow().UnixNano() > e.until {
		delete(s.m, userID)
		s.size.Store(int32(len(s.m)))
		return false
	}
	return e.n > 0
}

type sessMarkMsg struct {
	U string `json:"u"`
	D bool   `json:"d"` // false = 印を付ける (2xx の前)、true = コミット済み (取り込んだら外す)
}

func registerSessMarkRoute(e *echo.Echo, h *HandlerContext, p *memPump) {
	e.POST("/internal/mem/sess-mark", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		if h.Store != nil {
			// store はメモリが正本で、外からの PG 書き込みを前提にしていない
			return c.NoContent(http.StatusConflict)
		}
		var msg sessMarkMsg
		if err := json.NewDecoder(c.Request().Body).Decode(&msg); err != nil || msg.U == "" {
			return c.NoContent(http.StatusBadRequest)
		}
		if !msg.D {
			p.m.marks.add(msg.U)
			return c.NoContent(http.StatusOK)
		}
		go func() {
			// done はコミットの応答の後に送られる → この後に始まるサイクルのスナップショットに含まれる
			p.barrierScope(false)
			p.m.marks.done(msg.U)
		}()
		return c.NoContent(http.StatusOK)
	})
}

// ---- 地域内の login / logout (eu/am) ----

// pgIdent: SQL に埋め込む値 (生成した UUID / hex、メモリの user_id) の文字種を限る。
func pgLit(s string) (string, bool) {
	if s == "" || len(s) > 128 {
		return "", false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '-' || ch == '_') {
			return "", false
		}
	}
	return "'" + s + "'", true
}

func localLoginSQL(userID, secretHash, sessionID, tokenHash string) (string, bool) {
	u, ok1 := pgLit(userID)
	h, ok2 := pgLit(secretHash)
	s, ok3 := pgLit(sessionID)
	t, ok4 := pgLit(tokenHash)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return "", false
	}
	cond := fmt.Sprintf(`id = %s AND status = 'active' AND secret_key_hash = %s`, u, h)
	return fmt.Sprintf(lockUserRowSQL(`SELECT id, username, display_name, bio, region, account_type, status FROM users WHERE %[1]s`, userAdvSession)+`;
UPDATE login_sessions SET active = false, invalidated_at = clock_timestamp(), reason = 'replaced' WHERE user_id = %[2]s AND active = true AND EXISTS (SELECT 1 FROM users WHERE %[1]s);
INSERT INTO login_sessions (id, user_id, token_hash) SELECT %[3]s, id, %[4]s FROM users WHERE %[1]s`, cond, u, s, t), true
}

func localLogoutSQL(userID, tokenHash string) (string, bool) {
	u, ok1 := pgLit(userID)
	t, ok2 := pgLit(tokenHash)
	if !ok1 || !ok2 {
		return "", false
	}
	return fmt.Sprintf(lockUserRowSQL(`SELECT id FROM users WHERE id = %[1]s AND status = 'active'`, userAdvSession)+`;
UPDATE login_sessions SET active = false, invalidated_at = clock_timestamp(), reason = 'logout' WHERE user_id = %[1]s AND active = true AND token_hash = %[2]s AND EXISTS (SELECT 1 FROM users WHERE id = %[1]s AND status = 'active')`, u, t), true
}

// execMulti: simple protocol で 1 往復 (複数文 = 暗黙の 1 トランザクション)。
func execMulti(ctx context.Context, db *sqlx.DB, q string) ([]*pgconn.Result, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var res []*pgconn.Result
	err = conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			return errors.New("local session: not a pgx connection")
		}
		var err error
		res, err = sc.Conn().PgConn().Exec(ctx, q).ReadAll()
		return err
	})
	return res, err
}

// userForLogin: mem からユーザー名で引く (sortedUsers は dirty なら書き換えるので、読みロックでは使わない)。
func (m *memStore) userForLogin(username string) (id, secretHash string, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d := m.d
	if !d.sortedDirty && d.sorted != nil {
		list := d.sorted
		i := sort.Search(len(list), func(i int) bool { return list[i].Username >= username })
		if i < len(list) && list[i].Username == username {
			return list[i].ID, list[i].SecretKeyHash, true
		}
		return "", "", false
	}
	for _, u := range d.users {
		if u.Username == username {
			return u.ID, u.SecretKeyHash, true
		}
	}
	return "", "", false
}

func (ls *localSessionReq) mark(userID string, done bool) error {
	_, err := memHTTPPost(ls.pump, "/internal/mem/sess-mark", sessMarkMsg{U: userID, D: done}, 3*time.Second)
	return err
}

// commit: 印と PG の書き込みを並行に行い、両方を待つ。
func (ls *localSessionReq) commit(ctx context.Context, h *HandlerContext, userID, q string) ([]*pgconn.Result, error, error) {
	markErr := make(chan error, 1)
	go func() { markErr <- ls.mark(userID, false) }()
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, dbErr := execMulti(dctx, h.DB, q)
	mErr := <-markErr
	// 成否にかかわらず done を送る (印を外すのは pump がコミットを取り込んだ後)
	go func() { _ = ls.mark(userID, true) }()
	return res, dbErr, mErr
}

func (ls *localSessionReq) login(c *echo.Context, h *HandlerContext, input loginRequest) error {
	id, secretHash, ok := h.Mem.userForLogin(input.Username)
	if !ok {
		return ls.forward(c)
	}
	hash := sha256.Sum256([]byte(input.SecretKey))
	if secretHash != hex.EncodeToString(hash[:]) {
		return NewAPIError(http.StatusUnauthorized, "invalid_credentials")
	}
	token := uuid.NewString()
	sum := sha256.Sum256([]byte(token))
	tokenHash := hex.EncodeToString(sum[:])
	q, ok := localLoginSQL(id, secretHash, uuid.NewString(), tokenHash)
	if !ok {
		return ls.forward(c)
	}
	res, dbErr, mErr := ls.commit(c.Request().Context(), h, id, q)
	if dbErr != nil {
		return dbErr
	}
	if len(res) != 3 {
		return fmt.Errorf("local login: %d results", len(res))
	}
	if len(res[0].Rows) == 0 || res[2].CommandTag.RowsAffected() != 1 {
		return NewAPIError(http.StatusUnauthorized, "invalid_credentials")
	}
	if mErr != nil {
		// 古いトークンが asia1 で 401 になる保証が取れない → 2xx を返さない
		return fmt.Errorf("local login: sess-mark: %w", mErr)
	}
	r := res[0].Rows[0]
	if len(r) != 7 {
		return fmt.Errorf("local login: %d columns", len(r))
	}
	localSessionHits.Add(1)
	return c.JSON(http.StatusOK, loginResponse{Token: token, User: sessionUserResponse{
		ID: string(r[0]), Username: string(r[1]), DisplayName: string(r[2]), Bio: string(r[3]),
		Region: string(r[4]), AccountType: string(r[5]), Status: string(r[6]),
	}})
}

func (ls *localSessionReq) logout(c *echo.Context, h *HandlerContext, session *Session) error {
	q, ok := localLogoutSQL(session.UserID, session.TokenHash)
	if !ok {
		return ls.forward(c)
	}
	res, dbErr, mErr := ls.commit(c.Request().Context(), h, session.UserID, q)
	if dbErr != nil {
		return dbErr
	}
	if len(res) != 2 {
		return fmt.Errorf("local logout: %d results", len(res))
	}
	if res[1].CommandTag.RowsAffected() == 0 {
		// メモリでは有効に見えたが DB では既に無効 (または退会済み)
		return NewAPIError(http.StatusUnauthorized, "invalid_session")
	}
	if mErr != nil {
		return fmt.Errorf("local logout: sess-mark: %w", mErr)
	}
	localSessionHits.Add(1)
	return c.NoContent(http.StatusNoContent)
}
