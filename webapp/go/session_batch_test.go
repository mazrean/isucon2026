package isureal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// ISUREAL_TEST_PG_RW_DSN='postgres://isureal:isureal@127.0.0.1:15433/isureal_lb?sslmode=disable' go test -run SessionBatch .

func sessionBatchSeed(t *testing.T, db *sqlx.DB) (userID, username, secret string) {
	userID = uuid.NewString()
	username = "u" + userID[:8]
	secret = uuid.NewString()
	h := sha256.Sum256([]byte(secret))
	db.MustExec(`INSERT INTO users (id, username, secret_key_hash, display_name, region) VALUES ($1, $2, $3, 'disp', 'asia')`,
		userID, username, hex.EncodeToString(h[:]))
	return
}

func tokenHashOf(tok string) string {
	s := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(s[:])
}

// 従来の DB 経路 (loginHandler の tx) を関数にしたもの。比較用。
func legacyLogin(ctx context.Context, db *sqlx.DB, username, secret string) (*Session, error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var user User
	if err := tx.GetContext(ctx, &user, lockUserRowSQL(`SELECT id, username, secret_key_hash, display_name, bio, profile_image_id, region, account_type, status FROM users WHERE username = $1`, userAdvSession), username); err != nil {
		return nil, err
	}
	h := sha256.Sum256([]byte(secret))
	if user.Status != "active" || user.SecretKeyHash != hex.EncodeToString(h[:]) {
		return nil, errLoginInvalid
	}
	s, err := createLoginSessionTx(ctx, tx, user.ID, true)
	if err != nil {
		return nil, err
	}
	return s, tx.Commit()
}

type sessState struct {
	total, active int
	activeHash    string
}

func sessionStateOf(t *testing.T, db *sqlx.DB, userID string) sessState {
	var st sessState
	if err := db.Get(&st.total, `SELECT count(*) FROM login_sessions WHERE user_id = $1`, userID); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&st.active, `SELECT count(*) FROM login_sessions WHERE user_id = $1 AND active`, userID); err != nil {
		t.Fatal(err)
	}
	if st.active == 1 {
		db.Get(&st.activeHash, `SELECT token_hash FROM login_sessions WHERE user_id = $1 AND active`, userID)
	}
	return st
}

// 同じ利用者の同時 login: 全部成功し、有効なセッションはちょうど 1 つ (返したトークンのどれか)。
// 旧セッションは reason='replaced'。従来の経路 (legacy) と混ぜても同じ不変条件。
func TestSessionBatchConcurrentLogin(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	for _, mode := range []string{"batch", "legacy", "mixed"} {
		for round := 0; round < 3; round++ {
			userID, username, secret := sessionBatchSeed(t, db)
			// 事前に 1 つ有効なセッションを作っておく (これも無効化されるべき)
			_, pre, err := loginBatch(ctx, db, username, secret)
			if err != nil {
				t.Fatal(err)
			}
			const n = 32
			var wg sync.WaitGroup
			var mu sync.Mutex
			hashes := map[string]bool{}
			start := make(chan struct{})
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					var tok string
					useBatch := mode == "batch" || (mode == "mixed" && i%2 == 0)
					if useBatch {
						u, s, err := loginBatch(ctx, db, username, secret)
						if err != nil {
							t.Error(err)
							return
						}
						if u.ID != userID || s.UserID != userID || s.TokenHash != tokenHashOf(s.Token) {
							t.Errorf("bad login result %+v %+v", u, s)
						}
						tok = s.Token
					} else {
						s, err := legacyLogin(ctx, db, username, secret)
						if err != nil {
							t.Error(err)
							return
						}
						tok = s.Token
					}
					mu.Lock()
					hashes[tokenHashOf(tok)] = true
					mu.Unlock()
				}(i)
			}
			close(start)
			wg.Wait()
			st := sessionStateOf(t, db, userID)
			if st.total != n+1 || st.active != 1 || !hashes[st.activeHash] {
				t.Fatalf("%s: state %+v (want total %d, 1 active among returned)", mode, st, n+1)
			}
			var replaced int
			db.Get(&replaced, `SELECT count(*) FROM login_sessions WHERE user_id = $1 AND NOT active AND reason = 'replaced' AND invalidated_at IS NOT NULL`, userID)
			if replaced != n {
				t.Fatalf("%s: replaced %d want %d", mode, replaced, n)
			}
			// 旧セッション (事前の 1 つ) は DB の認証で見つからない、有効なものは見つかる (RYW)
			var cnt int
			db.Get(&cnt, `SELECT count(*) FROM login_sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash = $1 AND s.active AND u.status = 'active'`, pre.TokenHash)
			if cnt != 0 {
				t.Fatalf("%s: pre-existing session still active", mode)
			}
			db.Get(&cnt, `SELECT count(*) FROM login_sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash = $1 AND s.active AND u.status = 'active'`, st.activeHash)
			if cnt != 1 {
				t.Fatalf("%s: active session not visible", mode)
			}
		}
	}
}

// 直列の login は、毎回直後に DB で自分のセッションだけが有効 (RYW)。
func TestSessionBatchLoginRYW(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	userID, username, secret := sessionBatchSeed(t, db)
	for i := 0; i < 5; i++ {
		_, s, err := loginBatch(ctx, db, username, secret)
		if err != nil {
			t.Fatal(err)
		}
		st := sessionStateOf(t, db, userID)
		if st.active != 1 || st.activeHash != s.TokenHash || st.total != i+1 {
			t.Fatalf("round %d: %+v", i, st)
		}
	}
}

// パスワード違い・存在しない利用者・退会済み: 401 で何も書かない。
func TestSessionBatchLoginInvalid(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	userID, username, secret := sessionBatchSeed(t, db)
	if _, _, err := loginBatch(ctx, db, username, secret); err != nil {
		t.Fatal(err)
	}
	before := sessionStateOf(t, db, userID)
	if _, _, err := loginBatch(ctx, db, username, secret+"x"); !errors.Is(err, errLoginInvalid) {
		t.Fatalf("wrong password: %v", err)
	}
	if _, _, err := loginBatch(ctx, db, "nouser-"+userID[:6], secret); !errors.Is(err, errLoginInvalid) {
		t.Fatalf("no user: %v", err)
	}
	if after := sessionStateOf(t, db, userID); after != before {
		t.Fatalf("state changed: %+v -> %+v", before, after)
	}
	db.MustExec(`UPDATE users SET status = 'deleted' WHERE id = $1`, userID)
	if _, _, err := loginBatch(ctx, db, username, secret); !errors.Is(err, errLoginInvalid) {
		t.Fatalf("deleted user: %v", err)
	}
	if after := sessionStateOf(t, db, userID); after != before {
		t.Fatalf("state changed after deleted login: %+v -> %+v", before, after)
	}
}

// 同じトークンの同時 logout と、並行する login: logout は全部成功、トークンは無効 (reason='logout')。
func TestSessionBatchLogout(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	userID, username, secret := sessionBatchSeed(t, db)
	_, s, err := loginBatch(ctx, db, username, secret)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := logoutBatch(ctx, db, userID, s.TokenHash); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var active bool
	var reason string
	if err := db.QueryRow(`SELECT active, reason FROM login_sessions WHERE token_hash = $1`, s.TokenHash).Scan(&active, &reason); err != nil {
		t.Fatal(err)
	}
	if active || reason != "logout" {
		t.Fatalf("after logout: active=%v reason=%q", active, reason)
	}
	// 無いユーザー: 従来 (InvalidateLoginSession) と同じくエラー
	if err := logoutBatch(ctx, db, uuid.NewString(), s.TokenHash); err == nil {
		t.Fatal("logout of missing user should fail")
	}
	// 接続がプールに失敗状態で戻っていない
	if _, _, err := loginBatch(ctx, db, username, secret); err != nil {
		t.Fatal(err)
	}
}

// 同じユーザー名の同時 register: 作られるのはちょうど 1 人。
func TestSessionBatchRegisterConcurrent(t *testing.T) {
	db := postBatchTestDB(t)
	ctx := context.Background()
	name := "r" + uuid.NewString()[:10]
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, ok, err := registerBatch(ctx, db, uuid.NewString(), name, tokenHashOf(name), "disp", "europe")
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				if u.Username != name || u.Status != "active" || u.AccountType != "regular" || u.Bio != "" || u.Region != "europe" || u.ProfileImageID != nil {
					t.Errorf("bad user %+v", u)
				}
				mu.Lock()
				created++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	var n int
	db.Get(&n, `SELECT count(*) FROM users WHERE username = $1`, name)
	if created != 1 || n != 1 {
		t.Fatalf("created=%d rows=%d", created, n)
	}
}

// パスワードの確認 (SHA-256 + hex) のコスト。
func BenchmarkSessionBatchPasswordHash(b *testing.B) {
	secret := uuid.NewString()
	for i := 0; i < b.N; i++ {
		_ = tokenHashOf(secret)
	}
}
