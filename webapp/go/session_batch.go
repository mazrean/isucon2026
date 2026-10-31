package isureal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// ISUREAL_LOGIN_BATCH=1 (既定 0): login / logout / register の tx を pgx の batch 1 往復で送る
// (post_batch.go / friend_request_batch.go と同じ方式)。書き込みは asia1 → asia2 の PG で、
// 従来は login 4 往復 (BEGIN / ユーザー行のロック / 無効化+INSERT / COMMIT)、logout 4 往復、register 6 往復。
//
// 正しさ:
//   - ロックの対象・強さは従来どおり (login: username の行を FOR NO KEY UPDATE、logout: id の行、
//     register: pg_advisory_xact_lock(1, hashtext(username)))。ロックと判定・書き込みは「別の文」にしてある。
//     READ COMMITTED なので 2 つ目の文はロックの後の新しいスナップショットを取り、ロック待ちの間にコミットされた
//     同じ利用者の login (= そのセッション) が必ず見えて無効化される (1 つの文にまとめると取りこぼす)。
//   - login の無効化 + INSERT は「username・active・secret_key_hash が一致する行」があるときだけ行う
//     (パスワード違い・退会済みは何も書かずに COMMIT → 401)。行は自分がロック中なので 1 つ目の文の版から変わらない。
//   - 200 / 201 / 204 は COMMIT の結果を読んでから返す。応答は従来と同じ値から作る (login は 1 つ目の文の行、
//     register は INSERT ... RETURNING の行 = getUser と同じ列)。
//   - mem への反映は従来と同じく mem_changelog のトリガー経由 (session_replication_role は触らない)。
//   - パスワードの確認は SHA-256 の 1 回 (数百 ns)。CPU は支配的でないのでキャッシュはしない。
var loginBatchOn = os.Getenv("ISUREAL_LOGIN_BATCH") == "1"

var errLoginInvalid = errors.New("login batch: invalid credentials")

func loginBatchUserSQL() string {
	return lockUserRowSQL(`SELECT `+strings.Join(userColumns, ", ")+` FROM users WHERE username = $1`, userAdvSession)
}

const loginBatchSessionSQL = `
WITH u AS (
  SELECT id FROM users WHERE username = $1::varchar AND status = 'active' AND secret_key_hash = $2::text LIMIT 1
), inv AS (
  UPDATE login_sessions
  SET active = false, invalidated_at = clock_timestamp(), reason = 'replaced'
  WHERE user_id = (SELECT id FROM u) AND active = true
  RETURNING 1
)
INSERT INTO login_sessions (id, user_id, token_hash)
SELECT $3::varchar, u.id, $4::text FROM u
RETURNING user_id, token_hash, created_at`

var registerBatchInsertSQL = `
INSERT INTO users (id, username, secret_key_hash, display_name, region)
SELECT $1::varchar, $2::varchar, $3::text, $4::text, $5::text
WHERE NOT EXISTS (SELECT 1 FROM users WHERE username = $2::varchar)
RETURNING ` + strings.Join(userColumns, ", ")

// runTxBatch: BEGIN; queue(b); COMMIT を 1 往復で送り、read で途中の結果を読む (BEGIN / COMMIT の結果はここで読む)。
// 途中の文が失敗すると COMMIT は飛ばされるので、接続をプールに返す前に ROLLBACK する。
func runTxBatch(ctx context.Context, db *sqlx.DB, queue func(b *pgx.Batch), read func(br pgx.BatchResults) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			return errors.New("tx batch: not a pgx connection")
		}
		pc := sc.Conn()
		b := &pgx.Batch{}
		b.Queue("BEGIN")
		queue(b)
		b.Queue("COMMIT")
		br := pc.SendBatch(ctx, b)
		err := func() error {
			if _, err := br.Exec(); err != nil {
				return err
			}
			if err := read(br); err != nil {
				return err
			}
			_, err := br.Exec()
			return err
		}()
		closeErr := br.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			_, _ = pc.Exec(context.WithoutCancel(ctx), "ROLLBACK")
		}
		return err
	})
}

// loginBatch: loginHandler の DB 経路と同じ tx。資格が違えば errLoginInvalid (何も書かずにコミット済み)。
func loginBatch(ctx context.Context, db *sqlx.DB, username, secretKey string) (User, *Session, error) {
	hash := sha256.Sum256([]byte(secretKey))
	secretHash := hex.EncodeToString(hash[:])
	token := uuid.NewString()
	sum := sha256.Sum256([]byte(token))
	tokenHash := hex.EncodeToString(sum[:])
	sessionID := uuid.NewString()

	var (
		user      User
		userFound bool
		sess      Session
		inserted  bool
	)
	err := runTxBatch(ctx, db, func(b *pgx.Batch) {
		b.Queue(loginBatchUserSQL(), username)
		b.Queue(loginBatchSessionSQL, username, secretHash, sessionID, tokenHash)
	}, func(br pgx.BatchResults) error {
		err := br.QueryRow().Scan(&user.ID, &user.Username, &user.SecretKeyHash, &user.DisplayName, &user.Bio,
			&user.ProfileImageID, &user.Region, &user.AccountType, &user.Status)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return err
		default:
			userFound = true
		}
		var createdAt time.Time
		err = br.QueryRow().Scan(&sess.UserID, &sess.TokenHash, &createdAt)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return err
		default:
			inserted = true
			sess.CreatedAt = createdAt
		}
		return nil
	})
	if err != nil {
		return User{}, nil, err
	}
	valid := userFound && user.Status == "active" && user.SecretKeyHash == secretHash
	if !valid {
		if inserted {
			return User{}, nil, fmt.Errorf("login batch: session inserted for invalid user %q", username)
		}
		return User{}, nil, errLoginInvalid
	}
	if !inserted {
		return User{}, nil, fmt.Errorf("login batch: no session inserted for %q", username)
	}
	if sess.UserID != user.ID {
		return User{}, nil, fmt.Errorf("login batch: user mismatch %q != %q", sess.UserID, user.ID)
	}
	sess.Token = token
	return user, &sess, nil
}

// logoutBatch: InvalidateLoginSession と同じ tx (ユーザー行が無ければエラー、無効化が 0 行でも成功)。
func logoutBatch(ctx context.Context, db *sqlx.DB, userID, tokenHash string) error {
	return runTxBatch(ctx, db, func(b *pgx.Batch) {
		b.Queue(lockUserRowSQL(`SELECT id FROM users WHERE id = $1`, userAdvSession), userID)
		b.Queue(`UPDATE login_sessions SET active = false, invalidated_at = clock_timestamp(), reason = 'logout'
WHERE user_id = $1 AND active = true AND token_hash = $2`, userID, tokenHash)
	}, func(br pgx.BatchResults) error {
		var id string
		if err := br.QueryRow().Scan(&id); err != nil {
			return fmt.Errorf("failed to lock user: %w", err)
		}
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to invalidate sessions: %w", err)
		}
		return nil
	})
}

// registerBatch: registerHandler の DB 経路と同じ tx。ユーザー名が使用済みなら created=false (何も書かずにコミット済み)。
func registerBatch(ctx context.Context, db *sqlx.DB, userID, username, secretHash, displayName, region string) (User, bool, error) {
	// 従来は INSERT 後の getUser (setImageKey) の失敗で ROLLBACK した。同じ結果になるよう、送る前に確かめる。
	if _, err := imageSigningKey(secretHash); err != nil {
		return User{}, false, err
	}
	var (
		user    User
		created bool
	)
	err := runTxBatch(ctx, db, func(b *pgx.Batch) {
		b.Queue(`SELECT pg_advisory_xact_lock(1, hashtext($1))`, username)
		b.Queue(registerBatchInsertSQL, userID, username, secretHash, displayName, region)
	}, func(br pgx.BatchResults) error {
		if _, err := br.Exec(); err != nil {
			return err
		}
		err := br.QueryRow().Scan(&user.ID, &user.Username, &user.SecretKeyHash, &user.DisplayName, &user.Bio,
			&user.ProfileImageID, &user.Region, &user.AccountType, &user.Status)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil
		case err != nil:
			return err
		}
		created = true
		return nil
	})
	if err != nil || !created {
		return User{}, false, err
	}
	if err := user.setImageKey(); err != nil {
		return User{}, false, err
	}
	return user, true, nil
}
