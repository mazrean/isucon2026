package isureal

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// ISUREAL_POST_BATCH=1 (既定 0): 投稿の tx を pgx の batch 1 往復 (BEGIN; ユーザー行のロック; 窓+件数+INSERT の CTE; COMMIT) で送る。
//
// 目的: 従来は users の行を FOR NO KEY UPDATE してから、窓の取得 → 件数 → INSERT posts → getPost → COMMIT の
// 5 往復の間ずっと行ロックを握っていた。asia2 では友達申請の組のロック (relationship.go) と同じ行で待ち合い
// (Lock:tuple / transactionid)、asia1 ではその間メインのプールの接続を握り続けてプール待ちが伸びた。
// batch にすると、行ロックを握っている間にネットワークの往復が 1 つも挟まらない (サーバー内の実行時間 + COMMIT の fsync だけ)。
//
// 正しさ (同時実行での一意性): ロックは従来どおり users の行の FOR NO KEY UPDATE で、その順序・強さは変えない。
//   - ロックと件数の確認は「別の文」にしてある。1 つの文 (CTE) の中では全体が文の開始時のスナップショットを使うので、
//     ロックを待っている間に先行の投稿がコミットされても件数に入らず、二重投稿になる。batch では 2 つ目の文は
//     1 つ目 (ロック) が終わってから始まり、READ COMMITTED なので新しいスナップショットを取る。先行の tx は
//     ロックを手放す前にコミットを可視にしている (ProcArrayEndTransaction → ReleaseLocks) ので、件数に必ず入る。
//   - INSERT は CTE の中で「件数 0・削除 2 未満・窓の開始後・ユーザーが active」のときだけ行われる。
//     ユーザーが active かは 2 つ目の文で読み直す: 行は自分がロック中なので、1 つ目の文で見た版から変わらない。
//     アカウント削除 (UPDATE status) はこの行ロックを待つので、削除済みのユーザーの投稿は入らない (従来と同じ)。
//   - 投稿の削除 (deletePostHandler) も同じ行を FOR NO KEY UPDATE するので、削除の回数の上限との競合も従来と同じ。
//   - ブロック・友達関係 (relationship.go) も同じ行の排他ロックで、この tx と直列になる (投稿は関係を読まないので、
//     順序が変わるだけ)。
//   - 窓は従来どおり FOR KEY SHARE。窓が取れなかった (切り替えの途中) ときは何も入れずに COMMIT され、
//     batch ごと 1 回だけ取り直す (従来の「新しいスナップショットで取り直す」と同じ)。
//   - 201 は COMMIT の完了 (batch の最後の結果) を読んでから返す。応答の投稿は INSERT ... RETURNING の値
//     (= DB に入った値。posted_at はマイクロ秒に丸めた値で getPost と同じ) と窓の列から作る。
//   - session_replication_role は触らない (mem_changelog のトリガーはそのまま動く)。
var postBatchOn = os.Getenv("ISUREAL_POST_BATCH") == "1"

var postBatchUserSQL = `SELECT ` + strings.Join(userColumns, ", ") + ` FROM users WHERE id = $1 FOR NO KEY UPDATE`

const postBatchInsertSQL = `
WITH w AS (
  SELECT id, local_date, starts_at, on_time_deadline, ended_at FROM posting_windows
  WHERE region = (SELECT region FROM users WHERE id = $1::varchar) AND ended_at IS NULL
  FOR KEY SHARE
), c AS (
  SELECT count(*) FILTER (WHERE p.deleted_at IS NULL) AS active,
         count(*) FILTER (WHERE p.deleted_at IS NOT NULL) AS deleted
  FROM posts p JOIN posting_windows pw ON pw.id = p.posting_window_id
  WHERE p.author_id = $1::varchar AND pw.local_date = (SELECT local_date FROM w)
), ins AS (
  INSERT INTO posts (id, author_id, posting_window_id, image_id, caption, visibility,
                     location_kind, latitude, longitude, locality, posted_at)
  SELECT $2::varchar, $1::varchar, w.id, $3::varchar, $4::text, $5::text, $6::text, $7::numeric, $8::numeric, $9::text, $10::timestamptz
  FROM w, c
  WHERE c.active = 0 AND c.deleted < 2 AND $10::timestamptz >= w.starts_at
    AND EXISTS (SELECT 1 FROM users u WHERE u.id = $1::varchar AND u.status = 'active')
  RETURNING id, author_id, posting_window_id, image_id, caption, visibility,
            location_kind, latitude, longitude, locality, posted_at
)
SELECT w.on_time_deadline, w.ended_at, ins.id, ins.author_id, ins.posting_window_id, ins.image_id, ins.caption,
       ins.visibility, ins.location_kind, ins.latitude, ins.longitude, ins.locality, ins.posted_at
FROM w LEFT JOIN ins ON true`

var errPostBatchNoWindow = errors.New("post batch: no open window")

// createPostBatch は 1 往復で投稿の tx を流す。戻り値:
//   - (user, post, true, nil): 投稿した (コミット済み)
//   - (_, _, false, nil): 枠が無い (409 post_slot_unavailable)
//   - err == sql.ErrNoRows: ユーザーが居ない / active でない (getUserForUpdate と同じ)
func createPostBatch(ctx context.Context, db *sqlx.DB, userID, postID, imageID string, input createPostRequest, location PostLocation) (User, Post, bool, error) {
	for attempt := 0; ; attempt++ {
		user, post, inserted, err := createPostBatchOnce(ctx, db, userID, postID, imageID, input, location)
		if errors.Is(err, errPostBatchNoWindow) {
			if attempt == 0 {
				continue
			}
			return User{}, Post{}, false, nil
		}
		return user, post, inserted, err
	}
}

func createPostBatchOnce(ctx context.Context, db *sqlx.DB, userID, postID, imageID string, input createPostRequest, location PostLocation) (User, Post, bool, error) {
	var (
		user     User
		post     Post
		inserted bool
		noWindow bool
	)
	conn, err := db.Conn(ctx)
	if err != nil {
		return user, post, false, err
	}
	defer conn.Close()
	err = conn.Raw(func(dc any) error {
		pc := dc.(*stdlib.Conn).Conn()
		postedAt := time.Now().UTC()
		b := &pgx.Batch{}
		b.Queue("BEGIN")
		b.Queue(postBatchUserSQL, userID)
		b.Queue(postBatchInsertSQL, userID, postID, imageID, input.Caption, input.Visibility,
			location.Kind, location.Latitude, location.Longitude, location.Locality, postedAt)
		b.Queue("COMMIT")
		br := pc.SendBatch(ctx, b)
		err := func() error {
			if _, err := br.Exec(); err != nil {
				return err
			}
			userFound := false
			err := br.QueryRow().Scan(&user.ID, &user.Username, &user.SecretKeyHash, &user.DisplayName, &user.Bio,
				&user.ProfileImageID, &user.Region, &user.AccountType, &user.Status)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
			case err != nil:
				return err
			default:
				userFound = true
			}
			var (
				onTime  time.Time
				endedAt *time.Time
				id      *string
				p       struct {
					authorID, windowID, imageID, caption, visibility, kind *string
					lat, lng                                               *float64
					locality                                               *string
					postedAt                                               *time.Time
				}
			)
			err = br.QueryRow().Scan(&onTime, &endedAt, &id, &p.authorID, &p.windowID, &p.imageID, &p.caption,
				&p.visibility, &p.kind, &p.lat, &p.lng, &p.locality, &p.postedAt)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				noWindow = true
			case err != nil:
				return err
			case id != nil:
				inserted = true
				post = Post{
					ID: *id, AuthorID: *p.authorID, PostingWindowID: *p.windowID, ImageID: *p.imageID,
					Caption: *p.caption, Visibility: *p.visibility,
					PostLocation:   PostLocation{Kind: *p.kind, Latitude: p.lat, Longitude: p.lng, Locality: p.locality},
					PostedAt:       *p.postedAt,
					OnTimeDeadline: onTime, WindowEndedAt: endedAt,
				}
			}
			if _, err := br.Exec(); err != nil {
				return err
			}
			if !userFound || user.Status != "active" {
				return sql.ErrNoRows
			}
			return nil
		}()
		closeErr := br.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			// 途中の文が失敗すると COMMIT は飛ばされ、tx は失敗状態のまま残る。接続をプールに返す前に閉じる。
			_, _ = pc.Exec(context.WithoutCancel(ctx), "ROLLBACK")
		}
		return err
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, Post{}, false, sql.ErrNoRows
		}
		return User{}, Post{}, false, err
	}
	if noWindow {
		return User{}, Post{}, false, errPostBatchNoWindow
	}
	if inserted {
		if err := user.setImageKey(); err != nil {
			return User{}, Post{}, false, err
		}
	}
	return user, post, inserted, nil
}
