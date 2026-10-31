package isureal

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type FriendRequest struct {
	ID          string    `db:"id"`
	RequesterID string    `db:"requester_id"`
	RecipientID string    `db:"recipient_id"`
	Status      string    `db:"status"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

const friendRequestColumns = `id, requester_id, recipient_id, status, created_at, updated_at`

func getFriendRequest(ctx context.Context, db sqlx.QueryerContext, id string) (FriendRequest, error) {
	var request FriendRequest
	err := sqlx.GetContext(ctx, db, &request, `SELECT id, requester_id, recipient_id, status, created_at, updated_at FROM friend_requests WHERE id=$1`, id)
	return request, err
}

func beginRelationshipTx(ctx context.Context, db *sqlx.DB, actor, target string) (*sqlx.Tx, error) {
	if actor == target || target == "" {
		return nil, NewAPIError(http.StatusBadRequest, "invalid_relationship")
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	lockedIDs, err := lockRelationshipPair(ctx, tx, actor, target, true)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if len(lockedIDs) != 2 {
		tx.Rollback()
		return nil, NewAPIError(http.StatusNotFound, "relationship_not_found")
	}
	return tx, nil
}

func isBlocked(ctx context.Context, db sqlx.QueryerContext, actor, target string) (bool, error) {
	var blocked bool
	err := sqlx.GetContext(ctx, db, &blocked, `SELECT EXISTS(SELECT 1 FROM block_relations WHERE (blocker_id=$1 AND blocked_id=$2) OR (blocker_id=$2 AND blocked_id=$1))`, actor, target)
	return blocked, err
}

func insertFriendship(ctx context.Context, tx *sqlx.Tx, request FriendRequest) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO friendships (id, user_low_id, user_high_id, source_request_id) VALUES ($1,$2,$3,$4)`, uuid.NewString(), min(request.RequesterID, request.RecipientID), max(request.RequesterID, request.RecipientID), request.ID)
	return err
}

func requireRelationshipChange(result sql.Result, err error, status int, code string) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return NewAPIError(status, code)
	}
	return nil
}

// lockRelationshipPair は、2 人の間の関係 (友達申請・友達・follow・block・相手の投稿への realmoji / comment) を
// 変える tx の直列化を「users の行の排他ロック」から「組ごとの advisory lock + users の行の共有ロック」に替える。
//
// 正しさ:
//   - これらの tx が読み書きする行 (friend_requests / friendships / follows / block_relations、
//     realmojis の (post, reactor)、canView/canComment の判定) はすべて 2 人の組で決まる。重複申請・相互申請の
//     自動承認・block との競合など、守るべき不変条件も組の中で閉じている。だから同じ組の tx どうしを
//     pg_advisory_xact_lock(3, hashtext(min:max)) で直列化すれば十分 (hash の衝突は余計に待つだけで安全)。
//   - 別の組の tx (例: 人気の投稿者への realmoji が多数) は互いに待たない。以前は投稿者の行の
//     FOR NO KEY UPDATE で全部が直列化され、asia2 の Lock:tuple/transactionid 待ちの主因だった。
//   - users の行には FOR SHARE を残す。これは UPDATE (アカウント削除の status 変更・プロフィールの変更)、
//     および他の FOR NO KEY UPDATE (login / 投稿 / comment の削除) と衝突する。だから「tx の間、2 人とも
//     active のまま」という保証は従来と同じ。FOR SHARE どうしは衝突しない。
//   - 友達の友達のような第三者が絡む可視性の判定は、別の組の tx と並行して走りうる。ただし READ COMMITTED の
//     文ごとのスナップショットで判定するので、先にレスポンスを返した書き込みは必ず見える。並行した 2 つの
//     リクエストは、どちらの順に並べても整合する (以前もこの判定はロックの外の行を読んでいた)。
//   - デッドロックしない: advisory lock は tx ごとに 1 つだけ取る。users の行の共有ロックは互いに待たない。
//     排他ロックを持つ側 (login / 投稿 / 削除など) は advisory lock を取らないので、待ちの輪ができない。
//   - DB 上のロックなので、asia1 以外のホストが書いても成り立つ。
//
// activeOnly: status='active' の行だけを返す (beginRelationshipTx)。false なら status は問わない (comment)。
func lockRelationshipPair(ctx context.Context, tx *sqlx.Tx, a, b string, activeOnly bool) ([]string, error) {
	lo, hi := min(a, b), max(a, b)
	cond := ""
	if activeOnly {
		cond = " AND u.status='active'"
	}
	var ids []string
	err := tx.SelectContext(ctx, &ids, `SELECT u.id FROM (SELECT pg_advisory_xact_lock(3, hashtext($3))) l, users u
   WHERE u.id IN ($1, $2)`+cond+` ORDER BY u.id COLLATE "C" FOR SHARE OF u`, lo, hi, lo+":"+hi)
	return ids, err
}
