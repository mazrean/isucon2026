package isureal

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// ISUREAL_FRIEND_BATCH=1 (既定 0): 友達申請 (createFriendRequestHandler) の tx を pgx の batch 1 往復
// (BEGIN; 組の行ロック; 判定+書き込みの CTE; COMMIT) で送る。
//
// 従来は組の 2 行を FOR NO KEY UPDATE してから、判定 → INSERT (または UPDATE + INSERT friendships) → COMMIT の
// 3〜4 往復の間ロックを握っていた。同じ行を投稿の tx も FOR NO KEY UPDATE するので、asia2 で
// Lock:tuple / transactionid の待ちになっていた。ロックの対象・強さ・順序 (id のバイト順) は変えずに、
// 握っている間の往復を 0 にする。
//
// 正しさ:
//   - ロックは beginRelationshipTx と同じ文で、判定とは別の文にしてある。判定の文は、ロックの文が終わってから
//     新しいスナップショットで始まる (READ COMMITTED) ので、ロックを待っている間にコミットされた同じ組の
//     申請・承認・block (いずれも組の行ロックを取る) が必ず見える。1 つの文にまとめると、ロック待ちの前の
//     スナップショットで判定してしまい、二重申請・相互申請の取りこぼしが起きる。
//   - 判定の順序は従来どおり: 2 人とも active でなければ 404、block か既に友達なら 409、自分の保留中の申請があれば 409、
//     相手の保留中の申請があれば承認 + friendships、無ければ新しい申請。書き込みは CTE の中で、条件を満たすとき
//     だけ行う (data-modifying CTE は全部、文の開始時の状態を見る。ここでは判定に使う状態そのもの)。
//   - 2 人とも active かは CTE で読み直す。active の行は自分がロック中なので、ロックの文で見た版から変わらない。
//   - 201 は COMMIT の結果を読んでから返す。friendships の (low, high) は Go の min/max (バイト順) で、従来と同じ。
var friendBatchOn = os.Getenv("ISUREAL_FRIEND_BATCH") == "1"

const relationshipLockSQL = `SELECT id FROM users WHERE id IN ($1, $2) AND status='active' ORDER BY id COLLATE "C" FOR NO KEY UPDATE`

const friendRequestBatchSQL = `
WITH ok AS (
  SELECT count(*) = 2 AS ok FROM users WHERE id IN ($1::varchar, $2::varchar) AND status = 'active'
), st AS (
  SELECT
    EXISTS(SELECT 1 FROM block_relations WHERE (blocker_id=$1::varchar AND blocked_id=$2::varchar) OR (blocker_id=$2::varchar AND blocked_id=$1::varchar)) AS blocked,
    EXISTS(SELECT 1 FROM friendships WHERE (user_low_id=$1::varchar AND user_high_id=$2::varchar) OR (user_low_id=$2::varchar AND user_high_id=$1::varchar)) AS friends,
    p.id AS pending_id, p.requester_id AS pending_requester
  FROM (SELECT 1) one LEFT JOIN LATERAL (
    SELECT id, requester_id FROM friend_requests WHERE status='pending' AND
     ((requester_id=$1::varchar AND recipient_id=$2::varchar) OR (requester_id=$2::varchar AND recipient_id=$1::varchar))
    LIMIT 1) p ON true
), upd AS (
  UPDATE friend_requests f SET status='accepted', updated_at=clock_timestamp()
  FROM ok, st
  WHERE ok.ok AND NOT st.blocked AND NOT st.friends AND f.id = st.pending_id AND st.pending_requester <> $1::varchar
  RETURNING f.id, f.status, f.updated_at
), fr AS (
  INSERT INTO friendships (id, user_low_id, user_high_id, source_request_id)
  SELECT $4::varchar, $5::varchar, $6::varchar, upd.id FROM upd
  RETURNING 1
), ins AS (
  INSERT INTO friend_requests (id, requester_id, recipient_id, status, created_at, updated_at)
  SELECT $3::varchar, $1::varchar, $2::varchar, 'pending', clock_timestamp(), clock_timestamp()
  FROM ok, st
  WHERE ok.ok AND NOT st.blocked AND NOT st.friends AND st.pending_id IS NULL
  RETURNING id, status, updated_at
)
SELECT ok.ok, st.blocked, st.friends,
       coalesce(upd.id, ins.id), coalesce(upd.status, ins.status), coalesce(upd.updated_at, ins.updated_at)
FROM ok, st LEFT JOIN upd ON true LEFT JOIN ins ON true`

// createFriendRequestBatch は createFriendRequestHandler の tx 部分 (beginRelationshipTx 以降) と同じ結果を返す。
func createFriendRequestBatch(ctx context.Context, db *sqlx.DB, actor, recipient string) (createFriendRequestResponse, error) {
	var resp createFriendRequestResponse
	if actor == recipient || recipient == "" {
		return resp, NewAPIError(http.StatusBadRequest, "invalid_relationship")
	}
	var (
		ok, blocked, friends bool
		id, status           *string
		updatedAt            *time.Time
	)
	conn, err := db.Conn(ctx)
	if err != nil {
		return resp, err
	}
	defer conn.Close()
	err = conn.Raw(func(dc any) error {
		pc := dc.(*stdlib.Conn).Conn()
		b := &pgx.Batch{}
		b.Queue("BEGIN")
		lockSQL, lockArgs := relationshipLock(actor, recipient)
		b.Queue(lockSQL, lockArgs...)
		b.Queue(friendRequestBatchSQL, actor, recipient, uuid.NewString(), uuid.NewString(), min(actor, recipient), max(actor, recipient))
		b.Queue("COMMIT")
		br := pc.SendBatch(ctx, b)
		err := func() error {
			if _, err := br.Exec(); err != nil {
				return err
			}
			if _, err := br.Exec(); err != nil { // ロック (行数は CTE の ok で見る)
				return err
			}
			if err := br.QueryRow().Scan(&ok, &blocked, &friends, &id, &status, &updatedAt); err != nil {
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
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = sql.ErrNoRows
		}
		return resp, err
	}
	switch {
	case !ok:
		return resp, NewAPIError(http.StatusNotFound, "relationship_not_found")
	case blocked || friends:
		return resp, NewAPIError(http.StatusConflict, "relationship_conflict")
	case id == nil: // 自分の保留中の申請がある
		return resp, NewAPIError(http.StatusConflict, "relationship_conflict")
	}
	return createFriendRequestResponse{RelationID: *id, Status: *status, UpdatedAt: *updatedAt}, nil
}

// transitionFriendRequestBatch は transitionFriendRequestHandler の tx 部分 (beginRelationshipTx 以降) を
// 同じく 1 往復で流す (ISUREAL_FRIEND_BATCH=1)。ロックは組 (requester, recipient) の行で従来どおり、判定は別の文。
// 判定の順序も従来どおり: 2 人とも active でなければ 404、申請が pending でなければ 403、承認で block があれば 409。
// 条件を満たしたときだけ、承認なら friendships を入れ、申請の status を変える。
const friendTransitionBatchSQL = `
WITH ok AS (
  SELECT count(*) = 2 AS ok FROM users WHERE id IN ($6::varchar, $7::varchar) AND status = 'active'
), f AS (
  SELECT f.id, f.status,
    EXISTS(SELECT 1 FROM block_relations b WHERE (b.blocker_id=f.requester_id AND b.blocked_id=f.recipient_id) OR (b.blocker_id=f.recipient_id AND b.blocked_id=f.requester_id)) AS blocked
  FROM friend_requests f WHERE f.id = $1::varchar
), cond AS (
  SELECT f.id FROM f, ok WHERE ok.ok AND f.status = 'pending' AND ($2::text <> 'accepted' OR NOT f.blocked)
), fr AS (
  INSERT INTO friendships (id, user_low_id, user_high_id, source_request_id)
  SELECT $3::varchar, $4::varchar, $5::varchar, cond.id FROM cond WHERE $2::text = 'accepted'
  RETURNING 1
), upd AS (
  UPDATE friend_requests SET status = $2::text, updated_at = clock_timestamp()
  WHERE id = (SELECT id FROM cond)
  RETURNING id, status, updated_at
)
SELECT ok.ok, f.status, f.blocked, upd.id, upd.status, upd.updated_at
FROM ok LEFT JOIN f ON true LEFT JOIN upd ON true`

func transitionFriendRequestBatch(ctx context.Context, db *sqlx.DB, request FriendRequest, status string) (transitionFriendRequestResponse, error) {
	var resp transitionFriendRequestResponse
	if request.RequesterID == request.RecipientID || request.RecipientID == "" {
		return resp, NewAPIError(http.StatusBadRequest, "invalid_relationship")
	}
	var (
		ok                       bool
		curStatus, id, newStatus *string
		blocked                  *bool
		updatedAt                *time.Time
	)
	a, b := request.RequesterID, request.RecipientID
	conn, err := db.Conn(ctx)
	if err != nil {
		return resp, err
	}
	defer conn.Close()
	err = conn.Raw(func(dc any) error {
		pc := dc.(*stdlib.Conn).Conn()
		batch := &pgx.Batch{}
		batch.Queue("BEGIN")
		lockSQL, lockArgs := relationshipLock(a, b)
		batch.Queue(lockSQL, lockArgs...)
		batch.Queue(friendTransitionBatchSQL, request.ID, status, uuid.NewString(), min(a, b), max(a, b), a, b)
		batch.Queue("COMMIT")
		br := pc.SendBatch(ctx, batch)
		err := func() error {
			if _, err := br.Exec(); err != nil {
				return err
			}
			if _, err := br.Exec(); err != nil {
				return err
			}
			if err := br.QueryRow().Scan(&ok, &curStatus, &blocked, &id, &newStatus, &updatedAt); err != nil {
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
	if err != nil {
		return resp, err
	}
	switch {
	case !ok:
		return resp, NewAPIError(http.StatusNotFound, "relationship_not_found")
	case curStatus == nil:
		return resp, sql.ErrNoRows // 従来も申請の読み直しが ErrNoRows ならそのまま返していた
	case *curStatus != "pending":
		return resp, NewAPIError(http.StatusForbidden, "relationship_action_forbidden")
	case status == "accepted" && blocked != nil && *blocked:
		return resp, NewAPIError(http.StatusConflict, "relationship_conflict")
	case id == nil:
		return resp, sql.ErrNoRows
	}
	return transitionFriendRequestResponse{RelationID: *id, Status: *newStatus, UpdatedAt: *updatedAt}, nil
}
