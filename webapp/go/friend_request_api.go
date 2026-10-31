package isureal

import (
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

type createFriendRequestRequest struct {
	RecipientID string `json:"recipientId"`
}

type createFriendRequestResponse struct {
	RelationID string    `json:"relationId"`
	Status     string    `json:"status"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

func createFriendRequestHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input createFriendRequestRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		ctx := c.Request().Context()
		actor := GetSession(c).UserID
		if h.Store != nil {
			if ok, err := h.Store.createFriendRequest(c, actor, input.RecipientID); ok {
				return err
			}
		}
		if friendBatchOn {
			resp, err := createFriendRequestBatch(ctx, h.DB, actor, input.RecipientID)
			if err != nil {
				return err
			}
			return c.JSON(http.StatusCreated, resp)
		}
		tx, err := beginRelationshipTx(ctx, h.DB, actor, input.RecipientID)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		// block / 既存の友達 / 保留中の申請を 1 往復で調べる (判定の順序は従来どおり block → 友達 → 保留中)。
		var st struct {
			Blocked          bool           `db:"blocked"`
			Friends          bool           `db:"friends"`
			PendingID        sql.NullString `db:"pending_id"`
			PendingRequester sql.NullString `db:"pending_requester"`
		}
		if err := tx.GetContext(ctx, &st, `
   SELECT
     EXISTS(SELECT 1 FROM block_relations WHERE (blocker_id=$1 AND blocked_id=$2) OR (blocker_id=$2 AND blocked_id=$1)) AS blocked,
     EXISTS(SELECT 1 FROM friendships WHERE (user_low_id=$1 AND user_high_id=$2) OR (user_low_id=$2 AND user_high_id=$1)) AS friends,
     p.id AS pending_id, p.requester_id AS pending_requester
   FROM (SELECT 1) one LEFT JOIN LATERAL (
     SELECT id, requester_id FROM friend_requests WHERE status='pending' AND
      ((requester_id=$1 AND recipient_id=$2) OR (requester_id=$2 AND recipient_id=$1))
     LIMIT 1) p ON true
  `, actor, input.RecipientID); err != nil {
			return err
		}
		if st.Blocked || st.Friends {
			return NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		var request FriendRequest
		if st.PendingID.Valid {
			if st.PendingRequester.String == actor {
				return NewAPIError(http.StatusConflict, "relationship_conflict")
			}
			if err := tx.GetContext(ctx, &request, `UPDATE friend_requests SET status='accepted', updated_at=clock_timestamp() WHERE id=$1 RETURNING `+friendRequestColumns, st.PendingID.String); err != nil {
				return err
			}
			if err := insertFriendship(ctx, tx, request); err != nil {
				return err
			}
		} else {
			if err := tx.GetContext(ctx, &request, `INSERT INTO friend_requests (id, requester_id, recipient_id, status, created_at, updated_at) VALUES ($1,$2,$3,'pending',clock_timestamp(),clock_timestamp()) RETURNING `+friendRequestColumns, uuid.NewString(), actor, input.RecipientID); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return c.JSON(http.StatusCreated, createFriendRequestResponse{RelationID: request.ID, Status: request.Status, UpdatedAt: request.UpdatedAt})
	}
}

type transitionFriendRequestRequest struct {
	RequestID string `param:"requestId" json:"-"`
	Action    string `param:"action" json:"-"`
}

type transitionFriendRequestResponse createFriendRequestResponse

func transitionFriendRequestHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input transitionFriendRequestRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		var status string
		switch input.Action {
		case "accept":
			status = "accepted"
		case "reject":
			status = "rejected"
		case "cancel":
			status = "cancelled"
		default:
			return NewAPIError(http.StatusForbidden, "relationship_action_forbidden")
		}
		ctx := c.Request().Context()
		actor := GetSession(c).UserID
		if h.Store != nil {
			if ok, err := h.Store.transitionFriendRequest(c, actor, input.RequestID, status); ok {
				return err
			}
		}
		request, err := getFriendRequest(ctx, h.Reader(), input.RequestID)
		if errors.Is(err, sql.ErrNoRows) {
			return NewAPIError(http.StatusNotFound, "relationship_not_found")
		}
		if err != nil {
			return err
		}
		if (status == "cancelled" && actor != request.RequesterID) || (status != "cancelled" && actor != request.RecipientID) {
			return NewAPIError(http.StatusForbidden, "relationship_action_forbidden")
		}
		tx, err := beginRelationshipTx(ctx, h.DB, request.RequesterID, request.RecipientID)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		// 申請の読み直しと block の確認を 1 往復で (block は accept のときだけ使う)。
		var locked struct {
			FriendRequest
			Blocked bool `db:"blocked"`
		}
		if err := tx.GetContext(ctx, &locked, `SELECT `+friendRequestColumns+`,
     EXISTS(SELECT 1 FROM block_relations b WHERE (b.blocker_id=f.requester_id AND b.blocked_id=f.recipient_id) OR (b.blocker_id=f.recipient_id AND b.blocked_id=f.requester_id)) AS blocked
   FROM friend_requests f WHERE id=$1`, input.RequestID); err != nil {
			return err
		}
		request = locked.FriendRequest
		if request.Status != "pending" {
			return NewAPIError(http.StatusForbidden, "relationship_action_forbidden")
		}
		if status == "accepted" {
			if locked.Blocked {
				return NewAPIError(http.StatusConflict, "relationship_conflict")
			}
			if err := insertFriendship(ctx, tx, request); err != nil {
				return err
			}
		}
		if err := tx.GetContext(ctx, &request, `UPDATE friend_requests SET status=$2, updated_at=clock_timestamp() WHERE id=$1 RETURNING `+friendRequestColumns, request.ID, status); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return c.JSON(http.StatusOK, transitionFriendRequestResponse{RelationID: request.ID, Status: request.Status, UpdatedAt: request.UpdatedAt})
	}
}
