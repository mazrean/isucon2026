package isureal

import (
	"context"
	"database/sql"
	"errors"
	"net/http"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

func deleteAccountHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		userID := GetSession(c).UserID
		if h.Store != nil {
			if ok, err := h.Store.deleteAccount(c, userID); ok {
				return err
			}
		}
		if err := deleteAccountDB(ctx, h.DB, userID); err != nil {
			return err
		}
		return c.NoContent(http.StatusNoContent)
	}
}

// deleteAccountDB: 退会の tx。USER_LOCK_SPLIT=1 では users の行を FOR UPDATE で取る (user_lock_split.go)。
func deleteAccountDB(ctx context.Context, db *sqlx.DB, userID string) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var lockedID string
	err = tx.GetContext(ctx, &lockedID, deleteAccountLockSQL(), userID)
	if errors.Is(err, sql.ErrNoRows) {
		return NewAPIError(http.StatusNotFound, "account_not_found")
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE users SET status='deleted', deleted_at=clock_timestamp(), updated_at=clock_timestamp() WHERE id=$1`, userID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE login_sessions SET active=false, invalidated_at=clock_timestamp(), reason='account_deletion' WHERE user_id=$1 AND active=true`, userID); err != nil {
		return err
	}
	return tx.Commit()
}
