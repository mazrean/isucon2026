package isureal

import (
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

type openPostingWindowRequest struct {
	Region        string `json:"region"`
	LocalDate     string `json:"localDate"`
	OnTimeSeconds *int64 `json:"onTimeSeconds"`
}

type openPostingWindowResponse struct {
	ID             string    `json:"id"`
	Region         string    `json:"region"`
	LocalDate      string    `json:"localDate"`
	StartsAt       time.Time `json:"startsAt"`
	OnTimeDeadline time.Time `json:"onTimeDeadline"`
}

func openPostingWindowHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input openPostingWindowRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		region := input.Region
		localDate, err := time.Parse(time.DateOnly, input.LocalDate)
		if !validRegion(region) || err != nil {
			return NewAPIError(http.StatusBadRequest, "invalid_posting_window")
		}
		seconds := int64(24 * 60 * 60)
		if input.OnTimeSeconds != nil {
			seconds = *input.OnTimeSeconds
		}
		ctx := c.Request().Context()
		tx, err := h.DB.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		// 以前は LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE を取っていたが、users への UPDATE/INSERT と
		// 競合してロックの行列ができ、投稿ラッシュ中にタイムアウトしていた。同一リージョンの開始処理だけを直列化する。
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(2, hashtext($1))`, region); err != nil {
			return err
		}
		var check struct {
			Conflict bool      `db:"conflict"`
			Now      time.Time `db:"now"`
		}
		if err := tx.GetContext(ctx, &check, `SELECT EXISTS(SELECT 1 FROM posting_windows WHERE region = $1 AND local_date >= $2) AS conflict, clock_timestamp() AS now`, region, localDate); err != nil {
			return err
		}
		if check.Conflict {
			return NewAPIError(http.StatusConflict, "posting_window_conflict")
		}
		startsAt := check.Now
		if _, err := tx.ExecContext(ctx, `UPDATE posting_windows SET ended_at = $2 WHERE region = $1 AND ended_at IS NULL`, region, startsAt); err != nil {
			return err
		}
		windowID := uuid.NewString()
		onTimeDeadline := startsAt.Add(time.Duration(seconds) * time.Second)
		if _, err := tx.ExecContext(ctx, `
   INSERT INTO posting_windows (id, region, local_date, starts_at, on_time_deadline)
   VALUES ($1, $2, $3, $4, $5)
  `, windowID, region, localDate, startsAt, onTimeDeadline); err != nil {
			if isUniqueViolation(err) {
				return NewAPIError(http.StatusConflict, "posting_window_conflict")
			}
			return err
		}
		// 通知は notifications 行を作らず、notification_hub.go で (期間, 利用者) から導出する。
		if err := tx.Commit(); err != nil {
			return err
		}
		return c.JSON(http.StatusCreated, openPostingWindowResponse{
			ID: windowID, Region: region, LocalDate: localDate.Format(time.DateOnly),
			StartsAt: startsAt, OnTimeDeadline: onTimeDeadline,
		})
	}
}
