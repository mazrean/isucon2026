package isureal

import (
	"database/sql"
	"errors"
	"io"
	"net/http"

	"github.com/labstack/echo/v5"
)

func putProfileImageHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if c.Request().Header.Get("Content-Type") != "image/jpeg" {
			return NewAPIError(http.StatusUnsupportedMediaType, "unsupported_media_type")
		}
		data, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return err
		}
		imgW, imgH, imgOK := checkJPEG(c.Request().Context(), data)
		if !imgOK {
			c.Set("abuse", "invalid_image")
			return NewAPIError(http.StatusBadRequest, "invalid_profile_image")
		}
		if imgW > 512 || imgH > 512 {
			return NewAPIError(http.StatusBadRequest, "invalid_profile_image")
		}

		ctx := c.Request().Context()
		userID := GetSession(c).UserID
		if h.Store != nil {
			if ok, err := h.Store.putProfileImage(c, userID, data); ok {
				return err
			}
		}
		tx, err := h.DB.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var oldID *string
		if err := tx.GetContext(ctx, &oldID, `SELECT profile_image_id FROM users WHERE id=$1 FOR NO KEY UPDATE`, userID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return NewAPIError(http.StatusNotFound, "account_not_found")
			}
			return err
		}
		imageID := newImageID(ctx)
		external := externalImage(c, userID)
		if !external {
			if _, err := tx.ExecContext(ctx, `INSERT INTO image_objects(id,owner_user_id,data,media_type) VALUES ($1,$2,$3,'image/jpeg')`, imageID, userID, data); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE users SET profile_image_id=$2,updated_at=clock_timestamp() WHERE id=$1`, userID, imageID); err != nil {
			return err
		}
		if oldID != nil {
			if _, err := tx.ExecContext(ctx, `DELETE FROM image_objects WHERE id=$1`, *oldID); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		if !external {
			replicateImage(imageID, userID, "image/jpeg", data)
		}
		return c.NoContent(http.StatusNoContent)
	}
}

func deleteProfileImageHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		userID := GetSession(c).UserID
		if h.Store != nil {
			if ok, err := h.Store.deleteProfileImage(c, userID); ok {
				return err
			}
		}
		tx, err := h.DB.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var imageID *string
		err = tx.GetContext(ctx, &imageID, `SELECT profile_image_id FROM users WHERE id=$1 FOR NO KEY UPDATE`, userID)
		if errors.Is(err, sql.ErrNoRows) {
			return NewAPIError(http.StatusNotFound, "account_not_found")
		}
		if err != nil {
			return err
		}
		if imageID == nil {
			return NewAPIError(http.StatusNotFound, "account_not_found")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE users SET profile_image_id=NULL,updated_at=clock_timestamp() WHERE id=$1`, userID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM image_objects WHERE id=$1`, *imageID); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return c.NoContent(http.StatusNoContent)
	}
}
