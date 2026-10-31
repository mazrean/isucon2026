package isureal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

type getNotificationsResponse struct {
	ID              string    `json:"id" db:"id"`
	PostingWindowID string    `json:"postingWindowId" db:"posting_window_id"`
	Region          string    `json:"region" db:"region"`
	CreatedAt       time.Time `json:"createdAt" db:"created_at"`
}

func getNotificationsHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		session := GetSession(c)
		userID := session.UserID
		// 接続時刻はローカル時計で取る (ホストは NTP 同期済み)。以前は primary へ
		// SELECT clock_timestamp() を投げており、europe1/america1 では primary が別リージョン
		// (往復 ~100ms) かつプール 25 本のため、グランプリ時の一斉再接続で数秒待ち →
		// クライアント切断 (499 / action log 500) やルーター側 503 の原因になっていた。
		connectedAt := time.Now()
		response := c.Response()
		response.Header().Set("Content-Type", "text/event-stream")
		response.WriteHeader(http.StatusOK)
		controller := http.NewResponseController(response)
		if err := controller.Flush(); err != nil {
			return nil
		}
		sub := h.Notifications.subscribe(userID, session.Region, connectedAt)
		defer h.Notifications.unsubscribe(sub)
		select {
		case <-ctx.Done():
			return nil
		case notification := <-sub.ch:
			if notification == nil {
				return nil
			}
			data, err := json.Marshal(notification)
			if err != nil {
				c.Logger().Error("failed to encode notification", "error", err)
				return nil
			}
			if _, err := fmt.Fprintf(response, "data: %s\n\n", data); err != nil {
				return nil
			}
			_ = controller.Flush()
			return nil
		}
	}
}
