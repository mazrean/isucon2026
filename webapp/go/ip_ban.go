package isureal

import (
	"context"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

const ipBanReloadInterval = 200 * time.Millisecond

type IPBanList struct {
	reloading sync.Mutex
	asyncOnce sync.Once
	snapshot  atomic.Pointer[ipBanSnapshot]
}

type ipBanSnapshot struct {
	loadedAt time.Time
	bans     map[string]time.Time
}

type ipBan struct {
	IP          string    `db:"ip"`
	BannedUntil time.Time `db:"banned_until"`
}

// ISUREAL_IPBAN_ASYNC=1 (既定 0): ip_bans の再読込を背景の 1 本 (200ms 周期) に移し、リクエストは待たない。
// 従来も、再読込を担当した 1 件以外は「直近の読込 (≦ 200ms + 1 往復前)」の snapshot で判定しているので、
// 鮮度の上限は変わらない。変わるのは、200ms ごとに 1 件のリクエストが primary への 1 往復
// (europe1 / america1 では約 100ms) を待たされなくなること。背景の読込が 2 秒止まったら従来の同期読込に戻る。
var ipBanAsync = os.Getenv("ISUREAL_IPBAN_ASYNC") == "1"

const ipBanAsyncStale = 2 * time.Second

func (l *IPBanList) loop(db *sqlx.DB) {
	t := time.NewTicker(ipBanReloadInterval)
	defer t.Stop()
	for range t.C {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if loaded, err := loadIPBans(ctx, db); err == nil {
			l.snapshot.Store(loaded)
		}
		cancel()
	}
}

func (l *IPBanList) Banned(ctx context.Context, db *sqlx.DB, ip string) (bool, error) {
	snapshot := l.snapshot.Load()
	if ipBanAsync {
		l.asyncOnce.Do(func() { go l.loop(db) })
		if snapshot != nil && time.Since(snapshot.loadedAt) < ipBanAsyncStale {
			until, ok := snapshot.bans[ip]
			return ok && time.Now().Before(until), nil
		}
	}
	if (snapshot == nil || time.Since(snapshot.loadedAt) >= ipBanReloadInterval) && l.reloading.TryLock() {
		defer l.reloading.Unlock()
		loaded, err := loadIPBans(ctx, db)
		if err != nil {
			return false, err
		}
		l.snapshot.Store(loaded)
		snapshot = loaded
	}
	if snapshot == nil {
		return false, nil
	}
	until, ok := snapshot.bans[ip]
	return ok && time.Now().Before(until), nil
}

func loadIPBans(ctx context.Context, db *sqlx.DB) (*ipBanSnapshot, error) {
	var rows []ipBan
	if err := db.SelectContext(ctx, &rows, `SELECT ip, banned_until FROM ip_bans WHERE banned_until > clock_timestamp()`); err != nil {
		return nil, err
	}
	bans := make(map[string]time.Time, len(rows))
	for _, row := range rows {
		bans[row.IP] = row.BannedUntil
	}
	return &ipBanSnapshot{loadedAt: time.Now(), bans: bans}, nil
}

func extractClientIP(req *http.Request) string {
	if ip := req.Header.Get("X-Client-IP"); ip != "" {
		return ip
	}
	return echo.ExtractIPDirect()(req)
}

func IPBanMiddleware(h *HandlerContext) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			banned, err := h.IPBans.Banned(c.Request().Context(), h.Reader(), c.RealIP())
			if err != nil {
				return err
			}
			if banned {
				return NewAPIError(http.StatusForbidden, "ip_banned")
			}
			return next(c)
		}
	}
}
