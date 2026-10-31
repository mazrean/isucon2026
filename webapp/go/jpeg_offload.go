package isureal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"example.com/isureal-company/isureal/internal/jpegcheck"
	"github.com/labstack/echo/v5"
)

// JPEG validation is the largest CPU cost on asia1 during the grand prix, so
// it is offloaded to idle hosts (ISUREAL_JPEG_REMOTES, in preference order).
// Each remote has a small in-flight cap; when every remote is busy or fails,
// the image is checked locally, so results never depend on a remote.

type jpegRemote struct {
	addr     string
	cap      int32
	timeout  time.Duration
	inflight atomic.Int32
}

var (
	jpegRemotes      = parseJPEGRemotes(os.Getenv("ISUREAL_JPEG_REMOTES"))
	jpegRemoteClient = &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 300 * time.Second},
	}
)

// "addr[/cap[/timeoutMs]],..." e.g. "192.168.4.10:8080/2/300,192.168.2.10:8080/2/600"
func parseJPEGRemotes(s string) []*jpegRemote {
	var out []*jpegRemote
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		parts := strings.Split(f, "/")
		r := &jpegRemote{addr: parts[0], cap: 2, timeout: 300 * time.Millisecond}
		if len(parts) > 1 {
			if _, err := fmt.Sscanf(parts[1], "%d", &r.cap); err != nil || r.cap <= 0 {
				r.cap = 2
			}
		}
		if len(parts) > 2 {
			var ms int
			if _, err := fmt.Sscanf(parts[2], "%d", &ms); err == nil && ms > 0 {
				r.timeout = time.Duration(ms) * time.Millisecond
			}
		}
		out = append(out, r)
	}
	return out
}

func checkJPEG(ctx context.Context, data []byte) (w, h int, ok bool) {
	for _, r := range jpegRemotes {
		if r.inflight.Add(1) > r.cap {
			r.inflight.Add(-1)
			continue
		}
		w, h, ok, err := r.check(ctx, data)
		r.inflight.Add(-1)
		if err == nil {
			return w, h, ok
		}
		break
	}
	w, h, err := jpegcheck.Check(bytes.NewReader(data))
	return w, h, err == nil
}

func (r *jpegRemote) check(ctx context.Context, data []byte) (w, h int, ok bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+r.addr+"/internal/jpegcheck", bytes.NewReader(data))
	if err != nil {
		return 0, 0, false, err
	}
	req.Header.Set(forwardedSecretHeader, forwardSecret)
	res, err := jpegRemoteClient.Do(req)
	if err != nil {
		return 0, 0, false, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, 0, false, err
	}
	if res.StatusCode != http.StatusOK {
		return 0, 0, false, fmt.Errorf("jpegcheck remote %s: %d", r.addr, res.StatusCode)
	}
	var okInt int
	if _, err := fmt.Sscanf(string(body), "%d %d %d", &okInt, &w, &h); err != nil {
		return 0, 0, false, err
	}
	return w, h, okInt == 1, nil
}

func registerJPEGCheckRoutes(e *echo.Echo) {
	e.POST("/internal/jpegcheck", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		data, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return c.NoContent(http.StatusBadRequest)
		}
		w, h, err := jpegcheck.Check(bytes.NewReader(data))
		if err != nil {
			return c.String(http.StatusOK, "0 0 0")
		}
		return c.String(http.StatusOK, fmt.Sprintf("1 %d %d", w, h))
	})
}
