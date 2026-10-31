package isureal

import (
	"bytes"
	"context"
	"crypto/subtle"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_MEMORIES_CACHE_ORIGIN=host:port (asia1, with ISUREAL_MEMORIES_CACHE=1):
// cache misses are computed by that app (asia2, next to PG) instead of
// locally. The request goes out with the same path, query and Authorization
// plus X-Isureal-Cache-Origin=<fwd secret>; the origin answers as usual and
// adds the cache tags in X-Isureal-Cache-Tags (only for such requests, and
// only when cacheable). begin() is taken before the request, so the fill race
// rule is unchanged: changes committed before the origin's query are in the
// body, later ones are applied by this host's model and veto / remove the entry.
var (
	memRespOrigin       = os.Getenv("ISUREAL_MEMORIES_CACHE_ORIGIN")
	memRespOriginRegion = os.Getenv("ISUREAL_REGION")
	memRespOriginClient = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 128, MaxIdleConns: 256, IdleConnTimeout: 90 * time.Second}}
)

const (
	memRespOriginHeader = "X-Isureal-Cache-Origin"
	memRespTagsHeader   = "X-Isureal-Cache-Tags"
)

func memRespIsOriginReq(c *echo.Context) bool {
	v := c.Request().Header.Get(memRespOriginHeader)
	return v != "" && forwardSecret != "" && subtle.ConstantTimeCompare([]byte(v), []byte(forwardSecret)) == 1
}

func memRespSetTags(c *echo.Context, tags []string) {
	if tags != nil && memRespIsOriginReq(c) {
		c.Response().Header().Set(memRespTagsHeader, strings.Join(tags, ","))
	}
}

type memOriginResp struct {
	code int
	hdr  http.Header
	body []byte
	tags []string // nil = not cacheable
}

// ISUREAL_MEMORIES_ORIGIN_DEADLINE_MS > 0: a Memories miss sent to the origin
// that has not answered within this time is cancelled and computed locally
// (same DB path as without an origin; the fill taken before still applies).
var (
	memOriginDeadline     = time.Duration(envIntDefault("ISUREAL_MEMORIES_ORIGIN_DEADLINE_MS", 0)) * time.Millisecond
	memOriginDeadlineHits atomic.Int64
)

// fetchOriginDeadline is fetchOrigin bounded by d (0 = no bound).
func fetchOriginDeadline(c *echo.Context, d time.Duration) (memOriginResp, bool) {
	if d <= 0 || memRespOrigin == "" || forwardSecret == "" {
		return fetchOrigin(c)
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), d)
	defer cancel()
	r, ok := fetchOriginCtx(ctx, c.Request())
	if !ok && ctx.Err() == context.DeadlineExceeded && c.Request().Context().Err() == nil {
		memOriginDeadlineHits.Add(1)
	}
	return r, ok
}

// fetchOrigin: ok=false = origin unusable, compute locally.
func fetchOrigin(c *echo.Context) (memOriginResp, bool) {
	return fetchOriginCtx(c.Request().Context(), c.Request())
}

func fetchOriginCtx(ctx context.Context, in *http.Request) (memOriginResp, bool) {
	var r memOriginResp
	if memRespOrigin == "" || forwardSecret == "" {
		return r, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+memRespOrigin+in.URL.RequestURI(), nil)
	if err != nil {
		return r, false
	}
	if v := in.Header.Get(echo.HeaderAuthorization); v != "" {
		req.Header.Set(echo.HeaderAuthorization, v)
	}
	req.Header.Set(memRespOriginHeader, forwardSecret)
	// europe1 / america1 (ISUREAL_MEMORIES_CACHE_ORIGIN set there too): the origin (asia2) checks the
	// viewer's region against the request's, so say which region the request arrived in, the way a
	// forwarded write does (trusted only from the app hosts' addresses with the secret).
	if memRespOriginRegion != "" && memRespOriginRegion != "asia" {
		req.Header.Set(forwardedRegionHeader, memRespOriginRegion)
		req.Header.Set(forwardedSecretHeader, forwardSecret)
	}
	resp, err := memRespOriginClient.Do(req)
	if err != nil {
		return r, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode >= 500 {
		return r, false
	}
	// the origin did not accept the request's region (fwd headers / source address not trusted):
	// a Memories / export of the viewer's own data never answers that, so compute locally instead.
	if resp.StatusCode == http.StatusForbidden && bytes.Contains(body, []byte("region_mismatch")) {
		return r, false
	}
	r.code, r.hdr, r.body = resp.StatusCode, resp.Header, body
	if t := resp.Header.Get(memRespTagsHeader); t != "" && resp.StatusCode == http.StatusOK {
		r.tags = strings.Split(t, ",")
	}
	return r, true
}

func (r memOriginResp) write(c *echo.Context) error {
	h := c.Response().Header()
	for _, k := range []string{echo.HeaderContentDisposition, echo.HeaderCacheControl} {
		if v := r.hdr.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	return c.Blob(r.code, r.hdr.Get(echo.HeaderContentType), r.body)
}
