package isureal

import (
	"crypto/subtle"
	"io"
	"net/http"
	"os"
	"strings"
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

// fetchOrigin: ok=false = origin unusable, compute locally.
func fetchOrigin(c *echo.Context) (memOriginResp, bool) {
	var r memOriginResp
	if memRespOrigin == "" || forwardSecret == "" {
		return r, false
	}
	in := c.Request()
	req, err := http.NewRequestWithContext(in.Context(), http.MethodGet, "http://"+memRespOrigin+in.URL.RequestURI(), nil)
	if err != nil {
		return r, false
	}
	if v := in.Header.Get(echo.HeaderAuthorization); v != "" {
		req.Header.Set(echo.HeaderAuthorization, v)
	}
	req.Header.Set(memRespOriginHeader, forwardSecret)
	resp, err := memRespOriginClient.Do(req)
	if err != nil {
		return r, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode >= 500 {
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
