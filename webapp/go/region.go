package isureal

import (
	"crypto/subtle"
	"net"
	"net/http"
	"os"

	"github.com/labstack/echo/v5"
)

// forwardedRegionHeader is set by a peer region's nginx when it forwards a
// request to this host. It is trusted only from peer app hosts with the
// shared secret; the local nginx clears both headers on external requests.
const (
	forwardedRegionHeader = "X-Isureal-Fwd-Region"
	forwardedSecretHeader = "X-Isureal-Fwd-Secret"
)

var forwardSecret = os.Getenv("ISUREAL_FWD_SECRET")

func requestRegion(h *HandlerContext, c *echo.Context) string {
	req := c.Request()
	region := req.Header.Get(forwardedRegionHeader)
	if region == "" || !validRegion(region) || forwardSecret == "" ||
		subtle.ConstantTimeCompare([]byte(req.Header.Get(forwardedSecretHeader)), []byte(forwardSecret)) != 1 {
		return h.Config.Region
	}
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return h.Config.Region
	}
	ip := net.ParseIP(host)
	if ip == nil || !(ip.Equal(net.IPv4(192, 168, 1, 10)) || ip.Equal(net.IPv4(192, 168, 2, 10)) || ip.Equal(net.IPv4(192, 168, 3, 10))) {
		return h.Config.Region
	}
	return region
}

func RegionMiddleware(h *HandlerContext) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			session := GetSession(c)
			if session == nil {
				return NewAPIError(http.StatusUnauthorized, "invalid_session")
			}
			if session.Region != requestRegion(h, c) {
				return NewAPIError(http.StatusForbidden, "region_mismatch")
			}
			return next(c)
		}
	}
}
