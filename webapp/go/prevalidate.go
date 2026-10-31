package isureal

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"example.com/isureal-company/isureal/internal/jpegcheck"
	"github.com/labstack/echo/v5"
)

// JPEG pre-validation (ISUREAL_PREVALIDATE=1).
//
// On europe1/america1 nginx sends the image-carrying writes (POST /api/posts,
// PUT /api/me/profile-image, POST /api/posts/:id/realmojis) to the local app
// with X-Isureal-Prevalidate=<secret> instead of straight to asia1. The local
// app checks every JPEG in the body with jpegcheck and forwards the unchanged
// body to asia1 with the result in X-Isureal-Jpeg:
//
//	<sha256(image bytes)>=<w>x<h>|bad,...;<hex HMAC-SHA256(secret, entries)>
//
// asia1 verifies the HMAC and stashes the results in the request context;
// checkJPEG then answers from it when sha256 of the bytes the handler is about
// to check matches an entry. A result is bound to the exact image bytes, so a
// header replayed with another body (or altered bytes) simply misses and the
// normal check runs. Handlers are untouched: checkJPEG is still called at the
// same point, so the order of errors cannot change.

const (
	prevalidateHeader = "X-Isureal-Prevalidate"
	jpegResultHeader  = "X-Isureal-Jpeg"
	jpegResultMACCtx  = "isureal-jpeg-v1|"
	prevalidateMaxImg = 4
)

var (
	prevalidateOn     = os.Getenv("ISUREAL_PREVALIDATE") == "1"
	prevalidateTarget = os.Getenv("ISUREAL_PREVALIDATE_TARGET") // e.g. 192.168.1.10:8080; empty = accept side only
	prevalidateHits   atomic.Int64
)

type jpegResult struct {
	w, h int
	ok   bool
}

type jpegResultsKey struct{}

func init() {
	if !prevalidateOn {
		return
	}
	go func() {
		for range time.Tick(10 * time.Second) {
			if n := prevalidateHits.Swap(0); n > 0 {
				slog.Info("jpegcheck", "prevalidated", n)
			}
		}
	}()
}

func jpegResultsMAC(secret, entries string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(jpegResultMACCtx))
	m.Write([]byte(entries))
	return hex.EncodeToString(m.Sum(nil))
}

func formatJPEGEntry(sum [32]byte, r jpegResult) string {
	s := hex.EncodeToString(sum[:]) + "="
	if !r.ok {
		return s + "bad"
	}
	return s + strconv.Itoa(r.w) + "x" + strconv.Itoa(r.h)
}

func signJPEGResults(secret string, entries []string) string {
	e := strings.Join(entries, ",")
	return e + ";" + jpegResultsMAC(secret, e)
}

// parseJPEGResults returns nil unless the header carries a valid MAC.
func parseJPEGResults(secret, v string) map[[32]byte]jpegResult {
	if secret == "" || v == "" {
		return nil
	}
	i := strings.LastIndexByte(v, ';')
	if i < 0 {
		return nil
	}
	entries, mac := v[:i], v[i+1:]
	if !hmac.Equal([]byte(mac), []byte(jpegResultsMAC(secret, entries))) {
		return nil
	}
	out := make(map[[32]byte]jpegResult, 2)
	for _, e := range strings.Split(entries, ",") {
		k, val, found := strings.Cut(e, "=")
		var sum [32]byte
		if !found || len(k) != 64 {
			return nil
		}
		if _, err := hex.Decode(sum[:], []byte(k)); err != nil {
			return nil
		}
		if val == "bad" {
			out[sum] = jpegResult{}
			continue
		}
		ws, hs, found := strings.Cut(val, "x")
		w, err1 := strconv.Atoi(ws)
		h, err2 := strconv.Atoi(hs)
		if !found || err1 != nil || err2 != nil {
			return nil
		}
		out[sum] = jpegResult{w: w, h: h, ok: true}
	}
	return out
}

// prevalidated is consulted first by checkJPEG.
func prevalidated(ctx context.Context, data []byte) (w, h int, ok, hit bool) {
	m, _ := ctx.Value(jpegResultsKey{}).(map[[32]byte]jpegResult)
	if m == nil {
		return 0, 0, false, false
	}
	r, found := m[sha256.Sum256(data)]
	if !found {
		return 0, 0, false, false
	}
	prevalidateHits.Add(1)
	return r.w, r.h, r.ok, true
}

func prevalidatePeer(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.Equal(net.IPv4(192, 168, 1, 10)) || ip.Equal(net.IPv4(192, 168, 2, 10)) || ip.Equal(net.IPv4(192, 168, 3, 10)))
}

// imageParts returns the bytes of every JPEG candidate in the body, exactly as
// the handlers will see them (raw body for image/jpeg, file parts decoded by
// mime/multipart like ReadForm does). Parse problems just end the scan.
func imageParts(contentType string, body []byte) [][]byte {
	if contentType == "image/jpeg" {
		return [][]byte{body}
	}
	mt, params, err := mime.ParseMediaType(contentType)
	if err != nil || mt != "multipart/form-data" || params["boundary"] == "" {
		return nil
	}
	mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	var out [][]byte
	for len(out) < prevalidateMaxImg {
		p, err := mr.NextPart()
		if err != nil {
			break
		}
		if p.FileName() == "" {
			p.Close()
			continue
		}
		data, err := io.ReadAll(p)
		p.Close()
		if err != nil {
			break
		}
		out = append(out, data)
	}
	return out
}

func jpegResultsHeader(secret, contentType string, body []byte) string {
	parts := imageParts(contentType, body)
	if len(parts) == 0 {
		return ""
	}
	entries := make([]string, 0, len(parts))
	for _, p := range parts {
		w, h, err := jpegcheck.Check(bytes.NewReader(p))
		entries = append(entries, formatJPEGEntry(sha256.Sum256(p), jpegResult{w: w, h: h, ok: err == nil}))
	}
	return signJPEGResults(secret, entries)
}

func newPrevalidateProxy(target, region string) *httputil.ReverseProxy {
	u := &url.URL{Scheme: "http", Host: target}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.Host = pr.In.Host // proxy_set_header Host $host
			// Rewrite drops these; nginx passes them through untouched.
			for _, k := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				if v, ok := pr.In.Header[k]; ok {
					pr.Out.Header[k] = v
				}
			}
			pr.Out.Header.Del(prevalidateHeader)
			pr.Out.Header.Set(forwardedRegionHeader, region)
			pr.Out.Header.Set(forwardedSecretHeader, forwardSecret)
		},
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     300 * time.Second,
			DisableCompression:  true,
			ForceAttemptHTTP2:   false,
		},
		BufferPool: proxyBufPool{},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Warn("prevalidate proxy", "error", err)
			w.WriteHeader(http.StatusBadGateway)
		},
	}
}

type proxyBufPool struct{}

var proxyBufs = make(chan []byte, 256)

func (proxyBufPool) Get() []byte {
	select {
	case b := <-proxyBufs:
		return b
	default:
		return make([]byte, 32*1024)
	}
}

func (proxyBufPool) Put(b []byte) {
	select {
	case proxyBufs <- b:
	default:
	}
}

// registerPrevalidate installs the accept side (all hosts) and, when a target
// is configured, the validate-and-forward side.
func registerPrevalidate(e *echo.Echo, region string) {
	if !prevalidateOn || forwardSecret == "" {
		return
	}
	var proxy *httputil.ReverseProxy
	if prevalidateTarget != "" {
		proxy = newPrevalidateProxy(prevalidateTarget, region)
	}
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			if proxy != nil && req.Header.Get(prevalidateHeader) == forwardSecret {
				return prevalidateForward(c, proxy)
			}
			if v := req.Header.Get(jpegResultHeader); v != "" {
				req.Header.Del(jpegResultHeader)
				if prevalidatePeer(req.RemoteAddr) {
					if m := parseJPEGResults(forwardSecret, v); m != nil {
						c.SetRequest(req.WithContext(context.WithValue(req.Context(), jpegResultsKey{}, m)))
					}
				}
			}
			return next(c)
		}
	})
}

func prevalidateForward(c *echo.Context, proxy *httputil.ReverseProxy) error {
	req := c.Request()
	var buf bytes.Buffer
	if req.ContentLength > 0 && req.ContentLength <= 64<<20 {
		buf.Grow(int(req.ContentLength))
	}
	if _, err := buf.ReadFrom(req.Body); err != nil {
		return err
	}
	req.Body.Close()
	body := buf.Bytes()
	req.Header.Del(jpegResultHeader)
	if v := jpegResultsHeader(forwardSecret, req.Header.Get("Content-Type"), body); v != "" {
		req.Header.Set(jpegResultHeader, v)
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	req.TransferEncoding = nil
	proxy.ServeHTTP(c.Response(), req)
	return nil
}
