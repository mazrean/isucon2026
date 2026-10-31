package isureal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"reflect"
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
	addr    string
	timeout time.Duration
	slots   chan struct{}
	label   string // POST_TIMING の段階名 (アドレスの第 3 オクテット: 4=asia2, 2=eu, 3=am)
}

var (
	jpegRemotes      = parseJPEGRemotes(os.Getenv("ISUREAL_JPEG_REMOTES"))
	jpegLocalSlots   = make(chan struct{}, envIntOrDefault("ISUREAL_JPEG_LOCAL_SLOTS", 1)) // asia1's own CPU is the scarce one
	jpegRemoteClient = &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 300 * time.Second},
	}
)

// "addr[/cap[/timeoutMs]],..." e.g. "192.168.4.10:8080/2/300,192.168.2.10:8080/8/900"
func parseJPEGRemotes(s string) []*jpegRemote {
	var out []*jpegRemote
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		parts := strings.Split(f, "/")
		capacity := 2
		r := &jpegRemote{addr: parts[0], timeout: 300 * time.Millisecond}
		if len(parts) > 1 {
			if _, err := fmt.Sscanf(parts[1], "%d", &capacity); err != nil || capacity <= 0 {
				capacity = 2
			}
		}
		if len(parts) > 2 {
			var ms int
			if _, err := fmt.Sscanf(parts[2], "%d", &ms); err == nil && ms > 0 {
				r.timeout = time.Duration(ms) * time.Millisecond
			}
		}
		r.slots = make(chan struct{}, capacity)
		r.label = remoteLabel(r.addr)
		out = append(out, r)
	}
	return out
}

func remoteLabel(addr string) string {
	host, _, _ := strings.Cut(addr, ":")
	if oct := strings.Split(host, "."); len(oct) == 4 {
		return oct[2]
	}
	return host
}

// ISUREAL_JPEG_LOCAL_SOCK points at webapp/rust/jpegcheckd (HTTP/1.1 on a
// unix socket, same protocol as POST /internal/jpegcheck). When set, the local
// check goes there; any error (daemon down, 5xx) falls back to in-process.
var (
	jpegLocalSock   = os.Getenv("ISUREAL_JPEG_LOCAL_SOCK")
	jpegLocalClient = &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", jpegLocalSock)
			},
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     300 * time.Second,
		},
	}
)

var jpegStats struct{ remoteOK, remoteErr, local, sockErr, gpCapped, gpAsia atomic.Int64 }

var jpegRegion = envOrDefault("ISUREAL_REGION", "asia")

func init() {
	go func() {
		for range time.Tick(10 * time.Second) {
			ok, bad, loc, se, gc, ga := jpegStats.remoteOK.Swap(0), jpegStats.remoteErr.Swap(0), jpegStats.local.Swap(0), jpegStats.sockErr.Swap(0), jpegStats.gpCapped.Swap(0), jpegStats.gpAsia.Swap(0)
			if ok+bad+loc > 0 {
				slog.Info("jpegcheck", "remote_ok", ok, "remote_err", bad, "local", loc, "sock", jpegLocalSock != "", "sock_err", se, "gp_capped", gc, "gp_asia", ga)
			}
		}
	}()
}

// checkJPEGLocal checks on this host: via jpegcheckd when configured, else
// (or on any daemon error) in-process.
func checkJPEGLocal(data []byte) (w, h int, ok bool) {
	jpegStats.local.Add(1)
	if jpegLocalSock != "" {
		w, h, ok, err := postJPEGCheck(context.Background(), jpegLocalClient, "http://jpegcheckd/internal/jpegcheck", "local", data, 2*time.Second)
		if err == nil {
			return w, h, ok
		}
		jpegStats.sockErr.Add(1)
	}
	w, h, err := jpegcheck.Check(bytes.NewReader(data))
	return w, h, err == nil
}

// ISUREAL_JPEG_GP_ASIA="L/R" (既定 "" = off): 自地域が GP の間、JPEG 検査を asia の中で回す。
// 委譲先は ISUREAL_JPEG_REMOTES の先頭 max(ISUREAL_GP_JPEG_REMOTES_MAX,1) 個 (asia2) だけ、
// asia1 のローカル枠は L、asia2 の枠は R (GP 用の別の枠、平時の枠とは独立)。ローカル (jpegcheckd 2.76ms/枚、
// 回線を使わない) を先に試し、空きが無ければ asia2、どちらも埋まっていれば先に空いた方を待つ。
// eu/am への 360KB の委譲は GP 以外の窓だけになる。判定 (受理/拒否) は変わらない。
var jpegGP = parseJPEGGP(os.Getenv("ISUREAL_JPEG_GP_ASIA"))

type jpegGPConf struct {
	local   chan struct{}
	remotes []*jpegRemote
}

func parseJPEGGP(s string) *jpegGPConf {
	var l, r int
	if n, _ := fmt.Sscanf(strings.TrimSpace(s), "%d/%d", &l, &r); n < 1 || l <= 0 {
		return nil
	}
	if r <= 0 {
		r = 2
	}
	keep := gpJPEGRemotesMax
	if keep <= 0 {
		keep = 1
	}
	c := &jpegGPConf{local: make(chan struct{}, l)}
	for i, base := range jpegRemotes {
		if i >= keep {
			break
		}
		c.remotes = append(c.remotes, &jpegRemote{addr: base.addr, timeout: base.timeout, slots: make(chan struct{}, r), label: base.label})
	}
	return c
}

// checkJPEG takes the first free slot: remotes in preference order, then the
// local slots; when everything is busy it waits for whichever frees up
// first. A failed remote call is redone locally, so the result never depends
// on a remote.
func checkJPEG(ctx context.Context, data []byte) (w, h int, ok bool) {
	if w, h, ok, hit := prevalidated(ctx, data); hit {
		return w, h, ok
	}
	remotes, local, localFirst := jpegRemotes, jpegLocalSlots, false
	if jpegGP != nil && len(jpegRemotes) > 0 && gpLocalActive(jpegRegion) {
		remotes, local, localFirst = jpegGP.remotes, jpegGP.local, true
		jpegStats.gpAsia.Add(1)
	} else if gpJPEGRemotesMax > 0 && len(remotes) > gpJPEGRemotesMax && gpLocalActive(jpegRegion) {
		remotes = remotes[:gpJPEGRemotesMax]
		jpegStats.gpCapped.Add(1)
	}
	return checkJPEGWith(ctx, data, remotes, local, localFirst)
}

func checkJPEGWith(ctx context.Context, data []byte, remotes []*jpegRemote, local chan struct{}, localFirst bool) (w, h int, ok bool) {
	if len(remotes) == 0 {
		return timedLocal(data)
	}
	if localFirst {
		select {
		case local <- struct{}{}:
			defer func() { <-local }()
			return timedLocal(data)
		default:
		}
	}
	for _, r := range remotes {
		select {
		case r.slots <- struct{}{}:
			return r.run(ctx, data)
		default:
		}
	}
	waitStart := time.Now()
	cases := make([]reflect.SelectCase, 0, len(remotes)+2)
	for _, r := range remotes {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(r.slots), Send: reflect.ValueOf(struct{}{})})
	}
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(local), Send: reflect.ValueOf(struct{}{})})
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	chosen, _, _ := reflect.Select(cases)
	postTimingAdd("jpeg_wait", time.Since(waitStart))
	switch {
	case chosen < len(remotes):
		return remotes[chosen].run(ctx, data)
	case chosen == len(remotes):
		defer func() { <-local }()
		return timedLocal(data)
	default: // client gone; the result is not used
		return checkJPEGLocal(data)
	}
}

func timedLocal(data []byte) (w, h int, ok bool) {
	t := time.Now()
	w, h, ok = checkJPEGLocal(data)
	postTimingAdd("jpeg_local", time.Since(t))
	return w, h, ok
}

func (r *jpegRemote) run(ctx context.Context, data []byte) (w, h int, ok bool) {
	t := time.Now()
	w, h, ok, err := r.check(ctx, data)
	<-r.slots
	if err != nil {
		jpegStats.remoteErr.Add(1)
		postTimingAdd("jpeg_err_"+r.label, time.Since(t))
		return timedLocal(data)
	}
	postTimingAdd("jpeg_rmt_"+r.label, time.Since(t))
	jpegStats.remoteOK.Add(1)
	return w, h, ok
}

func (r *jpegRemote) check(ctx context.Context, data []byte) (w, h int, ok bool, err error) {
	return postJPEGCheck(ctx, jpegRemoteClient, "http://"+r.addr+"/internal/jpegcheck", r.addr, data, r.timeout)
}

func postJPEGCheck(ctx context.Context, client *http.Client, url, name string, data []byte, timeout time.Duration) (w, h int, ok bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return 0, 0, false, err
	}
	req.Header.Set(forwardedSecretHeader, forwardSecret)
	res, err := client.Do(req)
	if err != nil {
		return 0, 0, false, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, 0, false, err
	}
	if res.StatusCode != http.StatusOK {
		return 0, 0, false, fmt.Errorf("jpegcheck remote %s: %d", name, res.StatusCode)
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
		data, err := readAllSized(c.Request().Body, c.Request().ContentLength)
		if err != nil {
			return c.NoContent(http.StatusBadRequest)
		}
		w, h, ok := checkJPEGLocal(data)
		if !ok {
			return c.String(http.StatusOK, "0 0 0")
		}
		return c.String(http.StatusOK, fmt.Sprintf("1 %d %d", w, h))
	})
}
