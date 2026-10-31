package isureal

// nginx (etc/app/isureal.conf + etc/app/nginx.conf 相当) と Front を同じリクエスト列で比べる差分テスト。
// 実物の nginx バイナリが要る:
//   apt-get download nginx && dpkg-deb -x nginx_*.deb root
//   FRONT_DIFF_NGINX=$PWD/root/usr/sbin/nginx go test -run TestFrontDiff -v .
// 比較しないもの: Server, Date, Connection: keep-alive (nginx は明示、Go は HTTP/1.1 で省略)。
// gzip の本文は展開してから比べる (zlib と Go flate で圧縮バイト列は違う)。

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// diffStub は upstream のアプリの代わり。受け取ったリクエストをそのまま JSON で返す。
func diffStub(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/notifications"):
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(200)
			fmt.Fprintf(w, "data: {\"server\":%q}\n\n", name)
			http.NewResponseController(w).Flush()
			time.Sleep(300 * time.Millisecond)
			fmt.Fprint(w, "data: 2\n\n")
			return
		case r.URL.Path == "/api/big":
			w.Header().Set("Content-Type", "application/json")
			for i := 0; i < 100; i++ {
				fmt.Fprintf(w, "%01000d", i)
			}
			return
		case r.URL.Path == "/api/memories/503" && name == "heavy":
			w.WriteHeader(503)
			return
		case r.URL.Path == "/api/nocontent":
			w.WriteHeader(204)
			return
		case r.URL.Path == "/api/html":
			w.Header().Set("Content-Type", "text/html")
			io.WriteString(w, strings.Repeat("<p>x</p>", 20))
			return
		}
		body, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(body)
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		hdr := map[string][]string{}
		for k, v := range r.Header {
			hdr[k] = v
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Stub", name)
		json.NewEncoder(w).Encode(map[string]any{
			"server": name, "method": r.Method, "uri": r.RequestURI, "host": r.Host, "remote": host,
			"proto": r.Proto, "headers": hdr, "len": len(body), "sha": hex.EncodeToString(sum[:8]), "cl": r.ContentLength,
		})
	})
}

func listenStub(t *testing.T, h http.Handler) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

func deadAddr(t *testing.T) string {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	a := ln.Addr().String()
	ln.Close()
	return a
}

type diffScenario struct {
	name                  string
	region                string
	fwd, fwdGet           bool
	fwdTarget, heavy      string // addr; heavy "" = local
	nginxHeavy, nginxFwdT string
}

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func startNginx(t *testing.T, bin, dir string, sc diffScenario, localAddr string) string {
	root := repoRoot()
	conf, err := os.ReadFile(filepath.Join(root, "etc/app/isureal.conf"))
	if err != nil {
		t.Fatal(err)
	}
	port := deadAddr(t)
	b01 := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	heavyHost := "127.0.0.1"
	c := string(conf)
	c = strings.ReplaceAll(c, "server 127.0.0.1:8080", "server "+localAddr)
	c = strings.ReplaceAll(c, "__HEAVY_TARGET__:8080", sc.nginxHeavy)
	c = strings.ReplaceAll(c, "__FWD_TARGET__:8080", sc.nginxFwdT)
	c = strings.NewReplacer("__REGION__", sc.region, "__FWD__", b01(sc.fwd), "__FWD_GET__", b01(sc.fwdGet),
		"__FWD_SECRET__", "s3cret", "__HEAVY_TARGET__", heavyHost).Replace(c)
	c = strings.Replace(c, "listen 80 default_server backlog=4096 reuseport;", "listen "+port+";", 1)
	c = strings.Replace(c, "root /home/isucon/webapp/public;", "root "+filepath.Join(root, "webapp/public")+";", 1)
	main := fmt.Sprintf(`worker_processes 1;
pid %[1]s/nginx.pid;
error_log %[1]s/error.log;
events { worker_connections 1024; multi_accept on; }
http {
  sendfile on; tcp_nopush on; types_hash_max_size 2048;
  include %[2]s/etc/asia1/nginx/mime.types;
  default_type application/octet-stream;
  keepalive_timeout 75s; keepalive_requests 100000;
  access_log off;
  gzip on;
  client_body_temp_path %[1]s/cbt; proxy_temp_path %[1]s/pt; fastcgi_temp_path %[1]s/ft; uwsgi_temp_path %[1]s/ut; scgi_temp_path %[1]s/st;
%[3]s
}
`, dir, root, c)
	cf := filepath.Join(dir, "nginx.conf")
	os.WriteFile(cf, []byte(main), 0o644)
	cmd := exec.Command(bin, "-p", dir, "-c", cf, "-e", filepath.Join(dir, "error.log"), "-g", "daemon off;")
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", port); err == nil {
			c.Close()
			return port
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("nginx did not start")
	return ""
}

type rawResp struct {
	status   int
	header   http.Header
	te       []string
	cl       int64
	body     []byte
	firstGap time.Duration // SSE: 最初の本文が届くまで
	err      string
}

func doRaw(addr, method, raw string, sse bool) rawResp {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return rawResp{err: err.Error()}
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(20 * time.Second))
	go io.Copy(conn, strings.NewReader(raw))
	br := bufio.NewReader(conn)
	start := time.Now()
	res, err := http.ReadResponse(br, &http.Request{Method: method})
	if err != nil {
		return rawResp{err: err.Error()}
	}
	out := rawResp{status: res.StatusCode, header: res.Header, te: res.TransferEncoding, cl: res.ContentLength}
	if sse {
		buf := make([]byte, 1)
		if _, err := io.ReadFull(res.Body, buf); err == nil {
			out.firstGap = time.Since(start)
		}
		rest, _ := io.ReadAll(res.Body)
		out.body = append(buf, rest...)
	} else {
		out.body, _ = io.ReadAll(res.Body)
	}
	if res.Header.Get("Content-Encoding") == "gzip" && len(out.body) > 0 {
		zr, err := gzip.NewReader(bytes.NewReader(out.body))
		if err != nil {
			out.err = "bad gzip: " + err.Error()
		} else {
			out.body, _ = io.ReadAll(zr)
		}
	}
	return out
}

func normHeader(h http.Header) []string {
	var out []string
	for k, v := range h {
		switch k {
		case "Server", "Date":
			continue
		case "Connection":
			if len(v) == 1 && strings.EqualFold(v[0], "keep-alive") {
				continue
			}
		}
		if k == "Location" { // listen ポートだけはテスト環境で違う
			v = []string{locPort.ReplaceAllString(strings.Join(v, ", "), ":PORT/")}
		}
		out = append(out, k+": "+strings.Join(v, ", "))
	}
	sort.Strings(out)
	return out
}

type diffReq struct {
	name   string
	method string
	path   string
	hdr    []string
	body   string
	chunk  bool
	sse    bool
	proto  string
	noBody bool // Content-Length だけ送り本文は送らない (413 判定)
	cl     int64
}

func (d diffReq) raw() string {
	var b strings.Builder
	proto := d.proto
	if proto == "" {
		proto = "HTTP/1.1"
	}
	fmt.Fprintf(&b, "%s %s %s\r\n", d.method, d.path, proto)
	hasHost := false
	for _, h := range d.hdr {
		if strings.HasPrefix(strings.ToLower(h), "host:") {
			hasHost = true
		}
	}
	if !hasHost {
		b.WriteString("Host: app.example.test\r\n")
	}
	for _, h := range d.hdr {
		b.WriteString(h + "\r\n")
	}
	switch {
	case d.noBody:
		fmt.Fprintf(&b, "Content-Length: %d\r\n", d.cl)
	case d.chunk:
		b.WriteString("Transfer-Encoding: chunked\r\n")
	case d.body != "":
		fmt.Fprintf(&b, "Content-Length: %d\r\n", len(d.body))
	}
	b.WriteString("Connection: close\r\n\r\n")
	if d.chunk {
		const step = 1 << 20
		for i := 0; i < len(d.body); i += step {
			j := min(i+step, len(d.body))
			fmt.Fprintf(&b, "%x\r\n%s\r\n", j-i, d.body[i:j])
		}
		b.WriteString("0\r\n\r\n")
	} else if !d.noBody {
		b.WriteString(d.body)
	}
	return b.String()
}

func TestFrontDiff(t *testing.T) {
	bin := os.Getenv("FRONT_DIFF_NGINX")
	if bin == "" {
		t.Skip("FRONT_DIFF_NGINX (nginx 1.24 binary) not set")
	}
	local := listenStub(t, diffStub("local"))
	remote := listenStub(t, diffStub("remote"))
	heavy := listenStub(t, diffStub("heavy"))
	dead := deadAddr(t)
	scenarios := []diffScenario{
		{name: "asia1", region: "asia", heavy: heavy, nginxHeavy: heavy, nginxFwdT: remote, fwdTarget: remote},
		{name: "europe1", region: "europe", fwd: true, fwdTarget: remote, nginxHeavy: local, nginxFwdT: remote},
		{name: "europe1-fwdget", region: "europe", fwd: true, fwdGet: true, fwdTarget: remote, nginxHeavy: local, nginxFwdT: remote},
		{name: "asia1-heavydown", region: "asia", heavy: dead, nginxHeavy: dead, nginxFwdT: remote, fwdTarget: remote},
		{name: "europe1-remotedown", region: "europe", fwd: true, fwdTarget: dead, nginxHeavy: local, nginxFwdT: dead},
	}
	idx, _ := os.Stat(filepath.Join(repoRoot(), "webapp/public/index.html"))
	lm := idx.ModTime().UTC().Format(http.TimeFormat)
	etag := fmt.Sprintf("\"%x-%x\"", idx.ModTime().Unix(), idx.Size())
	big32 := strings.Repeat("a", 32<<20)
	reqs := []diffReq{
		{name: "root", method: "GET", path: "/"},
		{name: "index", method: "GET", path: "/index.html"},
		{name: "css", method: "GET", path: "/assets/index-DRoYlDJr.css"},
		{name: "js", method: "GET", path: "/assets/index-DVx2k2rA.js"},
		{name: "svg", method: "GET", path: "/isureal-logo.svg"},
		{name: "spa", method: "GET", path: "/posts/123?x=1"},
		{name: "spa-deep", method: "GET", path: "/a/b/c/d.png"},
		{name: "dir-slash", method: "GET", path: "/assets/"},
		{name: "dir", method: "GET", path: "/assets"},
		{name: "api-noslash", method: "GET", path: "/api"},
		{name: "api-noslash-q", method: "POST", path: "/api?x=1", body: "a"},
		{name: "api-noslash-head", method: "HEAD", path: "/api"},
		{name: "images-noslash", method: "GET", path: "/api/images"},
		{name: "internal-dir", method: "GET", path: "/internal/"},
		{name: "internal-noslash", method: "GET", path: "/internal"},
		{name: "merge-slash", method: "GET", path: "//assets//index-DRoYlDJr.css"},
		{name: "dotdot", method: "GET", path: "/assets/../index.html"},
		{name: "dotdot-root", method: "GET", path: "/../etc/passwd"},
		{name: "pct", method: "GET", path: "/%61ssets/index-DRoYlDJr.css"},
		{name: "upper-ext", method: "GET", path: "/INDEX.HTML"},
		{name: "gzip", method: "GET", path: "/", hdr: []string{"Accept-Encoding: gzip, deflate, br"}},
		{name: "gzip-spa", method: "GET", path: "/foo", hdr: []string{"Accept-Encoding: gzip"}},
		{name: "gzip-head", method: "HEAD", path: "/", hdr: []string{"Accept-Encoding: gzip"}},
		{name: "gzip-q0", method: "GET", path: "/", hdr: []string{"Accept-Encoding: gzip;q=0"}},
		{name: "gzip-q05", method: "GET", path: "/", hdr: []string{"Accept-Encoding: deflate, gzip;q=0.5"}},
		{name: "gzip-x", method: "GET", path: "/", hdr: []string{"Accept-Encoding: x-gzip"}},
		{name: "gzip-via", method: "GET", path: "/", hdr: []string{"Accept-Encoding: gzip", "Via: 1.1 haproxy"}},
		{name: "gzip-http10", method: "GET", path: "/", proto: "HTTP/1.0", hdr: []string{"Accept-Encoding: gzip"}},
		{name: "gzip-css", method: "GET", path: "/assets/index-DRoYlDJr.css", hdr: []string{"Accept-Encoding: gzip"}},
		{name: "head-css", method: "HEAD", path: "/assets/index-DRoYlDJr.css"},
		{name: "head-root", method: "HEAD", path: "/"},
		{name: "inm", method: "GET", path: "/", hdr: []string{"If-None-Match: " + etag}},
		{name: "inm-weak", method: "GET", path: "/", hdr: []string{"If-None-Match: W/" + etag}},
		{name: "inm-weak-gzip", method: "GET", path: "/", hdr: []string{"If-None-Match: W/" + etag, "Accept-Encoding: gzip"}},
		{name: "inm-list", method: "GET", path: "/", hdr: []string{"If-None-Match: \"x\", " + etag}},
		{name: "inm-miss", method: "GET", path: "/", hdr: []string{"If-None-Match: \"nope\""}},
		{name: "inm-star", method: "GET", path: "/", hdr: []string{"If-None-Match: *"}},
		{name: "ims", method: "GET", path: "/", hdr: []string{"If-Modified-Since: " + lm}},
		{name: "ims-later", method: "GET", path: "/", hdr: []string{"If-Modified-Since: Fri, 01 Jan 2100 00:00:00 GMT"}},
		{name: "ims-bad", method: "GET", path: "/", hdr: []string{"If-Modified-Since: garbage"}},
		{name: "ims-inm-miss", method: "GET", path: "/", hdr: []string{"If-Modified-Since: " + lm, "If-None-Match: \"nope\""}},
		{name: "if-match-miss", method: "GET", path: "/", hdr: []string{"If-Match: \"nope\""}},
		{name: "if-match-ok", method: "GET", path: "/", hdr: []string{"If-Match: " + etag}},
		{name: "ius-old", method: "GET", path: "/", hdr: []string{"If-Unmodified-Since: Thu, 01 Jan 1970 00:00:00 GMT"}},
		{name: "range", method: "GET", path: "/assets/index-DRoYlDJr.css", hdr: []string{"Range: bytes=0-9"}},
		{name: "range-suffix", method: "GET", path: "/assets/index-DRoYlDJr.css", hdr: []string{"Range: bytes=-5"}},
		{name: "range-open", method: "GET", path: "/assets/index-DRoYlDJr.css", hdr: []string{"Range: bytes=8000-"}},
		{name: "range-416", method: "GET", path: "/assets/index-DRoYlDJr.css", hdr: []string{"Range: bytes=99999-"}},
		{name: "range-bad", method: "GET", path: "/assets/index-DRoYlDJr.css", hdr: []string{"Range: bytes=abc"}},
		{name: "range-0-", method: "GET", path: "/assets/index-DRoYlDJr.css", hdr: []string{"Range: bytes=0-"}},
		{name: "range-ifrange-miss", method: "GET", path: "/assets/index-DRoYlDJr.css", hdr: []string{"Range: bytes=0-9", "If-Range: \"nope\""}},
		{name: "range-gzip", method: "GET", path: "/", hdr: []string{"Range: bytes=0-9", "Accept-Encoding: gzip"}},
		{name: "range-html", method: "GET", path: "/", hdr: []string{"Range: bytes=0-9"}},
		{name: "post-static", method: "POST", path: "/", body: "x=1"},
		{name: "post-css", method: "POST", path: "/assets/index-DRoYlDJr.css", body: "x=1"},
		{name: "delete-static", method: "DELETE", path: "/x"},
		{name: "options-static", method: "OPTIONS", path: "/"},
		{name: "internal", method: "GET", path: "/internal/mem/dump"},
		{name: "internal-gzip", method: "GET", path: "/internal/mem/dump", hdr: []string{"Accept-Encoding: gzip"}},
		{name: "internal-head", method: "HEAD", path: "/internal/images/x"},
		{name: "internal-post", method: "POST", path: "/internal/images/x", body: "abc"},
		{name: "api-get", method: "GET", path: "/api/users?q=a%20b&x=1", hdr: []string{"X-Client-IP: 10.0.0.1", "Accept-Encoding: gzip", "X-Country-Code: JP", "User-Agent: bench"}},
		{name: "api-spoof", method: "GET", path: "/api/me", hdr: []string{"X-Isureal-Fwd-Region: europe", "X-Isureal-Fwd-Secret: s3cret", "x-isureal-fwd-other: 1", "X_Client_IP: 9.9.9.9", "X.Dot: 1", "X-Forwarded-For: 1.2.3.4", "Host: APP.Example.TEST:8443"}},
		{name: "api-hop", method: "GET", path: "/api/me", hdr: []string{"Keep-Alive: timeout=5", "TE: trailers", "Upgrade: h2c"}},
		{name: "api-head", method: "HEAD", path: "/api/me"},
		{name: "api-post", method: "POST", path: "/api/posts", hdr: []string{"Content-Type: application/json"}, body: `{"a":1}`},
		{name: "api-chunked", method: "POST", path: "/api/posts", hdr: []string{"Content-Type: application/json"}, body: strings.Repeat("b", 3<<20), chunk: true},
		{name: "api-put-image", method: "PUT", path: "/api/me/profile-image", hdr: []string{"Content-Type: image/jpeg"}, body: strings.Repeat("j", 1<<20)},
		{name: "api-32m", method: "POST", path: "/api/posts", body: big32},
		{name: "api-413", method: "POST", path: "/api/posts", noBody: true, cl: 32<<20 + 1},
		{name: "static-413", method: "GET", path: "/", noBody: true, cl: 32<<20 + 1},
		{name: "internal-413", method: "POST", path: "/internal/x", noBody: true, cl: 32<<20 + 1},
		{name: "api-chunked-413", method: "POST", path: "/api/posts", body: big32 + "c", chunk: true},
		{name: "api-delete", method: "DELETE", path: "/api/posts/1"},
		{name: "api-patch", method: "PATCH", path: "/api/me", body: `{"x":1}`},
		{name: "api-big", method: "GET", path: "/api/big"},
		{name: "api-204", method: "POST", path: "/api/nocontent"},
		{name: "api-html", method: "GET", path: "/api/html", hdr: []string{"Accept-Encoding: gzip"}},
		{name: "api-dotdot", method: "GET", path: "/api/x/../users"},
		{name: "api-http10", method: "GET", path: "/api/me", proto: "HTTP/1.0"},
		{name: "memories", method: "GET", path: "/api/memories"},
		{name: "memories-date", method: "GET", path: "/api/memories/2026-10-31"},
		{name: "memories-503", method: "GET", path: "/api/memories/503"},
		{name: "memoriesX", method: "GET", path: "/api/memoriesX"},
		{name: "memories-post", method: "POST", path: "/api/memories", body: "{}"},
		{name: "export", method: "GET", path: "/api/me/export", hdr: []string{"X-Isureal-Fwd-Secret: s3cret"}},
		{name: "export-sub", method: "GET", path: "/api/me/export/x"},
		{name: "images", method: "GET", path: "/api/images/abc"},
		{name: "images-post", method: "POST", path: "/api/images/abc", body: "x"},
		{name: "sse", method: "GET", path: "/api/notifications", sse: true},
		{name: "sse-post", method: "POST", path: "/api/notifications", body: "x"},
		{name: "notificationsX", method: "POST", path: "/api/notificationsfoo", body: "x"},
	}
	// 既知の差分 (意図的、またはアプリから観測できないもの)
	known := map[string]string{
		"api-spoof":     "X-Isureal-Fwd-Other: nginx は Region/Secret だけ消すが Front は X-Isureal-Fwd-* を全部消す (アプリは他を読まない)",
		"api-html":      "upstream の text/html を nginx は gzip する。アプリは text/html を返さないので未実装",
		"api-http10":    "HTTP/1.0 クライアントのとき、ハンドラが見る r.Proto が 1.0 (nginx 経由は 1.1)。アプリは Proto を読まない",
		"memories-post": "本文付きの /api/memories (GET 専用ルート) は asia2 でなくローカルへ。どちらも同じアプリが 405 を返すので外からは同一",
	}
	fails, knownHits := 0, 0
	for _, sc := range scenarios {
		dir := t.TempDir()
		ng := startNginx(t, bin, dir, sc, local)
		f, err := NewFront(diffStub("local"), FrontConfig{
			Public: filepath.Join(repoRoot(), "webapp/public"), Region: sc.region, Secret: "s3cret",
			Fwd: sc.fwd, FwdGet: sc.fwdGet, FwdTarget: sc.fwdTarget, HeavyTarget: sc.heavy,
		})
		if err != nil {
			t.Fatal(err)
		}
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		srv := f.Server()
		go srv.Serve(ln)
		fr := ln.Addr().String()
		for _, rq := range reqs {
			raw := rq.raw()
			a := doRaw(ng, rq.method, raw, rq.sse)
			b := doRaw(fr, rq.method, raw, rq.sse)
			var diffs []string
			if a.err != b.err {
				diffs = append(diffs, fmt.Sprintf("err nginx=%q front=%q", a.err, b.err))
			}
			if a.status != b.status {
				diffs = append(diffs, fmt.Sprintf("status nginx=%d front=%d", a.status, b.status))
			}
			if strings.Join(a.te, ",") != strings.Join(b.te, ",") {
				diffs = append(diffs, fmt.Sprintf("transfer-encoding nginx=%v front=%v", a.te, b.te))
			}
			if a.cl != b.cl {
				diffs = append(diffs, fmt.Sprintf("content-length nginx=%d front=%d", a.cl, b.cl))
			}
			ha, hb := normHeader(a.header), normHeader(b.header)
			if strings.Join(ha, "\n") != strings.Join(hb, "\n") {
				diffs = append(diffs, fmt.Sprintf("headers\n  nginx=%q\n  front=%q", ha, hb))
			}
			if !bytes.Equal(a.body, b.body) {
				diffs = append(diffs, fmt.Sprintf("body\n  nginx=%.600q\n  front=%.600q", a.body, b.body))
			}
			if rq.sse && (a.firstGap > 200*time.Millisecond || b.firstGap > 200*time.Millisecond) {
				diffs = append(diffs, fmt.Sprintf("sse first event late nginx=%v front=%v", a.firstGap, b.firstGap))
			}
			key := sc.name + "/" + rq.name
			if len(diffs) > 0 && known[rq.name] == "" {
				fails++
				t.Errorf("%s: %s", key, strings.Join(diffs, "; "))
			} else if len(diffs) > 0 {
				knownHits++
				t.Logf("%s: known diff (%s)", key, known[rq.name])
			} else if testing.Verbose() {
				t.Logf("%s: ok status=%d", key, a.status)
			}
		}
		srv.Close()
	}
	t.Logf("%d scenarios x %d requests: %d unexpected diffs, %d known", len(scenarios), len(reqs), fails, knownHits)
}

var locPort = regexp.MustCompile(`:[0-9]+/`)
