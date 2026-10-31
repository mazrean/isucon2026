package isureal

// frontproxy.go: ISUREAL_FRONT=1 のとき、nginx (etc/app/isureal.conf) の代わりに Go が :80 を受ける。
// ローカルの API はプロセス内でハンドラへ直接渡し (HTTP の往復なし)、転送だけ ReverseProxy を使う。
// 振り分け・静的配信・エラーページ・gzip は nginx 1.24.0 (Ubuntu) の挙動に合わせてある
// (frontproxy_diff_test.go で実物の nginx と比較)。

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frontMaxBody     = 32 << 20 // client_max_body_size 32m
	nginxServerToken = "nginx/1.24.0 (Ubuntu)"
	frontLocalAddr   = "127.0.0.1:1" // nginx 経由のときアプリが見ていた RemoteAddr (loopback)
)

type FrontConfig struct {
	Addr        string // ":80"
	Public      string // /home/isucon/webapp/public
	Region      string // X-Isureal-Fwd-Region に入れる自ホストの地域
	Secret      string // X-Isureal-Fwd-Secret
	Fwd         bool   // 非 GET/HEAD の /api/ を FwdTarget へ (europe1/america1)
	FwdGet      bool   // GET/HEAD も FwdTarget へ
	FwdTarget   string // "192.168.1.10:8080"
	HeavyTarget string // /api/memories, /api/me/export の転送先 (asia1 → "192.168.4.10:8080")。空ならローカル
	LogPath     string // nginx の timed 形式の access log。空なら出さない
}

func frontConfigFromEnv(region string) FrontConfig {
	return FrontConfig{
		Addr:        envOrDefault("ISUREAL_FRONT_ADDR", ":80"),
		Public:      envOrDefault("ISUREAL_FRONT_PUBLIC", "/home/isucon/webapp/public"),
		Region:      envOrDefault("ISUREAL_FRONT_REGION", region),
		Secret:      forwardSecret,
		Fwd:         os.Getenv("ISUREAL_FRONT_FWD") == "1",
		FwdGet:      os.Getenv("ISUREAL_FRONT_FWD_GET") == "1",
		FwdTarget:   envOrDefault("ISUREAL_FRONT_FWD_TARGET", "192.168.1.10:8080"),
		HeavyTarget: os.Getenv("ISUREAL_FRONT_HEAVY_TARGET"),
		LogPath:     os.Getenv("ISUREAL_FRONT_LOG"),
	}
}

// StartFrontFromEnv は ISUREAL_FRONT=1 のときだけ :80 の待ち受けを始める。
// :80 が使用中 (nginx が動いている) の間は 200ms ごとに再試行するので、
// アプリを先に再起動してから nginx を止めれば切り替えの空白は 200ms 程度。
func StartFrontFromEnv(local http.Handler, region string) {
	if os.Getenv("ISUREAL_FRONT") != "1" {
		return
	}
	f, err := NewFront(local, frontConfigFromEnv(region))
	if err != nil {
		panic("front: " + err.Error())
	}
	srv := f.Server()
	go func() {
		warned := false
		for {
			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				if !warned {
					log.Printf("front: listen %s: %v (retrying)", srv.Addr, err)
					warned = true
				}
				time.Sleep(200 * time.Millisecond)
				continue
			}
			log.Printf("front: serving on %s", srv.Addr)
			if err := srv.Serve(ln); err != nil {
				log.Printf("front: serve: %v", err)
			}
			return
		}
	}()
}

type Front struct {
	cfg    FrontConfig
	local  http.Handler
	static map[string]*staticFile
	index  *staticFile
	remote *httputil.ReverseProxy
	heavy  *httputil.ReverseProxy

	heavyMu    sync.Mutex
	heavyFails int
	heavyFirst time.Time
	heavyDown  atomic.Int64 // unix nano; この時刻まで HeavyTarget を使わない

	alog *accessLog
}

func NewFront(local http.Handler, cfg FrontConfig) (*Front, error) {
	f := &Front{cfg: cfg, local: local, static: map[string]*staticFile{}}
	if err := f.loadStatic(cfg.Public); err != nil {
		return nil, err
	}
	if f.index = f.static["/index.html"]; f.index == nil {
		return nil, errors.New("index.html not found in " + cfg.Public)
	}
	if cfg.Fwd || cfg.FwdGet {
		f.remote = f.newProxy(cfg.FwdTarget, true, 256, false)
	}
	if cfg.HeavyTarget != "" && cfg.HeavyTarget != "127.0.0.1:8080" {
		f.heavy = f.newProxy(cfg.HeavyTarget, false, 64, true)
	}
	if cfg.LogPath != "" {
		al, err := openAccessLog(cfg.LogPath)
		if err != nil {
			return nil, err
		}
		f.alog = al
	}
	return f, nil
}

func (f *Front) Server() *http.Server {
	return &http.Server{
		Addr:              f.cfg.Addr,
		Handler:           f,
		ReadHeaderTimeout: 60 * time.Second, // client_header_timeout
		IdleTimeout:       75 * time.Second, // keepalive_timeout
		ErrorLog:          frontErrLog,
	}
}

// ---------------- dispatch ----------------

const (
	upNone   = 0 // nginx が自分で返した (静的・エラーページ)
	upLocal  = 1
	upRemote = 2
)

func (f *Front) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.alog == nil {
		f.serve(w, r)
		return
	}
	start := time.Now()
	lw := &logWriter{ResponseWriter: w}
	remoteAddr, line := r.RemoteAddr, r.Method+" "+r.RequestURI+" "+r.Proto
	ae, cc := r.Header.Get("Accept-Encoding"), r.Header.Get("X-Country-Code")
	up := f.serve(lw, r)
	f.alog.write(remoteAddr, start, line, lw, up, ae, cc)
}

func (f *Front) serve(w http.ResponseWriter, r *http.Request) int {
	sanitizeRequestHeaders(r.Header)
	p, ok := nginxURI(r.URL.Path)
	if !ok {
		writeNginxError(w, r, http.StatusBadRequest)
		return upNone
	}
	if r.ContentLength > frontMaxBody {
		writeNginxError(w, r, http.StatusRequestEntityTooLarge)
		return upNone
	}
	r.Host = hostOnly(r.Host)
	switch {
	case p == "/api" || p == "/api/images":
		// proxy_pass の location /api/ と /api/images/ は末尾スラッシュなしを 301 にする (auto_redirect)
		writeNginxRedirect(w, r, p+"/")
		return upNone
	case strings.HasPrefix(p, "/api/notifications"):
		return f.serveLocal(w, r)
	case strings.HasPrefix(p, "/internal/"):
		writeNginxError(w, r, http.StatusNotFound)
		return upNone
	case isHeavyPath(p):
		if !f.bufferChunked(w, r) {
			return upNone
		}
		if f.remote != nil && f.fwdMethod(r.Method) {
			f.remote.ServeHTTP(w, r)
			return upRemote
		}
		if f.heavy != nil && r.ContentLength == 0 && time.Now().UnixNano() >= f.heavyDown.Load() {
			f.heavy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), frontInReqKey{}, r)))
			return upRemote
		}
		return f.serveLocal(w, r)
	case strings.HasPrefix(p, "/api/images/"):
		if !f.bufferChunked(w, r) {
			return upNone
		}
		return f.serveLocal(w, r)
	case strings.HasPrefix(p, "/api/"):
		if !f.bufferChunked(w, r) {
			return upNone
		}
		if f.remote != nil && f.fwdMethod(r.Method) {
			f.remote.ServeHTTP(w, r)
			return upRemote
		}
		return f.serveLocal(w, r)
	default:
		f.serveStatic(w, r, p)
		return upNone
	}
}

func (f *Front) fwdMethod(m string) bool {
	if m == http.MethodGet || m == http.MethodHead {
		return f.cfg.FwdGet
	}
	return f.cfg.Fwd
}

func (f *Front) serveLocal(w http.ResponseWriter, r *http.Request) int {
	r.RemoteAddr = frontLocalAddr
	// nginx は Connection "" と、既定の proxy_set_header で TE/Keep-Alive/Expect/Upgrade を消して渡す。
	// (keep-alive の判定は net/http がハンドラ呼び出し前に済ませているので消してよい)
	h := r.Header
	delete(h, "Connection")
	delete(h, "Keep-Alive")
	delete(h, "Te")
	delete(h, "Expect")
	delete(h, "Upgrade")
	f.local.ServeHTTP(w, r)
	return upLocal
}

func isHeavyPath(p string) bool { // location ~ ^/api/(memories|me/export)(/|$)
	for _, pre := range [...]string{"/api/memories", "/api/me/export"} {
		if strings.HasPrefix(p, pre) && (len(p) == len(pre) || p[len(pre)] == '/') {
			return true
		}
	}
	return false
}

// bufferChunked: nginx は本文を全部受けてから Content-Length 付きで upstream に渡す。
// 長さ不明 (chunked) の本文だけ読み切り、上限を超えたら 413。
func (f *Front) bufferChunked(w http.ResponseWriter, r *http.Request) bool {
	if r.ContentLength >= 0 || r.Body == nil || r.Body == http.NoBody {
		return true
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, frontMaxBody+1))
	if err != nil {
		writeNginxError(w, r, http.StatusBadRequest)
		return false
	}
	if len(body) > frontMaxBody {
		writeNginxError(w, r, http.StatusRequestEntityTooLarge)
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.TransferEncoding = nil
	r.Header["Content-Length"] = []string{strconv.Itoa(len(body))}
	return true
}

// sanitizeRequestHeaders: X-Isureal-Fwd-* は外部から来たら必ず消す (偽装防止)。
// nginx の ignore_invalid_headers / underscores_in_headers off に合わせ、
// 英数字と '-' 以外を含む名前のヘッダも落とす。
func sanitizeRequestHeaders(h http.Header) {
	for k := range h {
		if len(k) > 14 && (k[0] == 'X' || k[0] == 'x') && strings.EqualFold(k[:14], "X-Isureal-Fwd-") {
			delete(h, k)
			continue
		}
		for i := 0; i < len(k); i++ {
			c := k[i]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				delete(h, k)
				break
			}
		}
	}
}

func hostOnly(h string) string { // nginx の $host: ポートを外し小文字
	if i := strings.LastIndexByte(h, ':'); i >= 0 && !strings.Contains(h[i:], "]") {
		h = h[:i]
	}
	for i := 0; i < len(h); i++ {
		if c := h[i]; c >= 'A' && c <= 'Z' {
			return strings.ToLower(h)
		}
	}
	return h
}

// nginxURI は nginx の $uri と同じ正規化 (スラッシュの併合、. と .. の解決) を行う。
// ルートより上に出る .. は 400。
func nginxURI(p string) (string, bool) {
	if p == "" || p[0] != '/' {
		return p, false
	}
	if !strings.Contains(p, "//") && !strings.Contains(p, "/.") {
		return p, true
	}
	segs := make([]string, 0, 8)
	parts := strings.Split(p[1:], "/")
	for i, s := range parts {
		last := i == len(parts)-1
		switch s {
		case "":
			if last {
				segs = append(segs, "")
			}
		case ".":
			if last {
				segs = append(segs, "")
			}
		case "..":
			if len(segs) > 0 && segs[len(segs)-1] == "" {
				segs = segs[:len(segs)-1]
			}
			if len(segs) == 0 {
				return p, false
			}
			segs = segs[:len(segs)-1]
			if last {
				segs = append(segs, "")
			}
		default:
			segs = append(segs, s)
		}
	}
	return "/" + strings.Join(segs, "/"), true
}

// ---------------- reverse proxy ----------------

var errUpstreamBad = errors.New("upstream 502/503")

func (f *Front) newProxy(target string, setFwd bool, idle int, heavy bool) *httputil.ReverseProxy {
	tr := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          idle * 2,
		MaxIdleConnsPerHost:   idle,
		IdleConnTimeout:       300 * time.Second,
		DisableCompression:    true,
		ResponseHeaderTimeout: 60 * time.Second, // proxy_read_timeout
		ForceAttemptHTTP2:     false,
		WriteBufferSize:       32 << 10,
		ReadBufferSize:        32 << 10,
	}
	region, secret := f.cfg.Region, f.cfg.Secret
	rp := &httputil.ReverseProxy{
		Transport:  tr,
		BufferPool: proxyBufPool{},
		ErrorLog:   log.New(io.Discard, "", 0),
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = target
			pr.Out.Host = pr.In.Host
			// Rewrite は X-Forwarded-* / Forwarded を消すが、nginx はそのまま渡していた。
			for _, k := range [...]string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "Forwarded"} {
				if v, ok := pr.In.Header[k]; ok {
					pr.Out.Header[k] = v
				}
			}
			delete(pr.Out.Header, "Te") // nginx は TE "" で消す
			if setFwd {
				pr.Out.Header.Set(forwardedRegionHeader, region)
				pr.Out.Header.Set(forwardedSecretHeader, secret)
			}
		},
	}
	if heavy {
		// upstream app_heavy { server HEAVY max_fails=3 fail_timeout=5s; server 127.0.0.1:8080 backup; }
		// proxy_next_upstream error timeout http_502 http_503;
		rp.ModifyResponse = func(res *http.Response) error {
			if res.StatusCode == http.StatusBadGateway || res.StatusCode == http.StatusServiceUnavailable {
				return errUpstreamBad
			}
			f.heavyOK()
			return nil
		}
		rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				return
			}
			f.heavyFail()
			if in, ok := r.Context().Value(frontInReqKey{}).(*http.Request); ok {
				f.serveLocal(w, in)
			}
		}
	} else {
		rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				writeNginxError(w, r, http.StatusGatewayTimeout)
				return
			}
			writeNginxError(w, r, http.StatusBadGateway)
		}
	}
	return rp
}

func (f *Front) heavyOK() {
	f.heavyMu.Lock()
	f.heavyFails = 0
	f.heavyMu.Unlock()
}

func (f *Front) heavyFail() {
	now := time.Now()
	f.heavyMu.Lock()
	defer f.heavyMu.Unlock()
	if f.heavyFails == 0 || now.Sub(f.heavyFirst) > 5*time.Second {
		f.heavyFails, f.heavyFirst = 0, now
	}
	f.heavyFails++
	if f.heavyFails >= 3 {
		f.heavyFails = 0
		f.heavyDown.Store(now.Add(5 * time.Second).UnixNano())
	}
}

var proxyBufs = sync.Pool{New: func() any { b := make([]byte, 32<<10); return &b }}

type proxyBufPool struct{}

func (proxyBufPool) Get() []byte  { return *(proxyBufs.Get().(*[]byte)) }
func (proxyBufPool) Put(b []byte) { proxyBufs.Put(&b) }

// ---------------- static files ----------------

type staticFile struct {
	data         []byte
	gz           []byte // gzip_types text/html のときだけ
	contentType  string
	etag         string // "mtime-size" (hex)
	weakETag     string
	lastModified string
	mtime        int64
	size         string
}

func (f *Front) loadStatic(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		st, err := os.Stat(path) // シンボリックリンクは nginx 同様にたどる
		if err != nil || !st.Mode().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		uri := "/" + filepath.ToSlash(rel)
		mt := st.ModTime().Unix()
		sf := &staticFile{
			data:         data,
			contentType:  nginxContentType(uri),
			etag:         fmt.Sprintf("\"%x-%x\"", mt, st.Size()),
			lastModified: time.Unix(mt, 0).UTC().Format(http.TimeFormat),
			mtime:        mt,
			size:         strconv.Itoa(len(data)),
		}
		sf.weakETag = "W/" + sf.etag
		if sf.contentType == "text/html" && len(data) >= 20 {
			sf.gz = nginxGzip(data)
		}
		f.static[uri] = sf
		return nil
	})
}

func nginxContentType(uri string) string {
	for i := len(uri) - 1; i >= 0 && uri[i] != '/'; i-- {
		if uri[i] == '.' {
			if t, ok := nginxMimeTypes[strings.ToLower(uri[i+1:])]; ok {
				return t
			}
			break
		}
	}
	return "application/octet-stream"
}

func (f *Front) serveStatic(w http.ResponseWriter, r *http.Request, p string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeNginxError(w, r, http.StatusMethodNotAllowed)
		return
	}
	sf := f.static[p]
	if sf == nil { // try_files $uri /index.html
		sf = f.index
	}
	h := w.Header()
	// ngx_http_not_modified_filter (gzip より前に強い ETag で判定する)
	if v := r.Header.Get("If-Unmodified-Since"); v != "" {
		if t, err := http.ParseTime(v); err != nil || t.Unix() < sf.mtime {
			writeNginxError(w, r, http.StatusPreconditionFailed)
			return
		}
	}
	if v := r.Header.Get("If-Match"); v != "" && !nginxETagMatch(v, sf.etag, false) {
		writeNginxError(w, r, http.StatusPreconditionFailed)
		return
	}
	ims, inm := r.Header.Get("If-Modified-Since"), r.Header.Get("If-None-Match")
	if ims != "" || inm != "" {
		modified := false
		if ims != "" {
			t, err := http.ParseTime(ims)
			modified = err != nil || t.Unix() != sf.mtime
		}
		if !modified && inm != "" && !nginxETagMatch(inm, sf.etag, true) {
			modified = true
		}
		if !modified {
			h["Last-Modified"] = []string{sf.lastModified}
			h["ETag"] = []string{sf.etag}
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	h["Content-Type"] = []string{sf.contentType}
	h["Last-Modified"] = []string{sf.lastModified}
	if sf.gz != nil && nginxGzipOK(r) {
		h["ETag"] = []string{sf.weakETag}
		h["Content-Encoding"] = []string{"gzip"}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		h["Transfer-Encoding"] = []string{"chunked"}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(sf.gz)
		return
	}
	h["ETag"] = []string{sf.etag}
	if rg := r.Header.Get("Range"); len(rg) >= 7 && strings.EqualFold(rg[:6], "bytes=") && ifRangeOK(r, sf) {
		start, end, st := nginxRange(rg[6:], int64(len(sf.data)))
		switch st {
		case http.StatusPartialContent:
			h["Content-Range"] = []string{fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(sf.data))}
			h["Content-Length"] = []string{strconv.FormatInt(end-start, 10)}
			w.WriteHeader(http.StatusPartialContent)
			if r.Method != http.MethodHead {
				_, _ = w.Write(sf.data[start:end])
			}
			return
		case http.StatusRequestedRangeNotSatisfiable:
			delete(h, "Last-Modified")
			delete(h, "ETag")
			h["Content-Range"] = []string{"bytes */" + sf.size}
			writeNginxError(w, r, http.StatusRequestedRangeNotSatisfiable)
			return
		}
	}
	h["Content-Length"] = []string{sf.size}
	h["Accept-Ranges"] = []string{"bytes"}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(sf.data)
	}
}

func ifRangeOK(r *http.Request, sf *staticFile) bool {
	v := r.Header.Get("If-Range")
	if v == "" {
		return true
	}
	if len(v) >= 2 && v[len(v)-1] == '"' {
		return v == sf.etag
	}
	t, err := http.ParseTime(v)
	return err == nil && t.Unix() == sf.mtime
}

// nginxRange は ngx_http_range_parse の移植。200 = Range を無視して全体を返す。
// 複数範囲 (multipart/byteranges) は未対応で 200 にする。
func nginxRange(p string, size int64) (int64, int64, int) {
	type rng struct{ s, e int64 }
	var ranges []rng
	var total int64
	i := 0
	digit := func() bool { return i < len(p) && p[i] >= '0' && p[i] <= '9' }
	skipSP := func() {
		for i < len(p) && p[i] == ' ' {
			i++
		}
	}
	for {
		var start, end int64
		suffix := false
		skipSP()
		if i >= len(p) || p[i] != '-' {
			if !digit() {
				return 0, 0, http.StatusRequestedRangeNotSatisfiable
			}
			for digit() {
				if start >= (1<<62)/10 {
					return 0, 0, http.StatusRequestedRangeNotSatisfiable
				}
				start = start*10 + int64(p[i]-'0')
				i++
			}
			skipSP()
			if i >= len(p) || p[i] != '-' {
				return 0, 0, http.StatusRequestedRangeNotSatisfiable
			}
			i++
			skipSP()
			if i >= len(p) || p[i] == ',' {
				end = size
				goto found
			}
		} else {
			suffix = true
			i++
		}
		if !digit() {
			return 0, 0, http.StatusRequestedRangeNotSatisfiable
		}
		for digit() {
			if end >= (1<<62)/10 {
				return 0, 0, http.StatusRequestedRangeNotSatisfiable
			}
			end = end*10 + int64(p[i]-'0')
			i++
		}
		skipSP()
		if i < len(p) && p[i] != ',' {
			return 0, 0, http.StatusRequestedRangeNotSatisfiable
		}
		if suffix {
			if end < size {
				start = size - end
			} else {
				start = 0
			}
			end = size - 1
		}
		if end >= size {
			end = size
		} else {
			end++
		}
	found:
		if start < end {
			ranges = append(ranges, rng{start, end})
			total += end - start
		} else if start == 0 {
			return 0, 0, http.StatusOK
		}
		if i >= len(p) || p[i] != ',' {
			break
		}
		i++
	}
	if len(ranges) == 0 {
		return 0, 0, http.StatusRequestedRangeNotSatisfiable
	}
	if total > size || len(ranges) > 1 {
		return 0, 0, http.StatusOK
	}
	return ranges[0].s, ranges[0].e, http.StatusPartialContent
}

// nginxETagMatch は ngx_http_test_if_match の移植。
func nginxETagMatch(list, etag string, weak bool) bool {
	if list == "*" {
		return true
	}
	if !weak && strings.HasPrefix(etag, "W/") {
		return false
	}
	if weak {
		etag = strings.TrimPrefix(etag, "W/")
	}
	s := list
	for len(s) > 0 {
		if weak && len(s) > 2 && s[0] == 'W' && s[1] == '/' {
			s = s[2:]
		}
		if len(s) < len(etag) {
			return false
		}
		if s[:len(etag)] == etag {
			rest := s[len(etag):]
			if rest == "" || rest[0] == ' ' || rest[0] == ',' {
				return true
			}
		}
		j := strings.IndexByte(s, ',')
		if j < 0 {
			return false
		}
		s = strings.TrimLeft(s[j+1:], " ")
	}
	return false
}

// nginxGzipOK は ngx_http_gzip_ok (gzip_http_version 1.1, gzip_proxied off) と
// ngx_http_gzip_accept_encoding の移植。
func nginxGzipOK(r *http.Request) bool {
	if !r.ProtoAtLeast(1, 1) {
		return false
	}
	if _, ok := r.Header["Via"]; ok {
		return false
	}
	ae := r.Header.Get("Accept-Encoding")
	start := 0
	for {
		j := indexFold(ae[start:], "gzip")
		if j < 0 {
			return false
		}
		pos := start + j
		if pos == 0 || ae[pos-1] == ',' || ae[pos-1] == ' ' {
			return gzipQuantity(ae[pos+4:]) != 0
		}
		start = pos + 4
	}
}

func indexFold(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if strings.EqualFold(s[i:i+len(sub)], sub) {
			return i
		}
	}
	return -1
}

func gzipQuantity(p string) int {
	i := 0
	for i < len(p) {
		c := p[i]
		i++
		switch c {
		case ',':
			return 1
		case ';':
			goto quantity
		case ' ':
			continue
		default:
			return 0
		}
	}
	return 1
quantity:
	for i < len(p) {
		c := p[i]
		i++
		switch c {
		case 'q', 'Q':
			goto equal
		case ' ':
			continue
		default:
			return 0
		}
	}
	return 1
equal:
	if i+2 > len(p) || p[i] != '=' {
		return 0
	}
	i++
	c := p[i]
	i++
	if c == '1' {
		if i == len(p) || p[i] == ',' || p[i] == ' ' {
			return 1
		}
		return 0
	}
	if c != '0' {
		return 0
	}
	if i == len(p) {
		return 0
	}
	if p[i] != '.' {
		return 0
	}
	i++
	q, n := 0, 0
	for i < len(p) {
		c := p[i]
		i++
		if c == ',' || c == ' ' {
			break
		}
		if c >= '0' && c <= '9' {
			n++
			q = q*10 + int(c-'0')
			continue
		}
		return 0
	}
	if n > 3 {
		return 0
	}
	return q
}

// nginxGzip は nginx の gzip 出力と同じヘッダ (mtime 0, OS=unix) で level 1 圧縮する。
// 圧縮後のバイト列は zlib と Go の flate で異なるが、展開結果は同一。
func nginxGzip(data []byte) []byte {
	var b bytes.Buffer
	b.Write([]byte{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 3})
	fw, _ := flate.NewWriter(&b, 1)
	_, _ = fw.Write(data)
	_ = fw.Close()
	var tail [8]byte
	binary.LittleEndian.PutUint32(tail[:4], crc32.ChecksumIEEE(data))
	binary.LittleEndian.PutUint32(tail[4:], uint32(len(data)))
	b.Write(tail[:])
	return b.Bytes()
}

// ---------------- nginx error pages ----------------

var nginxErrorBodies sync.Map // int -> []byte

func nginxErrorBody(status int) []byte {
	if b, ok := nginxErrorBodies.Load(status); ok {
		return b.([]byte)
	}
	title := strconv.Itoa(status) + " " + nginxStatusText(status)
	b := []byte("<html>\r\n<head><title>" + title + "</title></head>\r\n<body>\r\n<center><h1>" + title +
		"</h1></center>\r\n<hr><center>" + nginxServerToken + "</center>\r\n</body>\r\n</html>\r\n")
	nginxErrorBodies.Store(status, b)
	return b
}

func nginxStatusText(status int) string {
	switch status {
	case http.StatusMethodNotAllowed:
		return "Not Allowed"
	case http.StatusRequestEntityTooLarge:
		return "Request Entity Too Large"
	case http.StatusGatewayTimeout:
		return "Gateway Time-out"
	}
	return http.StatusText(status)
}

// writeNginxRedirect: Location は "http://" + $host + (80 以外なら :port) + uri + 引数。
func writeNginxRedirect(w http.ResponseWriter, r *http.Request, uri string) {
	loc := "http://" + r.Host
	if la, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
		if _, port, err := net.SplitHostPort(la.String()); err == nil && port != "80" {
			loc += ":" + port
		}
	}
	loc += uri
	if r.URL.RawQuery != "" {
		loc += "?" + r.URL.RawQuery
	}
	w.Header()["Location"] = []string{loc}
	writeNginxError(w, r, http.StatusMovedPermanently)
}

var gzip404 atomic.Pointer[[]byte]

func writeNginxError(w http.ResponseWriter, r *http.Request, status int) {
	body := nginxErrorBody(status)
	h := w.Header()
	h["Content-Type"] = []string{"text/html"}
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
		h["Connection"] = []string{"close"}
	}
	if status == http.StatusNotFound && nginxGzipOK(r) { // gzip は 200/403/404 だけ
		gz := gzip404.Load()
		if gz == nil {
			b := nginxGzip(body)
			gz = &b
			gzip404.Store(gz)
		}
		h["Content-Encoding"] = []string{"gzip"}
		if r.Method == http.MethodHead {
			w.WriteHeader(status)
			return
		}
		h["Transfer-Encoding"] = []string{"chunked"}
		w.WriteHeader(status)
		_, _ = w.Write(*gz)
		return
	}
	h["Content-Length"] = []string{strconv.Itoa(len(body))}
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// ---------------- access log (log_format timed) ----------------

type logWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (l *logWriter) WriteHeader(code int) {
	if l.status == 0 {
		l.status = code
	}
	l.ResponseWriter.WriteHeader(code)
}

func (l *logWriter) Write(b []byte) (int, error) {
	if l.status == 0 {
		l.status = http.StatusOK
	}
	n, err := l.ResponseWriter.Write(b)
	l.bytes += int64(n)
	return n, err
}

func (l *logWriter) Flush() {
	if fl, ok := l.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func (l *logWriter) Unwrap() http.ResponseWriter { return l.ResponseWriter }

type accessLog struct {
	mu sync.Mutex
	w  *bufio.Writer
	f  *os.File
}

func openAccessLog(path string) (*accessLog, error) {
	fh, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &accessLog{w: bufio.NewWriterSize(fh, 64<<10), f: fh}
	go func() { // buffer=64k flush=5s
		for range time.Tick(5 * time.Second) {
			a.mu.Lock()
			_ = a.w.Flush()
			a.mu.Unlock()
		}
	}()
	return a, nil
}

func (a *accessLog) write(remoteAddr string, start time.Time, line string, lw *logWriter, up int, ae, cc string) {
	now := time.Now()
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	status := lw.status
	if status == 0 {
		status = http.StatusOK
	}
	rt := fmt.Sprintf("%.3f", now.Sub(start).Seconds())
	urt := "-"
	if up != upNone {
		urt = rt
	}
	a.mu.Lock()
	fmt.Fprintf(a.w, "%s [%s] \"%s\" %d %d %s %s \"%s\" \"%s\"\n", host, start.Format("02/Jan/2006:15:04:05 -0700"),
		logEscape(line), status, lw.bytes, rt, urt, logEscape(orDash(ae)), logEscape(orDash(cc)))
	a.mu.Unlock()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func logEscape(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c >= 0x7f || c == '"' || c == '\\' {
			var b strings.Builder
			for j := 0; j < len(s); j++ {
				c := s[j]
				if c < 0x20 || c >= 0x7f || c == '"' || c == '\\' {
					fmt.Fprintf(&b, "\\x%02X", c)
				} else {
					b.WriteByte(c)
				}
			}
			return b.String()
		}
	}
	return s
}

var frontErrLog = func() *log.Logger {
	if os.Getenv("ISUREAL_FRONT_DEBUG") == "1" {
		return log.New(os.Stderr, "front: ", 0)
	}
	return log.New(io.Discard, "", 0)
}()

type frontInReqKey struct{}
