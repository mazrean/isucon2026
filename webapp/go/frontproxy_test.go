package isureal

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

func newTestFront(t *testing.T, local http.Handler, logPath string) string {
	t.Helper()
	f, err := NewFront(local, FrontConfig{Public: filepath.Join(repoRoot(), "webapp/public"), Region: "asia", LogPath: logPath})
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := f.Server()
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

// echo のハンドラ (notification_api.go と同じ ResponseController.Flush) が、
// アクセスログのラッパー越しでも逐次届くこと、ヘッダ偽装が消えること、RemoteAddr が loopback になること。
func TestFrontEchoSSEAndLog(t *testing.T) {
	e := echo.New()
	e.GET("/api/notifications", func(c *echo.Context) error {
		res := c.Response()
		res.Header().Set("Content-Type", "text/event-stream")
		res.WriteHeader(200)
		io.WriteString(res, "data: 1\n\n")
		if err := http.NewResponseController(res).Flush(); err != nil {
			return err
		}
		time.Sleep(300 * time.Millisecond)
		io.WriteString(res, "data: 2\n\n")
		return nil
	})
	e.GET("/api/who", func(c *echo.Context) error {
		r := c.Request()
		return c.String(200, r.RemoteAddr+"|"+r.Header.Get(forwardedSecretHeader)+"|"+r.Header.Get("X-Client-IP"))
	})
	logPath := filepath.Join(t.TempDir(), "access.log")
	addr := newTestFront(t, e, logPath)

	start := time.Now()
	res, err := http.Get("http://" + addr + "/api/notifications")
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(res.Body)
	line, _ := br.ReadString('\n')
	if line != "data: 1\n" || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("first SSE event %q after %v", line, time.Since(start))
	}
	io.ReadAll(br)
	res.Body.Close()

	req, _ := http.NewRequest("GET", "http://"+addr+"/api/who", nil)
	req.Header.Set(forwardedSecretHeader, "spoof")
	req.Header.Set("X-Client-IP", "10.1.2.3")
	req.Header.Set("X-Country-Code", "JP")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(b) != frontLocalAddr+"||10.1.2.3" {
		t.Fatalf("who = %q", b)
	}
	if _, err := http.Get("http://" + addr + "/assets/index-DRoYlDJr.css"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5500 * time.Millisecond) // flush=5s
	data, _ := os.ReadFile(logPath)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	re := regexp.MustCompile(`^127\.0\.0\.1 \[\d\d/\w{3}/\d{4}:\d\d:\d\d:\d\d [+-]\d{4}\] "GET (\S+) HTTP/1\.1" (\d+) (\d+) \d+\.\d{3} (\S+) "([^"]*)" "([^"]*)"$`)
	if len(lines) != 3 {
		t.Fatalf("log lines = %q", lines)
	}
	for i, want := range [][]string{
		{"/api/notifications", "200", "18", "", "gzip", "-"},
		{"/api/who", "200", "", "", "gzip", "JP"},
		{"/assets/index-DRoYlDJr.css", "200", "8572", "-", "gzip", "-"},
	} {
		m := re.FindStringSubmatch(lines[i])
		if m == nil || m[1] != want[0] || m[2] != want[1] || (want[2] != "" && m[3] != want[2]) ||
			(want[3] == "-") != (m[4] == "-") || m[5] != want[4] || m[6] != want[5] {
			t.Errorf("log line %d = %q", i, lines[i])
		}
	}
	t.Logf("%s", data)
}

func TestNginxURI(t *testing.T) {
	for in, want := range map[string]string{
		"/":                 "/",
		"//a//b":            "/a/b",
		"/a/./b/":           "/a/b/",
		"/a/b/..":           "/a/",
		"/a/b/../c":         "/a/c",
		"/a/.":              "/a/",
		"/api/x/../users":   "/api/users",
		"/..":               "",
		"/a/../../b":        "",
		"/assets/.hidden/x": "/assets/.hidden/x",
	} {
		got, ok := nginxURI(in)
		if want == "" {
			if ok {
				t.Errorf("%q: want 400, got %q", in, got)
			}
			continue
		}
		if !ok || got != want {
			t.Errorf("%q: got %q %v want %q", in, got, ok, want)
		}
	}
}
