package jpegcheck

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Differential test of webapp/rust/jpegcheckd against Go's image/jpeg over
// the same inputs as TestEquivalence. Set JPEGCHECK_RUST_BIN to the binary.

func startRust(tb testing.TB) *http.Client {
	bin := os.Getenv("JPEGCHECK_RUST_BIN")
	if bin == "" {
		tb.Skip("JPEGCHECK_RUST_BIN not set")
	}
	sock := fmt.Sprintf("@jpegcheckd-test-%d", os.Getpid())
	cmd := exec.Command(bin, "--sock", sock)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	for i := 0; ; i++ {
		c, err := net.Dial("unix", sock)
		if err == nil {
			c.Close()
			break
		}
		if i > 200 {
			tb.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
		MaxIdleConnsPerHost: 8,
	}}
}

func rustCheck(tb testing.TB, c *http.Client, data []byte) (int, int, bool) {
	res, err := c.Post("http://jpegcheckd/internal/jpegcheck", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		tb.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		tb.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		tb.Fatalf("status %d", res.StatusCode)
	}
	var ok, w, h int
	if _, err := fmt.Sscanf(string(body), "%d %d %d", &ok, &w, &h); err != nil {
		tb.Fatal(err)
	}
	return w, h, ok == 1
}

func TestRustEquivalence(t *testing.T) {
	dir := os.Getenv("JPEGCHECK_CORPUS")
	if dir == "" {
		t.Skip("JPEGCHECK_CORPUS not set")
	}
	c := startRust(t)
	cases, okCount, mismatch := 0, 0, 0
	nfiles := forEachCase(t, dir, func(name string, data []byte) {
		cases++
		rw, rh, rok := ref(data)
		gw, gh, gok := rustCheck(t, c, data)
		if rw != gw || rh != gh || rok != gok {
			mismatch++
			t.Errorf("%s: ref=(%d,%d,%v) rust=(%d,%d,%v)", name, rw, rh, rok, gw, gh, gok)
			return
		}
		if rok {
			okCount++
		}
	})
	t.Logf("files=%d cases=%d ok=%d rejected=%d mismatch=%d", nfiles, cases, okCount, cases-okCount-mismatch, mismatch)
}

func BenchmarkRustSock(b *testing.B) {
	dir := os.Getenv("JPEGCHECK_CORPUS")
	data, err := os.ReadFile(filepath.Join(dir, "jpg", "000.jpg"))
	if err != nil {
		b.Skip()
	}
	c := startRust(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rustCheck(b, c, data)
	}
}
