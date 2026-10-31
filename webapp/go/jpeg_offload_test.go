package isureal

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"example.com/isureal-company/isureal/internal/jpegcheck"
)

// Exercises postJPEGCheck against webapp/rust/jpegcheckd over a unix socket
// (JPEGCHECK_RUST_BIN=binary, JPEGCHECK_CORPUS=dir with */*.jpg).
func TestPostJPEGCheckLocalSock(t *testing.T) {
	bin, dir := os.Getenv("JPEGCHECK_RUST_BIN"), os.Getenv("JPEGCHECK_CORPUS")
	if bin == "" || dir == "" {
		t.Skip("JPEGCHECK_RUST_BIN / JPEGCHECK_CORPUS not set")
	}
	sock := fmt.Sprintf("@jpegcheckd-offload-test-%d", os.Getpid())
	cmd := exec.Command(bin, "--sock", sock)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	for i := 0; ; i++ {
		if c, err := net.Dial("unix", sock); err == nil {
			c.Close()
			break
		} else if i > 200 {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}}
	files, _ := filepath.Glob(filepath.Join(dir, "*", "*.jpg"))
	if len(files) > 20 {
		files = files[:20]
	}
	for _, f := range files {
		data, _ := os.ReadFile(f)
		for _, in := range [][]byte{data, data[:len(data)/2], nil} {
			w, h, ok, err := postJPEGCheck(context.Background(), client, "http://jpegcheckd/internal/jpegcheck", "local", in, 2*time.Second)
			if err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			gw, gh, gerr := jpegcheck.Check(bytes.NewReader(in))
			if w != gw || h != gh || ok != (gerr == nil) {
				t.Errorf("%s len=%d: sock=(%d,%d,%v) go=(%d,%d,%v)", f, len(in), w, h, ok, gw, gh, gerr == nil)
			}
		}
	}
	// A dead socket must surface as an error (the caller then falls back).
	dead := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", "@jpegcheckd-nonexistent")
		},
	}}
	if _, _, _, err := postJPEGCheck(context.Background(), dead, "http://jpegcheckd/internal/jpegcheck", "dead", []byte("x"), time.Second); err == nil {
		t.Fatal("expected error from dead socket")
	}
}
