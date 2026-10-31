package isureal

import (
	"bytes"
	"context"
	"image"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

func TestJPEGOffloadMatchesLocal(t *testing.T) {
	old := forwardSecret
	forwardSecret = "test-secret"
	defer func() { forwardSecret = old }()
	e := echo.New()
	registerJPEGCheckRoutes(e)
	srv := httptest.NewServer(e)
	defer srv.Close()
	jpegRemotes = parseJPEGRemotes(strings.TrimPrefix(srv.URL, "http://") + "/4")
	defer func() { jpegRemotes = nil }()

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 640, 480)), nil); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	cases := map[string][]byte{"good": good, "trunc": good[:len(good)/2], "empty": nil, "garbage": []byte("not a jpeg")}
	for name, data := range cases {
		w, h, ok := checkJPEG(context.Background(), data)
		r := jpegRemotes[0]
		rw, rh, rok, err := r.check(context.Background(), data)
		if err != nil {
			t.Fatalf("%s: remote err %v", name, err)
		}
		if w != rw || h != rh || ok != rok {
			t.Fatalf("%s: local=(%d,%d,%v) remote=(%d,%d,%v)", name, w, h, ok, rw, rh, rok)
		}
		if name == "good" && (!ok || w != 640 || h != 480) {
			t.Fatalf("good: %d %d %v", w, h, ok)
		}
		if name != "good" && ok {
			t.Fatalf("%s accepted", name)
		}
	}
	// a request without the shared secret is refused
	res, err := http.Post(srv.URL+"/internal/jpegcheck", "image/jpeg", bytes.NewReader(good))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("no-secret status %d", res.StatusCode)
	}
}
