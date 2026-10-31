package isureal

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"example.com/isureal-company/isureal/internal/jpegcheck"
)

func TestParseJPEGGP(t *testing.T) {
	old := jpegRemotes
	t.Cleanup(func() { jpegRemotes = old })
	jpegRemotes = parseJPEGRemotes("192.168.4.10:8080/2/400,192.168.2.10:8080/8/1200,192.168.3.10:8080/8/1200")
	for _, s := range []string{"", "0", "0/4", "x"} {
		if parseJPEGGP(s) != nil {
			t.Errorf("%q: want off", s)
		}
	}
	c := parseJPEGGP("4/6")
	if c == nil || cap(c.local) != 4 || len(c.remotes) != 1 || c.remotes[0].addr != "192.168.4.10:8080" || cap(c.remotes[0].slots) != 6 || c.remotes[0].label != "4" {
		t.Fatalf("got %+v", c)
	}
	if c.remotes[0].slots == jpegRemotes[0].slots {
		t.Fatal("GP slots must be separate from the normal slots")
	}
	if c := parseJPEGGP("3"); c == nil || cap(c.remotes[0].slots) != 2 {
		t.Fatalf("default R: %+v", c)
	}
}

func gpTestJPEG(t *testing.T) []byte {
	var b bytes.Buffer
	if err := jpeg.Encode(&b, image.NewGray(image.Rect(0, 0, 17, 9)), nil); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// The GP path tries the local slot first, then the asia2 slot, and the
// verdict always equals jpegcheck.Check whichever side ran it.
func TestCheckJPEGWithLocalFirst(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		data, _ := io.ReadAll(r.Body)
		gw, gh, err := jpegcheck.Check(bytes.NewReader(data))
		if err != nil {
			fmt.Fprint(w, "0 0 0")
			return
		}
		fmt.Fprintf(w, "1 %d %d", gw, gh)
	}))
	defer srv.Close()
	rs := parseJPEGRemotes(strings.TrimPrefix(srv.URL, "http://") + "/1/2000")
	good := gpTestJPEG(t)
	inputs := [][]byte{good, good[:len(good)/2], nil, []byte("not a jpeg")}

	loc := make(chan struct{}, 1)
	verify := func(tag string) {
		for _, in := range inputs {
			w, h, ok := checkJPEGWith(context.Background(), in, rs, loc, true)
			gw, gh, gerr := jpegcheck.Check(bytes.NewReader(in))
			if w != gw || h != gh || ok != (gerr == nil) {
				t.Errorf("%s len=%d: got (%d,%d,%v) want (%d,%d,%v)", tag, len(in), w, h, ok, gw, gh, gerr == nil)
			}
		}
	}
	verify("local")
	if hits.Load() != 0 {
		t.Fatalf("local slot was free, remote got %d", hits.Load())
	}
	loc <- struct{}{} // local busy -> remote
	verify("remote")
	if hits.Load() != int64(len(inputs)) {
		t.Fatalf("remote hits %d want %d", hits.Load(), len(inputs))
	}
	if len(rs[0].slots) != 0 {
		t.Fatal("remote slot leaked")
	}
	<-loc
	// Non-GP order is unchanged: remote first even when local is free.
	checkJPEGWith(context.Background(), good, rs, loc, false)
	if hits.Load() != int64(len(inputs))+1 {
		t.Fatal("non-GP path should prefer the remote")
	}
}

// A dead asia2 falls back to the local check with the same verdict.
func TestCheckJPEGWithRemoteDown(t *testing.T) {
	rs := parseJPEGRemotes("127.0.0.1:1/1/200")
	loc := make(chan struct{}, 1)
	loc <- struct{}{}
	w, h, ok := checkJPEGWith(context.Background(), gpTestJPEG(t), rs, loc, true)
	if !ok || w != 17 || h != 9 {
		t.Fatalf("got (%d,%d,%v)", w, h, ok)
	}
}
