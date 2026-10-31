package isureal

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

const imgReplTestSecret = "s3cret"

func newImgReplTestEcho(store *imageStore) *echo.Echo {
	e := echo.New()
	e.POST("/internal/images/:imageId", imageReplReceiveHandler(store, imgReplTestSecret))
	return e
}

func imgReplTestRequest(id, remote, secret, sig string, data []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/internal/images/"+id, bytes.NewReader(data))
	req.RemoteAddr = remote
	req.Header.Set(forwardedSecretHeader, secret)
	req.Header.Set(imgReplOwnerHeader, "owner-1")
	req.Header.Set(imgReplMediaTypeHeader, "image/jpeg")
	req.Header.Set(imgReplSigHeader, sig)
	return req
}

func TestImageReplReceive(t *testing.T) {
	id := "0b0c5f3e-1111-4222-8333-944455556666"
	data := []byte("\xff\xd8jpeg\nbytes")
	goodSig := imgReplSign(imgReplTestSecret, id, "owner-1", "image/jpeg", data)

	cases := []struct {
		name   string
		id     string
		remote string
		secret string
		sig    string
		want   int
	}{
		{"wrong secret", id, "192.168.2.10:5555", "nope", goodSig, http.StatusForbidden},
		{"empty secret", id, "192.168.2.10:5555", "", goodSig, http.StatusForbidden},
		{"bad source", id, "10.0.0.1:1", imgReplTestSecret, goodSig, http.StatusForbidden},
		{"loopback source", id, "127.0.0.1:1", imgReplTestSecret, goodSig, http.StatusForbidden},
		{"bad sig", id, "192.168.2.10:5555", imgReplTestSecret, strings.Repeat("0", 64), http.StatusForbidden},
		{"unsafe id", "..%2Fx", "192.168.2.10:5555", imgReplTestSecret, goodSig, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newImageStore(t.TempDir())
			rec := httptest.NewRecorder()
			newImgReplTestEcho(store).ServeHTTP(rec, imgReplTestRequest(tc.id, tc.remote, tc.secret, tc.sig, data))
			if rec.Code != tc.want {
				t.Fatalf("status %d, want %d", rec.Code, tc.want)
			}
			if _, ok := store.mem.get(id); ok {
				t.Fatal("stored despite rejection")
			}
		})
	}

	t.Run("ok", func(t *testing.T) {
		store := newImageStore(t.TempDir())
		rec := httptest.NewRecorder()
		newImgReplTestEcho(store).ServeHTTP(rec, imgReplTestRequest(id, "192.168.3.10:4444", imgReplTestSecret, goodSig, data))
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status %d", rec.Code)
		}
		// DB (nil) に触れずにヒットする。
		got, err := store.get(context.Background(), nil, nil, id)
		if err != nil || got == nil || got.Owner != "owner-1" || got.MediaType != "image/jpeg" || !bytes.Equal(got.Data, data) {
			t.Fatalf("get: %v %+v", err, got)
		}
		// ディスクにも書かれる。
		deadline := time.Now().Add(2 * time.Second)
		for store.readDisk(id) == nil {
			if time.Now().After(deadline) {
				t.Fatal("not written to disk")
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// 送信側 → 受信側の往復 (受信側は送信元をピア IP に見せかける)。
func TestImageReplPushEndToEnd(t *testing.T) {
	store := newImageStore(t.TempDir())
	e := newImgReplTestEcho(store)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.RemoteAddr = "192.168.1.10:12345"
		e.ServeHTTP(w, r)
	}))
	defer srv.Close()

	r := newImgReplicator(strings.TrimPrefix(srv.URL, "http://")+", ", imgReplTestSecret)
	if len(r.peers) != 1 {
		t.Fatalf("peers: %v", r.peers)
	}
	id := "1b0c5f3e-1111-4222-8333-944455556666"
	data := bytes.Repeat([]byte{0xab}, 90_000)
	r.enqueue(id, &imageObject{Owner: "o", MediaType: "image/jpeg", Data: data})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got, ok := store.mem.get(id); ok {
			if !bytes.Equal(got.Data, data) || got.Owner != "o" {
				t.Fatal("mismatch")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not replicated")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 秘密不一致の送信側は受け付けられない (リトライ後に諦める)。
	bad := newImgReplicator(strings.TrimPrefix(srv.URL, "http://"), "other")
	if err := bad.send(imgReplJob{peer: bad.peers[0], id: "2b0c5f3e-1111-4222-8333-944455556666", obj: &imageObject{Owner: "o", MediaType: "image/jpeg", Data: data}}); err == nil {
		t.Fatal("expected rejection")
	}

	// ピア未設定なら何もしない。
	if n := newImgReplicator("", imgReplTestSecret); n.peers != nil || n.jobs != nil {
		t.Fatal("expected no-op replicator")
	}
}
