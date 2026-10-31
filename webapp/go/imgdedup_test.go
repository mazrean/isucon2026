package isureal

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

func TestImgDedupPushSkipsWhenPeerHas(t *testing.T) {
	old := imgDedupOn
	imgDedupOn = true
	defer func() { imgDedupOn = old }()

	store := newImageStore(t.TempDir())
	e := echo.New()
	e.POST("/internal/images/:imageId", imageReplReceiveHandler(store, imgReplTestSecret))
	registerImgDedupRoutes(e, store, imgReplTestSecret)
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.RemoteAddr = "192.168.1.10:12345"
		if r.Method == http.MethodPost && !strings.HasSuffix(r.URL.Path, "purge") {
			posts.Add(1)
		}
		e.ServeHTTP(w, r)
	}))
	defer srv.Close()

	r := newImgReplicator(strings.TrimPrefix(srv.URL, "http://"), imgReplTestSecret)
	id := "3b0c5f3e-1111-4222-8333-944455556666"
	data := bytes.Repeat([]byte{0xcd}, 50_000)
	job := imgReplJob{peer: r.peers[0], id: id, obj: &imageObject{Owner: "o", MediaType: "image/jpeg", Data: data}}
	if err := r.send(job); err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 1 {
		t.Fatalf("first push: posts=%d", posts.Load())
	}
	// 受信側が持っているので本文は送らない。
	if err := r.send(job); err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 1 {
		t.Fatalf("second push should skip: posts=%d", posts.Load())
	}

	// purge でメモリもディスクも空になる。
	deadline := time.Now().Add(2 * time.Second)
	for store.readDisk(id) == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/internal/imgcache-purge", nil)
	req.Header.Set(forwardedSecretHeader, imgReplTestSecret)
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != http.StatusNoContent {
		t.Fatalf("purge: %v %v", err, res)
	}
	res.Body.Close()
	if store.has(id) {
		t.Fatal("still cached after purge")
	}
	// 未認証は拒否。
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/internal/images/"+id+"/have", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("have without secret: %v %v", err, res)
	}
	res.Body.Close()
}

func TestImgDedupWaitRecv(t *testing.T) {
	store := newImageStore("")
	id := "4b0c5f3e-1111-4222-8333-944455556666"
	done := store.beginRecv(id)
	if !store.has(id) {
		t.Fatal("receiving image should count as had")
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		store.put(id, &imageObject{Owner: "o", MediaType: "image/jpeg", Data: []byte("x")})
		done()
	}()
	obj := store.waitRecv(context.Background(), id)
	if obj == nil || string(obj.Data) != "x" {
		t.Fatalf("waitRecv: %+v", obj)
	}
	if store.waitRecv(context.Background(), "5b0c5f3e-1111-4222-8333-944455556666") != nil {
		t.Fatal("no receive in flight")
	}
}
