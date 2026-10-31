package isureal

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_IMG_SENDFILE: the disk-cache path returns the same status, type and
// body as the buffered path, and still rejects bad signatures / invalid images.
func TestImageSendfileMatchesBlob(t *testing.T) {
	defer func(s *imageStore, on bool) { images, imgSendfileOn = s, on }(images, imgSendfileOn)
	images = newImageStore(t.TempDir())
	id := "0b0c5f3e-1111-4222-8333-944455556666"
	data := bytes.Repeat([]byte("jpeg\n\x00\xff"), 40000) // ~280KB, has newlines
	images.writes <- imageDiskWrite{id: id, obj: &imageObject{Owner: "u1", MediaType: "image/jpeg", Data: data}}
	for deadline := time.Now().Add(2 * time.Second); images.openDisk(id) == nil; {
		if time.Now().After(deadline) {
			t.Fatal("disk write not visible")
		}
		time.Sleep(10 * time.Millisecond)
	}

	const skh = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	m.d.applyUser(memUserRow{ID: "u1", Username: "u1", SecretKeyHash: skh, ProfileImageID: &id, Status: "active"})
	h := &HandlerContext{Mem: m, reads: &readRouter{}}
	e := echo.New()
	e.GET("/api/images/:imageId", getImageHandler(h))
	srv := httptest.NewServer(e)
	defer srv.Close()

	key, _ := imageSigningKey(skh)
	exp := time.Now().Add(time.Hour).Unix()
	sig := base64.RawURLEncoding.EncodeToString(imageSignature(key, id, exp))
	get := func(id, sig string) (int, string, []byte) {
		resp, err := http.Get(srv.URL + "/api/images/" + id + "?exp=" + strconv.FormatInt(exp, 10) + "&sig=" + sig)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("Content-Type") + "|" + resp.Header.Get("Cache-Control"), b
	}

	imgSendfileOn = true
	c1, h1, b1 := get(id, sig) // disk -> sendfile
	if _, inMem := images.mem.get(id); inMem {
		t.Fatal("sendfile path should not load the LRU")
	}
	imgSendfileOn = false
	c2, h2, b2 := get(id, sig) // images.get: disk -> LRU -> Blob
	imgSendfileOn = true
	c3, h3, b3 := get(id, sig) // LRU hit
	if c1 != 200 || c1 != c2 || c2 != c3 || h1 != h2 || h2 != h3 || !bytes.Equal(b1, data) || !bytes.Equal(b1, b2) || !bytes.Equal(b2, b3) {
		t.Fatalf("mismatch: %d/%d/%d %q/%q/%q len %d/%d/%d", c1, c2, c3, h1, h2, h3, len(b1), len(b2), len(b3))
	}

	// bad signature, sendfile path (evict from memory first)
	images.mem = newImageLRU(imageMemCacheBudgetDefMB << 20)
	bad := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	// (echo.New's default error handler renders APIError as 500; compare with the flag off)
	cOn, _, bOn := get(id, bad)
	if _, inMem := images.mem.get(id); inMem {
		t.Fatal("bad sig request should have taken the disk path")
	}
	imgSendfileOn = false
	cOff, _, bOff := get(id, bad)
	if cOn == 200 || cOn != cOff || !bytes.Equal(bOn, bOff) {
		t.Fatalf("bad sig: on %d %q / off %d %q", cOn, bOn, cOff, bOff)
	}
}
