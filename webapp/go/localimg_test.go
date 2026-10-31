package isureal

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

func setupLocalImgTest(t *testing.T) {
	t.Helper()
	oldSecret, oldOn, oldTarget, oldLI, oldRegion, oldDir, oldOrigins := forwardSecret, prevalidateOn, prevalidateTarget, localImgOn, localImgRegion, localStore.dir, localImgOrigins
	forwardSecret, prevalidateOn, localImgOn = "test-secret", true, true
	localStore.dir = t.TempDir()
	t.Cleanup(func() {
		forwardSecret, prevalidateOn, prevalidateTarget, localImgOn, localImgRegion, localStore.dir, localImgOrigins = oldSecret, oldOn, oldTarget, oldLI, oldRegion, oldDir, oldOrigins
		for len(localStore.queue) > 0 {
			<-localStore.queue
		}
	})
}

func TestLocalImgIDTag(t *testing.T) {
	for _, r := range []string{"europe", "america"} {
		id := newLocalImageID(r)
		if len(id) != 36 || localImgOriginOf(id) != r || id[14] != '4' {
			t.Fatalf("%s: %s", r, id)
		}
	}
	if localImgOriginOf("00000000-0000-4000-8000-000000000000") != "" {
		t.Fatal("untagged id tagged")
	}
}

func TestLocalImgHeaderMAC(t *testing.T) {
	setupLocalImgTest(t)
	id := newLocalImageID("europe")
	fields := id + ",64,48,100," + strings.Repeat("ab", 32)
	v := fields + ";" + localImgMAC("POST", "/api/posts", fields)
	if m := parseLocalImgHeader("POST", "/api/posts", v); m == nil || m.id != id || m.w != 64 || m.h != 48 {
		t.Fatalf("roundtrip %v", m)
	}
	bad := map[string][3]string{
		"other path":   {"POST", "/api/posts/x/realmojis", v},
		"other method": {"PUT", "/api/posts", v},
		"tampered":     {"POST", "/api/posts", strings.Replace(v, ",64,", ",65,", 1)},
		"no mac":       {"POST", "/api/posts", fields},
		"untagged id":  {"POST", "/api/posts", "x,1,1,1,a;" + localImgMAC("POST", "/api/posts", "x,1,1,1,a")},
	}
	for name, c := range bad {
		if parseLocalImgHeader(c[0], c[1], c[2]) != nil {
			t.Fatalf("%s accepted", name)
		}
	}
	forwardSecret = "other"
	if parseLocalImgHeader("POST", "/api/posts", v) != nil {
		t.Fatal("wrong secret accepted")
	}
}

// Error paths must answer exactly as before; asia1 side is the real handlers
// with a failing DB (so a valid image ends in the same 500 on both paths).
func TestLocalImgDifferential(t *testing.T) {
	setupLocalImgTest(t)
	localImgRegion = "europe"
	runPrevalidateDifferential(t, true)
	// every via-eu request failed on asia1 (fake DB / validation): nothing kept
	if n := countFiles(t, localStore.dir); n != 0 || len(localStore.queue) != 0 {
		t.Fatalf("leftover files %d queue %d", n, len(localStore.queue))
	}
}

func TestLocalImgEndToEnd(t *testing.T) {
	setupLocalImgTest(t)
	good := testJPEG(t, 120, 90)

	// fake asia1: the same calls the handlers make
	var sawLen int
	asia := echo.New()
	asia.Pre(localImgAccept)
	handler := func(c *echo.Context) error {
		var data []byte
		if c.Request().Header.Get("Content-Type") == "image/jpeg" {
			data, _ = io.ReadAll(c.Request().Body)
		} else {
			fh, err := c.FormFile("image")
			if err != nil {
				return c.String(400, "noimage")
			}
			f, _ := fh.Open()
			data, _ = io.ReadAll(f)
			f.Close()
			if c.FormValue("caption") != "hi" {
				return c.String(400, "caption lost")
			}
		}
		sawLen = len(data)
		w, h, ok := checkJPEG(c.Request().Context(), data)
		if !ok || w != 120 || h != 90 {
			return c.String(400, "bad")
		}
		id := newImageID(c.Request().Context())
		if !externalImage(c, "owner-1") {
			return c.String(500, "not external")
		}
		return c.String(201, id)
	}
	asia.POST("/api/posts", handler)
	asia.PUT("/api/me/profile-image", handler)
	asiaSrv := httptest.NewServer(asia)
	t.Cleanup(asiaSrv.Close)

	prevalidateTarget = strings.TrimPrefix(asiaSrv.URL, "http://")
	eu := echo.New()
	registerPrevalidate(eu, "europe")
	registerLocalImgRoutes(eu)
	localImgRegion = "europe"
	euSrv := httptest.NewServer(eu)
	t.Cleanup(euSrv.Close)

	ct, body := multipartBody(t, map[string]string{"caption": "hi", "visibility": "public"}, map[string][]byte{"image": good})
	for _, rc := range []reqCase{
		{"post", "POST", "/api/posts", ct, body, true},
		{"profile", "PUT", "/api/me/profile-image", "image/jpeg", good, true},
	} {
		req, _ := http.NewRequest(rc.method, euSrv.URL+rc.path, bytes.NewReader(rc.body))
		req.Header.Set("Content-Type", rc.ctype)
		req.Header.Set(prevalidateHeader, "test-secret")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		idb, _ := io.ReadAll(res.Body)
		res.Body.Close()
		id := string(idb)
		if res.StatusCode != 201 || res.Header.Get(localImgOwnerHeader) != "" || sawLen != 0 {
			t.Fatalf("%s: %d %q owner-leak=%q sawLen=%d", rc.name, res.StatusCode, id, res.Header.Get(localImgOwnerHeader), sawLen)
		}
		if localImgOriginOf(id) != "europe" {
			t.Fatalf("%s: id %q", rc.name, id)
		}
		obj, _ := localStore.read(id)
		if obj == nil || obj.Owner != "owner-1" || !bytes.Equal(obj.Data, good) {
			t.Fatalf("%s: store %v", rc.name, obj)
		}
		select {
		case j := <-localStore.queue:
			if j.id != id {
				t.Fatalf("%s: queued %s", rc.name, j.id)
			}
		default:
			t.Fatalf("%s: not queued for PG", rc.name)
		}
		// a reader in another region pulls the same bytes from the origin
		localImgRegion = ""
		localImgOrigins = map[string]string{"europe": strings.TrimPrefix(euSrv.URL, "http://")}
		p := localImgPull(t.Context(), id)
		if p == nil || p.Owner != "owner-1" || !bytes.Equal(p.Data, good) {
			t.Fatalf("%s: pull %v", rc.name, p)
		}
		localImgRegion = "europe"
	}

	// a rejected write leaves nothing behind
	bad := echo.New()
	bad.Pre(localImgAccept)
	bad.POST("/api/posts", func(c *echo.Context) error { externalImage(c, "x"); return c.String(409, "slot") })
	badSrv := httptest.NewServer(bad)
	t.Cleanup(badSrv.Close)
	prevalidateTarget = strings.TrimPrefix(badSrv.URL, "http://")
	eu2 := echo.New()
	registerPrevalidate(eu2, "europe")
	eu2Srv := httptest.NewServer(eu2)
	t.Cleanup(eu2Srv.Close)
	before := countFiles(t, localStore.dir+"/data")
	req, _ := http.NewRequest("POST", eu2Srv.URL+"/api/posts", bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	req.Header.Set(prevalidateHeader, "test-secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 409 || res.Header.Get(localImgOwnerHeader) != "" || countFiles(t, localStore.dir+"/data") != before || len(localStore.queue) != 0 {
		t.Fatalf("rejected: %d files %d->%d queue %d", res.StatusCode, before, countFiles(t, localStore.dir+"/data"), len(localStore.queue))
	}

	// purge (initialize) empties the origin store; pulls then miss
	localImgOrigins = map[string]string{"europe": strings.TrimPrefix(euSrv.URL, "http://")}
	localImgPurgePeers(t.Context())
	if n := countFiles(t, localStore.dir); n != 0 {
		t.Fatalf("after purge: %d files", n)
	}
	localImgRegion = ""
	if p := localImgPull(t.Context(), newLocalImageID("europe")); p != nil {
		t.Fatal("pull after purge")
	}
}

func countFiles(t *testing.T, dir string) int {
	n := 0
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() {
			n += countFiles(t, dir+"/"+e.Name())
		} else {
			n++
		}
	}
	return n
}
