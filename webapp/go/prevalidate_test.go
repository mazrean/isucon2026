package isureal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"image"
	"image/jpeg"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

func testJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, image.NewRGBA(image.Rect(0, 0, w, h)), nil); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestJPEGResultsSignature(t *testing.T) {
	good, bad := testJPEG(t, 64, 48), []byte("not a jpeg")
	entries := []string{formatJPEGEntry(sha256.Sum256(good), jpegResult{64, 48, true}), formatJPEGEntry(sha256.Sum256(bad), jpegResult{})}
	v := signJPEGResults("s1", entries)

	m := parseJPEGResults("s1", v)
	if m == nil || m[sha256.Sum256(good)] != (jpegResult{64, 48, true}) || m[sha256.Sum256(bad)] != (jpegResult{}) {
		t.Fatalf("roundtrip: %v", m)
	}
	// tampered entries / mac, wrong secret, missing, garbage
	tampered := strings.Replace(v, "64x48", "64x49", 1)
	badToGood := strings.Replace(v, "=bad", "=1x1", 1)
	flipMac := v[:len(v)-1] + string("0123456789abcdef"[(strings.IndexByte("0123456789abcdef", v[len(v)-1])+1)%16])
	for name, hv := range map[string]string{"tampered": tampered, "badToGood": badToGood, "flipMac": flipMac, "nomac": strings.Split(v, ";")[0], "empty": "", "garbage": "x;y"} {
		if m := parseJPEGResults("s1", hv); m != nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if parseJPEGResults("s2", v) != nil || parseJPEGResults("", v) != nil {
		t.Fatal("wrong/empty secret accepted")
	}
	// results only apply to the exact bytes they were computed for
	ctx := context.WithValue(context.Background(), jpegResultsKey{}, m)
	if _, _, _, hit := prevalidated(ctx, append([]byte{}, good[:len(good)-1]...)); hit {
		t.Fatal("other bytes hit")
	}
	if w, h, ok, hit := prevalidated(ctx, good); !hit || !ok || w != 64 || h != 48 {
		t.Fatal("good miss")
	}
	if _, _, _, hit := prevalidated(context.Background(), good); hit {
		t.Fatal("hit without header")
	}
}

type failConnector struct{}

func (failConnector) Connect(context.Context) (driver.Conn, error) { return nil, errors.New("fake db") }
func (failConnector) Driver() driver.Driver                        { return nil }

type captured struct {
	mu                     sync.Mutex
	host, region, sec, jpg string
	bodySum                [32]byte
}

func newTestAsia(t *testing.T, cap *captured) *httptest.Server {
	db := sqlx.NewDb(sql.OpenDB(failConnector{}), "pgx")
	h := &HandlerContext{DB: db, reads: &readRouter{primary: db}, Config: &Config{Region: "asia"}}
	e := echo.New()
	e.HTTPErrorHandler = APIErrorHandler
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()
			b, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(b))
			cap.mu.Lock()
			cap.host, cap.region, cap.sec, cap.jpg = r.Host, r.Header.Get(forwardedRegionHeader), r.Header.Get(forwardedSecretHeader), r.Header.Get(jpegResultHeader)
			cap.bodySum = sha256.Sum256(b)
			cap.mu.Unlock()
			return next(c)
		}
	})
	prevalidateTarget = ""
	registerPrevalidate(e, "asia")
	sess := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			c.Set("session", &Session{UserID: "u1", Region: "europe"})
			return next(c)
		}
	}
	e.POST("/api/posts", createPostHandler(h), sess)
	e.PUT("/api/me/profile-image", putProfileImageHandler(h), sess)
	e.POST("/api/posts/:postId/realmojis", replaceRealMojiHandler(h), sess)
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	return srv
}

func multipartBody(t *testing.T, fields map[string]string, files map[string][]byte) (string, []byte) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		mw.WriteField(k, v)
	}
	for k, v := range files {
		hdr := textproto.MIMEHeader{}
		hdr.Set("Content-Disposition", `form-data; name="`+k+`"; filename="a.jpg"`)
		hdr.Set("Content-Type", "image/jpeg")
		pw, _ := mw.CreatePart(hdr)
		pw.Write(v)
	}
	mw.Close()
	return mw.FormDataContentType(), buf.Bytes()
}

type reqCase struct {
	name, method, path, ctype string
	body                      []byte
	wantHit                   bool
}

func doReq(t *testing.T, base string, rc reqCase, hdr map[string]string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(rc.method, base+rc.path, bytes.NewReader(rc.body))
	req.Header.Set("Content-Type", rc.ctype)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, res.Header.Get("Content-Type") + "\n" + string(b)
}

// Same requests straight to asia1 and via the eu/am validate-and-forward app
// must produce byte-identical responses, with the via-eu ones skipping the
// local JPEG decode on asia1.
func TestPrevalidateDifferential(t *testing.T) {
	oldSecret, oldOn, oldTarget := forwardSecret, prevalidateOn, prevalidateTarget
	forwardSecret, prevalidateOn = "test-secret", true
	t.Cleanup(func() { forwardSecret, prevalidateOn, prevalidateTarget = oldSecret, oldOn, oldTarget })

	cap := &captured{}
	asia := newTestAsia(t, cap)
	eu := echo.New()
	prevalidateTarget = strings.TrimPrefix(asia.URL, "http://")
	registerPrevalidate(eu, "europe")
	prevalidateTarget = ""
	euSrv := httptest.NewServer(eu)
	t.Cleanup(euSrv.Close)

	good, small, big, huge := testJPEG(t, 640, 480), testJPEG(t, 100, 100), testJPEG(t, 600, 600), testJPEG(t, 4097, 8)
	garbage := []byte("not a jpeg at all")
	trunc := good[:len(good)/2]
	var cases []reqCase
	post := func(name string, f map[string]string, img []byte, hit bool) {
		files := map[string][]byte{}
		if img != nil {
			files["image"] = img
		}
		ct, b := multipartBody(t, f, files)
		cases = append(cases, reqCase{"post/" + name, "POST", "/api/posts", ct, b, hit})
	}
	pf := map[string]string{"caption": "hi", "visibility": "public", "location_kind": "none"}
	post("good", pf, good, true)
	post("garbage", pf, garbage, true)
	post("trunc", pf, trunc, true)
	post("huge", pf, huge, true)
	post("noimage", pf, nil, false)
	post("badvis+garbage", map[string]string{"caption": "hi", "visibility": "x", "location_kind": "none"}, garbage, false)
	rm := func(name, typ string, img []byte, hit bool) {
		ct, b := multipartBody(t, map[string]string{"reactionType": typ}, map[string][]byte{"image": img})
		cases = append(cases, reqCase{"realmoji/" + name, "POST", "/api/posts/p1/realmojis", ct, b, hit})
	}
	rm("good", "like", small, true)
	rm("big", "like", big, true)
	rm("garbage", "smile", garbage, true)
	rm("badtype+garbage", "nope", garbage, false)
	for name, img := range map[string][]byte{"good": small, "big": big, "garbage": garbage, "empty": nil} {
		cases = append(cases, reqCase{"profile/" + name, "PUT", "/api/me/profile-image", "image/jpeg", img, true})
	}
	cases = append(cases, reqCase{"profile/wrongtype", "PUT", "/api/me/profile-image", "image/png", small, false})
	cases = append(cases, reqCase{"post/broken-multipart", "POST", "/api/posts", "multipart/form-data; boundary=zzz", []byte("--zzz\r\nbroken"), false})

	viaEU := map[string]string{prevalidateHeader: "test-secret"}
	for _, rc := range cases {
		ds, db := doReq(t, asia.URL, rc, nil)
		h0, l0 := prevalidateHits.Load(), jpegStats.local.Load()
		es, eb := doReq(t, euSrv.URL, rc, viaEU)
		hits, locals := prevalidateHits.Load()-h0, jpegStats.local.Load()-l0
		if ds != es || db != eb {
			t.Fatalf("%s: direct=%d %q via-eu=%d %q", rc.name, ds, db, es, eb)
		}
		cap.mu.Lock()
		if cap.region != "europe" || cap.sec != "test-secret" || cap.bodySum != sha256.Sum256(rc.body) || !strings.HasPrefix(euSrv.URL, "http://"+cap.host) {
			t.Fatalf("%s: forwarded region=%q secret-ok=%v body-ok=%v host=%q", rc.name, cap.region, cap.sec == "test-secret", cap.bodySum == sha256.Sum256(rc.body), cap.host)
		}
		cap.mu.Unlock()
		if rc.wantHit && (hits != 1 || locals != 0) {
			t.Fatalf("%s: hits=%d locals=%d", rc.name, hits, locals)
		}
		if !rc.wantHit && hits != 0 {
			t.Fatalf("%s: unexpected hit", rc.name)
		}
		t.Logf("%s: %d %s", rc.name, ds, strings.ReplaceAll(db, "\n", " "))
	}

	// a forged / replayed header straight to asia1 is ignored
	forged := signJPEGResults("other-secret", []string{formatJPEGEntry(sha256.Sum256(garbage), jpegResult{1, 1, true})})
	replay := signJPEGResults("test-secret", []string{formatJPEGEntry(sha256.Sum256(small), jpegResult{100, 100, true})})
	for name, hv := range map[string]string{"forged": forged, "replay-other-body": replay} {
		rc := reqCase{"profile/" + name, "PUT", "/api/me/profile-image", "image/jpeg", garbage, false}
		ds, db := doReq(t, asia.URL, rc, nil)
		h0 := prevalidateHits.Load()
		fs, fb := doReq(t, asia.URL, rc, map[string]string{jpegResultHeader: hv})
		if ds != fs || db != fb || prevalidateHits.Load() != h0 || ds != http.StatusBadRequest {
			t.Fatalf("%s: direct=%d %q forged=%d %q", name, ds, db, fs, fb)
		}
	}
	// a client cannot trigger forwarding without the nginx-held secret
	rc := cases[0]
	if s, _ := doReq(t, euSrv.URL, rc, map[string]string{prevalidateHeader: "wrong"}); s != http.StatusNotFound {
		t.Fatalf("wrong prevalidate secret: %d", s)
	}
}
