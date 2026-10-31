package isureal

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"example.com/isureal-company/isureal/internal/jpegcheck"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

// Local image origin (ISUREAL_LOCALIMG=1, needs ISUREAL_PREVALIDATE=1).
//
// europe1/america1 keep the bytes of their users' new images: the JPEG is
// checked, stored and fsynced locally under an ID minted here (UUIDv4 whose
// last 6 hex digits tag the origin region), and asia1 gets the same request
// with the image emptied plus a signed X-Isureal-Img (id, w, h, size, sha256).
// asia1 commits the metadata rows only (no image_objects INSERT) and reports
// the owner back in X-Isureal-Img-Owner, which the origin records (fsync)
// before answering the client. The bytes reach PostgreSQL asynchronously
// (journal = meta/ without flushed/ marker, replayed at startup).
//
// Readers elsewhere: imageStore.load falls back to a synchronous pull from the
// origin (/internal/localimg/:id) when PostgreSQL does not have the row yet.
// initialize on asia1 purges every origin store before resetting the DB.

const (
	localImgHeader      = "X-Isureal-Img"
	localImgOwnerHeader = "X-Isureal-Img-Owner"
	localImgMACCtx      = "isureal-img-v1|"
)

var (
	localImgOn      = os.Getenv("ISUREAL_LOCALIMG") == "1"
	localImgOrigins = parseLocalImgOrigins(envOrDefault("ISUREAL_LOCALIMG_ORIGINS", "europe=192.168.2.10:8080,america=192.168.3.10:8080"))
	localImgDir     = envOrDefault("ISUREAL_LOCALIMG_DIR", "/home/isucon/localimg")
	localImgRegion  string // set by registerLocalImg when this host is an origin
	localImgClient  = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 32, IdleConnTimeout: 300 * time.Second, DisableCompression: true}}
	localImgStats   struct{ stored, pulled, pullErr, flushed, flushErr atomic.Int64 }
)

var localImgTags = map[string]string{"europe": "e0e0e0", "america": "a0a0a0"}

func parseLocalImgOrigins(s string) map[string]string {
	out := map[string]string{}
	for _, f := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(f), "=")
		if ok && k != "" && v != "" {
			out[k] = v
		}
	}
	return out
}

func newLocalImageID(region string) string {
	id := uuid.NewString()
	return id[:len(id)-6] + localImgTags[region]
}

// localImgOriginOf returns the origin region encoded in an ID ("" = not tagged).
func localImgOriginOf(id string) string {
	if len(id) != 36 {
		return ""
	}
	for r, tag := range localImgTags {
		if strings.HasSuffix(id, tag) {
			return r
		}
	}
	return ""
}

// ---------- origin-side durable store ----------

type localImgStore struct {
	dir   string
	mu    sync.RWMutex // write lock = purge; flush inserts hold the read lock
	epoch atomic.Int64
	queue chan localImgJob
}

type localImgJob struct {
	epoch int64
	id    string
}

var localStore = &localImgStore{dir: localImgDir, queue: make(chan localImgJob, 1<<16)}

func (s *localImgStore) p(kind, id string) string { return filepath.Join(s.dir, kind, id[:2], id) }

func writeFileSync(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	if werr != nil || serr != nil || cerr != nil {
		os.Remove(f.Name())
		return fmt.Errorf("localimg write %s: %v %v %v", path, werr, serr, cerr)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		os.Remove(f.Name())
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	d.Close()
	return err
}

// meta file: "owner\ncreatedAtUnixNano\n"
func (s *localImgStore) read(id string) (obj *imageObject, createdAt time.Time) {
	if !safeImageID(id) || len(id) < 3 {
		return nil, time.Time{}
	}
	meta, err := os.ReadFile(s.p("meta", id))
	if err != nil {
		return nil, time.Time{}
	}
	owner, rest, ok := strings.Cut(string(meta), "\n")
	if !ok || owner == "" {
		return nil, time.Time{}
	}
	ns, _ := strconv.ParseInt(strings.TrimSpace(rest), 10, 64)
	data, err := os.ReadFile(s.p("data", id))
	if err != nil {
		return nil, time.Time{}
	}
	return &imageObject{Owner: owner, MediaType: "image/jpeg", Data: data}, time.Unix(0, ns)
}

func (s *localImgStore) enqueue(id string) {
	select {
	case s.queue <- localImgJob{epoch: s.epoch.Load(), id: id}:
	default:
		slog.Warn("localimg flush queue full", "id", id)
	}
}

func (s *localImgStore) flusher(db *sqlx.DB) {
	for j := range s.queue {
		for attempt := 0; ; attempt++ {
			done, err := s.flushOne(db, j)
			if done {
				break
			}
			localImgStats.flushErr.Add(1)
			if attempt >= 20 {
				slog.Error("localimg flush gave up", "id", j.id, "error", err)
				break
			}
			time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
		}
	}
}

// flushOne returns done=true when the job needs no retry.
func (s *localImgStore) flushOne(db *sqlx.DB, j localImgJob) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if j.epoch != s.epoch.Load() {
		return true, nil
	}
	obj, createdAt := s.read(j.id)
	if obj == nil {
		return true, nil
	}
	// short, so purge (initialize) never waits long behind an INSERT
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	_, err := db.ExecContext(ctx, `INSERT INTO image_objects (id, owner_user_id, data, media_type, created_at) VALUES ($1, $2, $3, 'image/jpeg', $4) ON CONFLICT DO NOTHING`, j.id, obj.Owner, obj.Data, createdAt)
	if err != nil {
		return false, err
	}
	localImgStats.flushed.Add(1)
	_ = writeFileSyncNoDir(s.p("flushed", j.id))
	return true, nil
}

func writeFileSyncNoDir(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, nil, 0o644)
}

// replay enqueues every committed image that has not reached PostgreSQL.
func (s *localImgStore) replay() {
	metaRoot := filepath.Join(s.dir, "meta")
	n := 0
	filepath.WalkDir(metaRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		id := d.Name()
		if _, err := os.Stat(s.p("flushed", id)); err == nil {
			return nil
		}
		s.enqueue(id)
		n++
		return nil
	})
	if n > 0 {
		slog.Info("localimg replay", "pending", n)
	}
}

func (s *localImgStore) purge() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch.Add(1)
	for {
		select {
		case <-s.queue:
			continue
		default:
		}
		break
	}
	trash := fmt.Sprintf("%s.trash-%d", s.dir, time.Now().UnixNano())
	if err := os.Rename(s.dir, trash); err == nil {
		go os.RemoveAll(trash)
	} else {
		os.RemoveAll(s.dir)
	}
	os.MkdirAll(s.dir, 0o755)
}

// ---------- origin side: rewrite + forward ----------

func jpegcheckBytes(b []byte) (w, h int, err error) { return jpegcheck.Check(bytes.NewReader(b)) }

type localImgPending struct {
	id string
}

type localImgPendingKey struct{}

func localImgMAC(method, path, fields string) string {
	m := hmac.New(sha256.New, []byte(forwardSecret))
	m.Write([]byte(localImgMACCtx + method + "|" + path + "|" + fields))
	return hex.EncodeToString(m.Sum(nil))
}

// splitImage returns the request body with the single image emptied, and the
// image bytes. ok=false = not eligible (caller falls back to stage 1).
func splitImage(contentType string, body []byte) (newBody, img []byte, ok bool) {
	if contentType == "image/jpeg" {
		return nil, body, len(body) > 0
	}
	mt, params, err := mime.ParseMediaType(contentType)
	if err != nil || mt != "multipart/form-data" || params["boundary"] == "" {
		return nil, nil, false
	}
	mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	var out bytes.Buffer
	out.Grow(4096)
	mw := multipart.NewWriter(&out)
	if err := mw.SetBoundary(params["boundary"]); err != nil {
		return nil, nil, false
	}
	files := 0
	for {
		p, err := mr.NextRawPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, false
		}
		data, err := io.ReadAll(p)
		if err != nil {
			return nil, nil, false
		}
		hdr := textproto.MIMEHeader{}
		for k, v := range p.Header {
			hdr[k] = v
		}
		if p.FileName() != "" {
			files++
			if files > 1 || p.FormName() != "image" || p.Header.Get("Content-Transfer-Encoding") != "" {
				return nil, nil, false
			}
			img = data
			data = nil
		}
		w, err := mw.CreatePart(hdr)
		if err != nil {
			return nil, nil, false
		}
		w.Write(data)
	}
	if files != 1 || len(img) == 0 {
		return nil, nil, false
	}
	if err := mw.Close(); err != nil {
		return nil, nil, false
	}
	return out.Bytes(), img, true
}

// localImgTryForward handles the request when the image can stay here.
func localImgTryForward(c *echo.Context, proxy http.Handler, body []byte) bool {
	req := c.Request()
	newBody, img, ok := splitImage(req.Header.Get("Content-Type"), body)
	if !ok {
		return false
	}
	w, h, err := jpegcheckBytes(img)
	if err != nil {
		return false // invalid images keep the stage-1 path (bytes go to asia1)
	}
	id := newLocalImageID(localImgRegion)
	if err := writeFileSync(localStore.p("data", id), img); err != nil {
		slog.Warn("localimg store", "error", err)
		return false
	}
	sum := sha256.Sum256(img)
	fields := id + "," + strconv.Itoa(w) + "," + strconv.Itoa(h) + "," + strconv.Itoa(len(img)) + "," + hex.EncodeToString(sum[:])
	req.Header.Del(jpegResultHeader)
	req.Header.Set(localImgHeader, fields+";"+localImgMAC(req.Method, req.URL.Path, fields))
	req = req.WithContext(context.WithValue(req.Context(), localImgPendingKey{}, &localImgPending{id: id}))
	setProxyBody(req, newBody)
	proxy.ServeHTTP(c.Response(), req)
	return true
}

func setProxyBody(req *http.Request, body []byte) {
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	req.TransferEncoding = nil
}

// localImgModifyResponse runs on the origin for every proxied response.
func localImgModifyResponse(resp *http.Response) error {
	owner := resp.Header.Get(localImgOwnerHeader)
	resp.Header.Del(localImgOwnerHeader)
	pend, _ := resp.Request.Context().Value(localImgPendingKey{}).(*localImgPending)
	if pend == nil {
		return nil
	}
	if resp.StatusCode/100 != 2 || owner == "" {
		os.Remove(localStore.p("data", pend.id))
		return nil
	}
	meta := owner + "\n" + strconv.FormatInt(time.Now().UnixNano(), 10) + "\n"
	if err := writeFileSync(localStore.p("meta", pend.id), []byte(meta)); err != nil {
		slog.Error("localimg meta", "id", pend.id, "error", err)
	}
	if data, err := os.ReadFile(localStore.p("data", pend.id)); err == nil {
		images.put(pend.id, &imageObject{Owner: owner, MediaType: "image/jpeg", Data: data})
	}
	localImgStats.stored.Add(1)
	localStore.enqueue(pend.id)
	return nil
}

func localImgAbort(r *http.Request) {
	if pend, _ := r.Context().Value(localImgPendingKey{}).(*localImgPending); pend != nil {
		os.Remove(localStore.p("data", pend.id))
	}
}

// ---------- asia1 side ----------

type localImgMeta struct {
	id   string
	w, h int
}

type localImgMetaKey struct{}

func parseLocalImgHeader(method, path, v string) *localImgMeta {
	i := strings.LastIndexByte(v, ';')
	if i < 0 || forwardSecret == "" {
		return nil
	}
	fields, mac := v[:i], v[i+1:]
	if !hmac.Equal([]byte(mac), []byte(localImgMAC(method, path, fields))) {
		return nil
	}
	f := strings.Split(fields, ",")
	if len(f) != 5 || localImgOriginOf(f[0]) == "" {
		return nil
	}
	w, err1 := strconv.Atoi(f[1])
	h, err2 := strconv.Atoi(f[2])
	if err1 != nil || err2 != nil {
		return nil
	}
	return &localImgMeta{id: f[0], w: w, h: h}
}

func localImgFromCtx(ctx context.Context) *localImgMeta {
	m, _ := ctx.Value(localImgMetaKey{}).(*localImgMeta)
	return m
}

// newImageID is the image ID for a new image_objects row.
func newImageID(ctx context.Context) string {
	if m := localImgFromCtx(ctx); m != nil {
		return m.id
	}
	return uuid.NewString()
}

// externalImage reports whether the bytes live at the origin; it then tells
// the origin who owns the image. Call only on the path that commits.
func externalImage(c *echo.Context, owner string) bool {
	if localImgFromCtx(c.Request().Context()) == nil {
		return false
	}
	c.Response().Header().Set(localImgOwnerHeader, owner)
	return true
}

// ---------- readers: pull from the origin ----------

// localImgLoad is consulted by imageStore.load: own store first (before the
// DB), then — after a DB miss — the origin.
func localImgOwn(id string) *imageObject {
	if !localImgOn || localImgRegion == "" || localImgOriginOf(id) != localImgRegion {
		return nil
	}
	obj, _ := localStore.read(id)
	return obj
}

// localImgPull: (nil, nil) = the origin does not have it; (nil, err) = the
// origin could not be reached (the caller answers 5xx, never 403).
func localImgPull(ctx context.Context, id string) (*imageObject, error) {
	if !localImgOn {
		return nil, nil
	}
	origin := localImgOriginOf(id)
	addr := localImgOrigins[origin]
	if origin == "" || origin == localImgRegion || addr == "" {
		return nil, nil
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		obj, found, err := localImgPullOnce(ctx, addr, id)
		if err == nil {
			if found {
				localImgStats.pulled.Add(1)
			}
			return obj, nil
		}
		lastErr = err
		localImgStats.pullErr.Add(1)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		time.Sleep(time.Duration(250*(attempt+1)) * time.Millisecond)
	}
	return nil, lastErr
}

func localImgPullOnce(ctx context.Context, addr, id string) (*imageObject, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/internal/localimg/"+id, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set(forwardedSecretHeader, forwardSecret)
	res, err := localImgClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, false, err
	}
	switch res.StatusCode {
	case http.StatusNotFound:
		return nil, false, nil
	case http.StatusOK:
	default:
		return nil, false, fmt.Errorf("localimg pull %s: %d", addr, res.StatusCode)
	}
	owner, data, ok := bytes.Cut(body, []byte("\n"))
	if !ok || len(owner) == 0 {
		return nil, false, fmt.Errorf("localimg pull %s: bad body", addr)
	}
	return &imageObject{Owner: string(owner), MediaType: "image/jpeg", Data: data}, true, nil
}

// localImgPurgePeers is called by initialize (asia1) before the DB reset.
func localImgPurgePeers(ctx context.Context) {
	if !localImgOn {
		return
	}
	var wg sync.WaitGroup
	for region, addr := range localImgOrigins {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 2; attempt++ {
				cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
				req, _ := http.NewRequestWithContext(cctx, http.MethodPost, "http://"+addr+"/internal/localimg-purge", nil)
				req.Header.Set(forwardedSecretHeader, forwardSecret)
				res, err := localImgClient.Do(req)
				cancel()
				if err == nil {
					res.Body.Close()
					if res.StatusCode == http.StatusNoContent {
						return
					}
				}
				slog.Warn("localimg purge", "region", region, "error", err)
			}
		}()
	}
	wg.Wait()
}

// registerLocalImg installs the routes, the asia1-side header check and, on
// an origin host, the flusher and the journal replay.
func registerLocalImg(e *echo.Echo, h *HandlerContext) {
	if !localImgOn || !prevalidateOn || forwardSecret == "" {
		return
	}
	if _, ok := localImgTags[h.Config.Region]; ok && prevalidateTarget != "" {
		localImgRegion = h.Config.Region
		os.MkdirAll(localStore.dir, 0o755)
		for range 2 {
			go localStore.flusher(h.DB)
		}
		go localStore.replay()
	}
	go func() {
		for range time.Tick(10 * time.Second) {
			a, b, c, d, f := localImgStats.stored.Swap(0), localImgStats.pulled.Swap(0), localImgStats.pullErr.Swap(0), localImgStats.flushed.Swap(0), localImgStats.flushErr.Swap(0)
			if a+b+c+d+f > 0 {
				slog.Info("localimg", "stored", a, "pulled", b, "pull_err", c, "flushed", d, "flush_err", f)
			}
		}
	}()
	e.Pre(localImgAccept)
	registerLocalImgRoutes(e)
}

func localImgAccept(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		req := c.Request()
		if v := req.Header.Get(localImgHeader); v != "" {
			req.Header.Del(localImgHeader)
			if prevalidatePeer(req.RemoteAddr) {
				if m := parseLocalImgHeader(req.Method, req.URL.Path, v); m != nil {
					c.SetRequest(req.WithContext(context.WithValue(req.Context(), localImgMetaKey{}, m)))
				}
			}
		}
		return next(c)
	}
}

func registerLocalImgRoutes(e *echo.Echo) {
	e.GET("/internal/localimg/:id", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		obj, _ := localStore.read(c.Param("id"))
		if obj == nil {
			return c.NoContent(http.StatusNotFound)
		}
		buf := make([]byte, 0, len(obj.Owner)+1+len(obj.Data))
		buf = append(append(append(buf, obj.Owner...), '\n'), obj.Data...)
		return c.Blob(http.StatusOK, "application/octet-stream", buf)
	})
	e.POST("/internal/localimg-purge", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		localStore.purge()
		return c.NoContent(http.StatusNoContent)
	})
}
