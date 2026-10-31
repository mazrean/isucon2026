package isureal

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"hash/fnv"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

// 署名ファーム (分散モードの peer のみ, ISUREAL_MEM_SIGFARM=0 で無効):
// peer は changelog で見える画像 (投稿 / RealMoji / プロフ画像) と、ログイン中ユーザーの Memories に出る画像について、
// 現在と次の exp 境界の署名を計算し、全ホストの署名キャッシュ (signatureCache) へ配る。
// キャッシュに無い署名は従来どおり各ホストで計算するので、正しさは配布に依存しない。

var memCurrentFarm *sigFarm // テスト用

const imageExpStep = int64((imageURLLifetime - imageURLReuseMargin) / time.Second)

func currentImageExp(now time.Time) int64 {
	exp := now.Add(imageURLLifetime).Unix()
	return exp - exp%imageExpStep
}

type sigItem struct {
	key     [sha256.Size]byte
	imageID string
}

type sigEntry struct {
	Key     []byte `json:"k"`
	ImageID string `json:"i"`
	Exp     int64  `json:"e"`
	Sig     []byte `json:"s"`
}

type sigFarm struct {
	m       *memStore
	db      *sqlx.DB
	index   int
	count   int
	targets []string

	mu       sync.Mutex
	cond     *sync.Cond
	lastExp  map[sigItem]int64 // 計算済みの最大 exp
	high     []sigItem
	low      []sigItem
	queued   map[sigItem]bool
	out      []sigEntry
	prefetch []string
	prefDone map[string]bool
	curExp   int64
}

func newSigFarm(config *Config, m *memStore, db *sqlx.DB) *sigFarm {
	if os.Getenv("ISUREAL_MEM_SIGFARM") == "0" {
		return nil
	}
	index := map[string]int{"europe": 0, "america": 1}[config.Region]
	if v, err := strconv.Atoi(os.Getenv("ISUREAL_MEM_SIG_INDEX")); err == nil {
		index = v
	}
	count := 2
	if v, err := strconv.Atoi(os.Getenv("ISUREAL_MEM_SIG_COUNT")); err == nil && v > 0 {
		count = v
	}
	var targets []string
	if v := os.Getenv("ISUREAL_MEM_SIG_TARGETS"); v != "" {
		for _, t := range strings.Split(v, ",") {
			if t = strings.TrimSpace(t); t != "" {
				targets = append(targets, t)
			}
		}
	} else {
		self := map[string]string{"asia": "192.168.1.10:8080", "europe": "192.168.2.10:8080", "america": "192.168.3.10:8080"}[config.Region]
		for _, t := range []string{"192.168.1.10:8080", "192.168.2.10:8080", "192.168.3.10:8080"} {
			if t != self {
				targets = append(targets, t)
			}
		}
	}
	f := &sigFarm{m: m, db: db, index: index, count: count, targets: targets,
		lastExp: map[sigItem]int64{}, queued: map[sigItem]bool{}, prefDone: map[string]bool{}}
	f.cond = sync.NewCond(&f.mu)
	memCurrentFarm = f
	workers := 2
	if v, err := strconv.Atoi(os.Getenv("ISUREAL_MEM_SIG_WORKERS")); err == nil && v > 0 {
		workers = v
	}
	for range workers {
		go f.worker()
	}
	go f.flusher()
	go f.roller()
	go f.prefetcher()
	return f
}

func (f *sigFarm) mine(imageID string) bool {
	h := fnv.New32a()
	h.Write([]byte(imageID))
	return int(h.Sum32()%uint32(f.count)) == f.index
}

// add は担当分の画像を登録して計算待ちに積む (呼び出し側はロック不要)。
func (f *sigFarm) add(key []byte, imageID string, high bool) {
	if len(key) != sha256.Size || imageID == "" || !f.mine(imageID) {
		return
	}
	var it sigItem
	copy(it.key[:], key)
	it.imageID = imageID
	f.mu.Lock()
	if _, ok := f.lastExp[it]; !ok {
		f.lastExp[it] = 0
	}
	if !f.queued[it] {
		f.queued[it] = true
		if high {
			f.high = append(f.high, it)
		} else {
			f.low = append(f.low, it)
		}
		f.cond.Broadcast()
	}
	f.mu.Unlock()
}

func (f *sigFarm) worker() {
	for {
		f.mu.Lock()
		for len(f.high) == 0 && len(f.low) == 0 {
			f.cond.Wait()
		}
		var it sigItem
		if len(f.high) > 0 {
			it, f.high = f.high[0], f.high[1:]
		} else {
			it, f.low = f.low[0], f.low[1:]
		}
		delete(f.queued, it)
		done := f.lastExp[it]
		f.mu.Unlock()

		cur := currentImageExp(time.Now())
		var produced []sigEntry
		for _, exp := range []int64{cur, cur + imageExpStep} {
			if exp <= done {
				continue
			}
			sig := computeImageSignature(it.key[:], it.imageID, exp)
			storeImageSignature(it.key[:], it.imageID, exp, sig)
			produced = append(produced, sigEntry{Key: append([]byte(nil), it.key[:]...), ImageID: it.imageID, Exp: exp, Sig: sig})
		}
		f.mu.Lock()
		if f.lastExp[it] < cur+imageExpStep {
			f.lastExp[it] = cur + imageExpStep
		}
		f.out = append(f.out, produced...)
		f.mu.Unlock()
	}
}

// roller: exp 境界が進んだら登録済みの全画像を次の境界向けに積み直す。
func (f *sigFarm) roller() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		cur := currentImageExp(time.Now())
		f.mu.Lock()
		if cur != f.curExp {
			f.curExp = cur
			for it, done := range f.lastExp {
				if done < cur+imageExpStep && !f.queued[it] {
					f.queued[it] = true
					f.low = append(f.low, it)
				}
			}
			f.cond.Broadcast()
		}
		f.mu.Unlock()
	}
}

func (f *sigFarm) flusher() {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		f.mu.Lock()
		out := f.out
		f.out = nil
		f.mu.Unlock()
		for len(out) > 0 {
			n := min(len(out), 2000)
			batch := out[:n]
			out = out[n:]
			var wg sync.WaitGroup
			for _, target := range f.targets {
				wg.Add(1)
				go func(target string) {
					defer wg.Done()
					if _, err := memHTTPPost(target, "/internal/mem/sigs", batch, 5*time.Second); err != nil {
						slog.Debug("mem: sig push failed", "target", target, "error", err)
					}
				}(target)
			}
			wg.Wait()
		}
	}
}

// ---- sources ----

// onReset: ダンプから作り直した直後。全ユーザーのプロフ画像・ホット投稿/RealMoji とログイン中ユーザーの Memories を積む。
func (f *sigFarm) onReset() {
	type kv struct {
		key []byte
		id  string
	}
	var items []kv
	var sessionUsers []string
	f.m.mu.RLock()
	d := f.m.d
	for _, u := range d.users {
		if u.active() && u.ProfileImageID != nil {
			items = append(items, kv{u.ImageKey, *u.ProfileImageID})
		}
	}
	for _, p := range d.posts {
		if !p.deleted {
			items = append(items, kv{p.author.ImageKey, p.ImageID})
		}
		for _, r := range p.realmojis {
			items = append(items, kv{r.reactor.ImageKey, r.ImageID})
		}
	}
	for _, s := range d.sessions {
		sessionUsers = append(sessionUsers, s.UserID)
	}
	f.m.mu.RUnlock()
	for _, it := range items {
		f.add(it.key, it.id, false)
	}
	for _, u := range sessionUsers {
		f.enqueuePrefetch(u)
	}
}

// onApplied: changelog バッチ適用後。新しい画像は優先、ログインしたユーザーは Memories 先読み。
func (f *sigFarm) onApplied(changes []memChange) {
	for _, ch := range changes {
		if ch.Op == "D" {
			continue
		}
		switch ch.Tbl {
		case "posts":
			var r memPostRow
			if json.Unmarshal(ch.Row, &r) == nil && r.DeletedAt == nil {
				f.add(f.userKey(r.AuthorID), r.ImageID, true)
			}
		case "realmojis":
			var r memRealMojiRow
			if json.Unmarshal(ch.Row, &r) == nil && r.DeletedAt == nil && r.ReplacedAt == nil {
				f.add(f.userKey(r.ReactorID), r.ImageID, true)
			}
		case "users":
			var r memUserRow
			if json.Unmarshal(ch.Row, &r) == nil && r.ProfileImageID != nil {
				if key, err := imageSigningKey(r.SecretKeyHash); err == nil {
					f.add(key, *r.ProfileImageID, true)
				}
			}
		case "login_sessions":
			var r memSessionRow
			if json.Unmarshal(ch.Row, &r) == nil && r.Active {
				f.enqueuePrefetch(r.UserID)
			}
		}
	}
}

func (f *sigFarm) userKey(id string) []byte {
	f.m.mu.RLock()
	defer f.m.mu.RUnlock()
	if u := f.m.d.users[id]; u != nil {
		return u.ImageKey
	}
	return nil
}

func (f *sigFarm) enqueuePrefetch(userID string) {
	f.mu.Lock()
	if !f.prefDone[userID] {
		f.prefDone[userID] = true
		f.prefetch = append(f.prefetch, userID)
		f.cond.Broadcast()
	}
	f.mu.Unlock()
}

// prefetcher: ログインしたユーザーの全投稿画像と、その投稿の有効な RealMoji 画像を primary から読み、低優先で積む。
func (f *sigFarm) prefetcher() {
	for {
		f.mu.Lock()
		for len(f.prefetch) == 0 {
			f.cond.Wait()
		}
		userID := f.prefetch[0]
		f.prefetch = f.prefetch[1:]
		f.mu.Unlock()
		if err := f.prefetchUser(userID); err != nil {
			slog.Debug("mem: sig prefetch failed", "user", userID, "error", err)
			f.mu.Lock()
			delete(f.prefDone, userID)
			f.mu.Unlock()
			time.Sleep(time.Second)
		}
	}
}

func (f *sigFarm) prefetchUser(userID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var posts []string
	if err := f.db.SelectContext(ctx, &posts, `SELECT image_id FROM posts WHERE author_id = $1 AND deleted_at IS NULL`, userID); err != nil {
		return err
	}
	var reactions []struct {
		ImageID   string `db:"image_id"`
		ReactorID string `db:"reactor_id"`
	}
	if err := f.db.SelectContext(ctx, &reactions, `
  SELECT r.image_id, r.reactor_id FROM realmojis r JOIN posts p ON p.id = r.post_id
  WHERE p.author_id = $1 AND p.deleted_at IS NULL AND r.replaced_at IS NULL AND r.deleted_at IS NULL`, userID); err != nil {
		return err
	}
	key := f.userKey(userID)
	for _, id := range posts {
		f.add(key, id, false)
	}
	for _, r := range reactions {
		f.add(f.userKey(r.ReactorID), r.ImageID, false)
	}
	return nil
}

// ---- receiving side (all hosts) ----

func storeImageSignature(key []byte, imageID string, exp int64, sig []byte) {
	var ck signatureCacheKey
	copy(ck.key[:], key)
	ck.imageID, ck.exp = imageID, exp
	signatureCacheMu.Lock()
	if len(signatureCache) >= signatureCacheLimit*3/4 {
		pruneSignatureCacheLocked()
	}
	signatureCache[ck] = sig
	signatureCacheMu.Unlock()
}

// 期限切れ (exp <= now) の署名は検証にも使われないので捨てる。
func pruneSignatureCacheLocked() {
	now := time.Now().Unix()
	for k := range signatureCache {
		if k.exp <= now {
			delete(signatureCache, k)
		}
	}
}

func registerMemSigRoute(e *echo.Echo) {
	e.POST("/internal/mem/sigs", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		var entries []sigEntry
		if err := json.NewDecoder(c.Request().Body).Decode(&entries); err != nil {
			return c.NoContent(http.StatusBadRequest)
		}
		now := time.Now().Unix()
		signatureCacheMu.Lock()
		if len(signatureCache)+len(entries) >= signatureCacheLimit*3/4 {
			pruneSignatureCacheLocked()
		}
		for _, en := range entries {
			if len(en.Key) != sha256.Size || len(en.Sig) != sha256.Size || en.Exp <= now {
				continue
			}
			var ck signatureCacheKey
			copy(ck.key[:], en.Key)
			ck.imageID, ck.exp = en.ImageID, en.Exp
			if _, ok := signatureCache[ck]; !ok {
				signatureCache[ck] = en.Sig
			}
		}
		signatureCacheMu.Unlock()
		return c.JSON(http.StatusOK, memPushResponse{OK: true})
	})
}
