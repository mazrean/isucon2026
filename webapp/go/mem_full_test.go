//go:build memdiff

package isureal

// ISUREAL_MEM_FULL=1 の冷たい読みのノードの検証 (ローカル PG 用; DB を initialize する)。
//   ISUREAL_MEM_FULL=1 ISUREAL_DB_NAME=isureal_full ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error \
//     go test -tags memdiff -run TestMemFullCold -v .
// DB 経路 (sa) と全期間メモリの peer (sc) の応答を、ホット外 (過去月) の Memories・投稿・コメント・RealMoji・
// 利用者・関係で比べ、sc がその要求の中で PG に文を投げていないことを tracer で数える。

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

type mfProbeKey struct{}

func TestMemFullCold(t *testing.T) {
	if !memFull {
		t.Skip("ISUREAL_MEM_FULL=1 only")
	}
	config := ConfigFromEnv()
	memoriesMem = true
	memFullLoaders = false // pump (asia1) はホット集合だけ。peer がホット外を自分の PG から足す経路を通す
	defer func() { memoriesMem, memFullLoaders = false, memFull }()
	memEnabled = false
	sa := httptest.NewServer(NewServer(config))
	defer sa.Close()
	memEnabled = true
	forwardSecret = "test-secret"
	sb := httptest.NewUnstartedServer(nil)
	sc := httptest.NewUnstartedServer(nil)
	os.Setenv("ISUREAL_MEM_PEERS", sc.Listener.Addr().String())
	sb.Config.Handler = NewServer(config)
	os.Unsetenv("ISUREAL_MEM_PEERS")
	os.Setenv("ISUREAL_MEM_ROLE", "peer")
	os.Setenv("ISUREAL_MEM_PUMP", sb.Listener.Addr().String())
	dbTraceOn = true // sc のプールだけ tracer 付き (集計ループは 10 秒ごとに捨てるだけ)
	inner := NewServer(config)
	dbTraceOn = false
	os.Unsetenv("ISUREAL_MEM_ROLE")
	os.Unsetenv("ISUREAL_MEM_PUMP")
	var probeMu sync.Mutex
	probeSQL := map[string]int{}
	var probeN atomic.Int64
	dbTraceObserve = func(ctx context.Context, sql string) {
		if ctx.Value(mfProbeKey{}) == nil {
			return
		}
		probeN.Add(1)
		probeMu.Lock()
		probeSQL[dbTraceSQLKey(sql)]++
		probeMu.Unlock()
	}
	defer func() { dbTraceObserve = nil }()
	sc.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Mf-Probe") != "" {
			r = r.WithContext(context.WithValue(r.Context(), mfProbeKey{}, true))
		}
		inner.ServeHTTP(w, r)
	})
	sb.Start()
	sc.Start()
	defer sb.Close()
	defer sc.Close()
	sb.URL = "http://" + sb.Listener.Addr().String()
	sc.URL = "http://" + sc.Listener.Addr().String()

	pb := &mdClient{t: t, base: sb.URL}
	time.Sleep(time.Second)
	if code, body := pb.json("POST", "/api/initialize", map[string]any{}); code != 200 {
		t.Fatalf("initialize: %d %s", code, body)
	}
	for i := 0; i < 600 && !memCurrentPeerStore.on(); i++ {
		time.Sleep(100 * time.Millisecond)
	}
	if !memCurrentPeerStore.on() {
		t.Fatal("peer not ready")
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	memCurrentPeerStore.mu.RLock()
	d := memCurrentPeerStore.d
	nPosts, nC, nR := len(d.posts), 0, 0
	for _, p := range d.posts {
		nC += len(p.comments)
		nR += len(p.realmojis)
	}
	memCurrentPeerStore.mu.RUnlock()
	t.Logf("peer: posts=%d comments=%d realmojis=%d hotFrom=%v; process (DB 経路 + pump + peer の 3 台分) HeapInuse=%dMiB Sys=%dMiB",
		nPosts, nC, nR, d.hotFrom, ms.HeapInuse>>20, ms.Sys>>20)

	db, err := sqlx.Open("pgx", config.DatabaseDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var dbPosts, dbC, dbR int
	_ = db.Get(&dbPosts, `SELECT count(*) FROM posts`)
	_ = db.Get(&dbC, `SELECT count(*) FROM comments WHERE deleted_at IS NULL`)
	_ = db.Get(&dbR, `SELECT count(*) FROM realmojis WHERE deleted_at IS NULL AND replaced_at IS NULL`)
	t.Logf("db: posts=%d comments=%d realmojis=%d", dbPosts, dbC, dbR)
	if nPosts != dbPosts || nC != dbC || nR != dbR {
		t.Errorf("peer memory is not the full history")
	}

	// 過去の投稿を多く持つ利用者 (secret は初期データの固定値を持たないので、登録済みの利用者の token をセッション表から作れない
	// → 既知の利用者でログインし、その友達の古い投稿を見る)
	u := &mdUser{name: "yuma_kobayashi_186", secret: "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs"}
	c := &mdClient{t: t, base: sb.URL}
	code, b := c.json("POST", "/api/auth/login", map[string]string{"username": u.name, "secretKey": u.secret})
	if code != 200 {
		t.Fatalf("login: %d %s", code, b)
	}
	var lr struct {
		Token string `json:"token"`
		User  struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	_ = json.Unmarshal(b, &lr)
	u.id = lr.User.ID
	time.Sleep(300 * time.Millisecond) // セッションの push

	var months []string
	_ = db.Select(&months, `SELECT DISTINCT to_char(w.local_date, 'YYYY-MM') FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id WHERE p.author_id = $1 ORDER BY 1`, u.id)
	var dates []string
	_ = db.Select(&dates, `SELECT DISTINCT w.local_date::text FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id WHERE p.author_id = $1 ORDER BY 1 LIMIT 5`, u.id)
	var postIDs []string
	_ = db.Select(&postIDs, `SELECT p.id FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
	  WHERE p.author_id = $1 OR p.author_id IN (SELECT CASE WHEN user_low_id = $1 THEN user_high_id ELSE user_low_id END FROM friendships WHERE user_low_id = $1 OR user_high_id = $1)
	  ORDER BY w.local_date LIMIT 30`, u.id)
	var otherIDs []string
	_ = db.Select(&otherIDs, `SELECT id FROM users ORDER BY id LIMIT 5`)
	t.Logf("user %s: months=%d dates=%d posts=%d", u.id, len(months), len(dates), len(postIDs))

	paths := []string{"/api/me", "/api/relationships/friends", "/api/relationships/following", "/api/relationships/followers",
		"/api/relationships/incoming-requests", "/api/relationships/outgoing-requests", "/api/users?q=a"}
	for _, id := range otherIDs {
		paths = append(paths, "/api/users/"+id)
	}
	for _, m := range months {
		paths = append(paths, "/api/memories?month="+m)
	}
	for _, dt := range dates {
		paths = append(paths, "/api/memories/"+dt)
	}
	for _, id := range postIDs {
		paths = append(paths, "/api/posts/"+id, "/api/posts/"+id+"/comments", "/api/posts/"+id+"/realmojis")
	}
	paths = append(paths, "/api/me/export")

	get := func(base, path string, hdr map[string]string) (int, string, time.Duration) {
		req, _ := http.NewRequest("GET", base+path, nil)
		req.Header.Set("Authorization", "Bearer "+lr.Token)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		start := time.Now()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var buf []byte
		buf, _ = readAllMF(resp)
		return resp.StatusCode, mdNorm(buf), time.Since(start)
	}
	pumpSeq := func() int64 {
		ps := memCurrentPump.peers
		ps.mu.Lock()
		defer ps.mu.Unlock()
		return ps.seq
	}
	hop := func(space string, seq int64) map[string]string {
		return map[string]string{coldHopHeader: forwardSecret, coldWaitHeader: strconv.FormatInt(seq, 10), coldSpaceHeader: space, "X-Mf-Probe": "1"}
	}
	mismatch, pgPaths := 0, 0
	for _, p := range paths {
		ca, ba, _ := get(sa.URL, p, nil)
		before := probeN.Load()
		cc, bc, el := get(sc.URL, p, hop("pump", pumpSeq()))
		n := probeN.Load() - before
		if ca != cc || ba != bc {
			mismatch++
			if mismatch <= 5 {
				t.Errorf("mismatch %s: db %d %.200s / peer %d %.200s", p, ca, ba, cc, bc)
			}
		}
		if n > 0 {
			pgPaths++
			t.Logf("PG on peer: %s (%d stmts, %v)", p, n, el)
		}
	}
	probeMu.Lock()
	for k, v := range probeSQL {
		t.Logf("peer PG stmt: %d x %s", v, k)
	}
	probeMu.Unlock()
	t.Logf("cold paths=%d mismatches=%d paths-with-PG=%d", len(paths), mismatch, pgPaths)

	// 待ちの意味: 届かない seq は 2000ms 待って PG で答える。store の系列 (push の SS) も同じ。
	p := "/api/posts/" + postIDs[0]
	ca, ba, _ := get(sa.URL, p, nil)
	for _, space := range []string{"pump", "store"} {
		before := probeN.Load()
		cc, bc, el := get(sc.URL, p, hop(space, 1<<40))
		n := probeN.Load() - before
		t.Logf("unreachable %s seq: %v, PG stmts %d, same=%v", space, el, n, ca == cc && ba == bc)
		if el < memColdMaxWait || el > memColdMaxWait+time.Second || n == 0 || ca != cc || ba != bc {
			t.Errorf("unreachable %s seq: want ~2s wait then PG answer", space)
		}
	}
	before := probeN.Load()
	_, _, el := get(sc.URL, p, hop("store", 0))
	if el > 200*time.Millisecond || probeN.Load() != before {
		t.Errorf("store seq 0: %v, PG %d", el, probeN.Load()-before)
	}
	// 書き込み直後の読み: pump の seq を待ってからメモリで答える (RYW)
	c.token = lr.Token
	var own string
	_ = db.Get(&own, `SELECT id FROM posts WHERE author_id = $1 AND deleted_at IS NULL ORDER BY posted_at LIMIT 1`, u.id)
	code, b = c.json("POST", "/api/posts/"+own+"/comments", map[string]string{"body": "mf-ryw"})
	seq := pumpSeq()
	cc, bc, el := get(sc.URL, "/api/posts/"+own+"/comments", hop("pump", seq))
	ca, ba, _ = get(sa.URL, "/api/posts/"+own+"/comments", nil)
	t.Logf("ryw: comment %d, peer wait %v, same=%v, seen=%v", code, el, ca == cc && ba == bc, strings.Contains(bc, "mf-ryw"))
	if ca != cc || ba != bc || (code == 201 && !strings.Contains(bc, "mf-ryw")) {
		t.Errorf("ryw mismatch: %d %.300s / db %.200s / peer %.200s", code, b, ba, bc)
	}
}

func readAllMF(resp *http.Response) ([]byte, error) {
	var out []byte
	buf := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out, nil
		}
	}
}
