//go:build memdiff

package isureal

// kill -9 → 再起動で同一応答になること、initialize で初期状態に戻ることを確認する (ローカル PG 用)。
//   ISUREAL_DB_NAME=isureal_store ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error \
//     go test -tags memdiff -run TestStoreCrash -v .

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

// TestStoreHelperServer は子プロセスとして store サーバーを起動する (STORE_HELPER_ADDR があるときだけ)。
func TestStoreHelperServer(t *testing.T) {
	addr := os.Getenv("STORE_HELPER_ADDR")
	if addr == "" {
		t.Skip("helper")
	}
	memEnabled = true
	storeEnabled = true
	srv := NewServer(ConfigFromEnv())
	_ = http.ListenAndServe(addr, srv)
}

func startStoreHelper(t *testing.T, addr string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestStoreHelperServer$")
	cmd.Env = append(append(os.Environ(), "STORE_HELPER_ADDR="+addr, "ISUREAL_MEM_PEERS="), storeHelperEnv...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// ready = メモリがロード済み (store が処理できる)。/api/posting-window/current は未ログインで 401 だが待つ指標にはなる
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get("http://" + addr + "/api/posting-window/current"); err == nil {
			resp.Body.Close()
			time.Sleep(1500 * time.Millisecond) // 初回ロード (pump ticker)
			return cmd
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("helper did not start")
	return nil
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

var storeHelperEnv []string

// TestStoreCrashWAL: ISUREAL_STORE_WAL + AWAIT_PG=0 + 書き戻しを遅らせて、WAL の fsync 後・PG の書き戻し前に kill -9。
// 応答済みの書き込みは、再起動時の WAL の流し直しで PG に入っていること。
func TestStoreCrashWAL(t *testing.T) {
	dir := t.TempDir()
	storeHelperEnv = []string{"ISUREAL_STORE_WAL=" + dir, "ISUREAL_STORE_AWAIT_PG=0", "ISUREAL_STORE_WB_DELAY_MS=3000"}
	defer func() { storeHelperEnv = nil }()
	storeCrashBody(t, func(comments [][2]string) {
		db, err := sqlx.Open("pgx", ConfigFromEnv().DatabaseDSN())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		missing := 0
		for _, c := range comments {
			var n int
			if err := db.Get(&n, `SELECT count(*) FROM comments WHERE id = $1`, c[1]); err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				missing++
			}
		}
		st, _ := os.Stat(walPath(dir))
		var sz int64
		if st != nil {
			sz = st.Size()
		}
		t.Logf("before restart: acked comments not yet in PG = %d / %d, wal bytes = %d", missing, len(comments), sz)
		if missing == 0 {
			t.Errorf("test did not exercise the WAL (every acked comment already in PG)")
		}
	})
}

func TestStoreCrash(t *testing.T) { storeCrashBody(t, nil) }

func storeCrashBody(t *testing.T, afterKill func(comments [][2]string)) {
	addr := freeAddr(t)
	base := "http://" + addr
	p1 := startStoreHelper(t, addr)
	defer func() { _ = p1.Process.Kill() }()

	root := &mdClient{t: t, base: base}
	if code, b := root.json("POST", "/api/initialize", map[string]any{}); code != 200 {
		t.Fatalf("initialize: %d %s", code, b)
	}
	yuma := &mdClient{t: t, base: base}
	loginAs := func(c *mdClient, name, secret string) {
		code, b := c.json("POST", "/api/auth/login", map[string]string{"username": name, "secretKey": secret})
		if code != 200 {
			t.Fatalf("login %s: %d %s", name, code, b)
		}
		var lr struct {
			Token string `json:"token"`
		}
		_ = json.Unmarshal(b, &lr)
		c.token = lr.Token
	}
	loginAs(yuma, "yuma_kobayashi_186", "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs")
	_, initialMe := yuma.do("GET", "/api/me", nil, "")
	_, initialFriends := yuma.do("GET", "/api/relationships/friends", nil, "")

	admin := &mdClient{t: t, base: base}
	_, b := admin.json("POST", "/api/admin/auth/login", map[string]string{"username": "admin", "password": "password1234"})
	var al struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(b, &al)
	admin.token = al.Token
	day := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, rg := range []string{"asia", "europe", "america"} {
		for {
			code, _ := admin.json("POST", "/api/admin/posting-window", map[string]any{"region": rg, "localDate": day.Format(time.DateOnly), "onTimeSeconds": 600})
			if code == 201 {
				break
			}
			day = day.AddDate(0, 0, 1)
		}
	}

	type user struct {
		id string
		c  *mdClient
	}
	var users []*user
	run := time.Now().UnixNano() % 100000
	for i := 0; i < 16; i++ {
		c := &mdClient{t: t, base: base}
		name := fmt.Sprintf("sc%d_%d", run, i)
		code, b := c.json("POST", "/api/auth/register", map[string]string{"username": name, "displayName": "D " + name, "region": "asia"})
		if code != 201 {
			t.Fatalf("register: %d %s", code, b)
		}
		var rr struct {
			SecretKey string `json:"secretKey"`
			User      struct {
				ID string `json:"id"`
			} `json:"user"`
		}
		_ = json.Unmarshal(b, &rr)
		loginAs(c, name, rr.SecretKey)
		users = append(users, &user{id: rr.User.ID, c: c})
	}
	users = append(users, &user{c: yuma})
	var postIDs []string
	var postAuthors []*mdClient
	var ackedComments [][2]string
	var ackedCommenters []*mdClient
	var mu sync.Mutex
	rng := rand.New(rand.NewSource(7))
	writeWith := func(rng *rand.Rand, wrap func(*mdClient) *mdClient) {
		u0 := users[rng.Intn(len(users))]
		o0 := users[rng.Intn(len(users))]
		u := &user{id: u0.id, c: wrap(u0.c)}
		o := &user{id: o0.id, c: wrap(o0.c)}
		switch rng.Intn(6) {
		case 0:
			code, b := u.c.multipart("/api/posts", map[string]string{"caption": "x", "visibility": "public", "location_kind": "none"}, mdJPEG(16, 16, rng.Intn(200)))
			if code == 201 {
				var p struct {
					ID string `json:"id"`
				}
				_ = json.Unmarshal(b, &p)
				mu.Lock()
				postIDs = append(postIDs, p.ID)
				postAuthors = append(postAuthors, u0.c)
				mu.Unlock()
			}
		case 1:
			mu.Lock()
			id := "00000000-0000-0000-0000-000000000000"
			if len(postIDs) > 0 {
				id = postIDs[rng.Intn(len(postIDs))]
			}
			mu.Unlock()
			if code, b := u.c.json("POST", "/api/posts/"+id+"/comments", map[string]string{"body": "hi"}); code == 201 {
				var cm struct {
					ID string `json:"id"`
				}
				_ = json.Unmarshal(b, &cm)
				mu.Lock()
				ackedComments = append(ackedComments, [2]string{id, cm.ID})
				ackedCommenters = append(ackedCommenters, u0.c)
				mu.Unlock()
			}
		case 2:
			mu.Lock()
			id := "00000000-0000-0000-0000-000000000000"
			if len(postIDs) > 0 {
				id = postIDs[rng.Intn(len(postIDs))]
			}
			mu.Unlock()
			u.c.multipart("/api/posts/"+id+"/realmojis", map[string]string{"reactionType": "smile"}, mdJPEG(8, 8, rng.Intn(200)))
		case 3, 4:
			if o.id != "" {
				u.c.json("POST", "/api/friend-requests", map[string]string{"recipientId": o.id})
				o.c.json("POST", "/api/friend-requests", map[string]string{"recipientId": u.id})
			}
		case 5:
			u.c.json("PATCH", "/api/me", map[string]string{"bio": fmt.Sprintf("bio %d", rng.Intn(1000))})
		}
	}
	write := func(rng *rand.Rand) { writeWith(rng, func(c *mdClient) *mdClient { return c }) }
	// kill 中の書き込みは接続エラーになるので、Fatalf しないクライアント (panic → recover) を使う
	chaos := func(c *mdClient) *mdClient { return &mdClient{t: new(testing.T), base: c.base, token: c.token} }
	for i := 0; i < 300; i++ {
		write(rng)
	}
	snapshot := func() map[string]string {
		out := map[string]string{}
		for i, u := range users {
			for _, p := range []string{"/api/me", "/api/posts/me/current", "/api/feeds/friends", "/api/feeds/public",
				"/api/relationships/friends", "/api/relationships/incoming-requests", "/api/relationships/outgoing-requests"} {
				code, b := u.c.do("GET", p, nil, "")
				out[fmt.Sprintf("%d %s", i, p)] = fmt.Sprintf("%d %s", code, mdNorm(b))
			}
		}
		for _, id := range postIDs {
			code, b := users[0].c.do("GET", "/api/posts/"+id+"/comments", nil, "")
			out["c "+id] = fmt.Sprintf("%d %s", code, mdNorm(b))
		}
		return out
	}
	before := snapshot()
	quiet := len(postIDs)

	// 書き込みを流しながら kill -9。kill 前に 2xx を返した書き込みは再起動後に残っていなければならない。
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				func() {
					defer func() { _ = recover() }()
					writeWith(r, chaos)
				}()
			}
		}(int64(w + 100))
	}
	time.Sleep(1500 * time.Millisecond)
	_ = p1.Process.Signal(syscall.SIGKILL)
	_, _ = p1.Process.Wait()
	close(stop)
	wg.Wait()
	if afterKill != nil {
		afterKill(ackedComments)
	}

	p2 := startStoreHelper(t, addr)
	defer func() { _ = p2.Process.Kill() }()
	// in-flight だった書き込みは結果がどちらでもよいので、ここでもう一度スナップショットを取り、
	// 再起動前の (静止状態の) スナップショットのキーのうち in-flight 書き込みの影響を受けないものだけでなく、
	// kill 後の 2 回目の再起動と完全一致することを確認する。
	after1 := snapshot()
	_ = p2.Process.Signal(syscall.SIGKILL)
	_, _ = p2.Process.Wait()
	p3 := startStoreHelper(t, addr)
	defer func() { _ = p3.Process.Kill() }()
	after2 := snapshot()
	diff := 0
	for k, v := range after1 {
		if after2[k] != v {
			diff++
			if diff < 5 {
				t.Errorf("restart mismatch %s\n 1st: %s\n 2nd: %s", k, trunc([]byte(v)), trunc([]byte(after2[k])))
			}
		}
	}
	// 静止状態のスナップショットにあった投稿・コメントは消えていないこと
	lost := 0
	for k, v := range before {
		if len(k) > 2 && k[:2] == "c " && after1[k][:3] != v[:3] {
			lost++
		}
	}
	// kill 中に 201 を返した投稿は残っていること
	for i := range postIDs {
		if code, b := postAuthors[i].do("GET", "/api/posts/"+postIDs[i], nil, ""); code != 200 {
			lost++
			t.Errorf("acked post %s lost after kill -9: %d %s", postIDs[i], code, trunc(b))
		}
	}
	allComments := func(c *mdClient, postID string) string {
		var all strings.Builder
		path := "/api/posts/" + postID + "/comments"
		for {
			_, b := c.do("GET", path, nil, "")
			all.Write(b)
			var cl struct {
				Items []struct {
					ID        string    `json:"id"`
					CreatedAt time.Time `json:"createdAt"`
				} `json:"items"`
				HasMore bool `json:"hasMore"`
			}
			_ = json.Unmarshal(b, &cl)
			if !cl.HasMore || len(cl.Items) == 0 {
				return all.String()
			}
			last := cl.Items[len(cl.Items)-1]
			path = fmt.Sprintf("/api/posts/%s/comments?afterCreatedAt=%s&afterId=%s", postID, url.QueryEscape(last.CreatedAt.Format(time.RFC3339Nano)), last.ID)
		}
	}
	for i, pc := range ackedComments {
		if b := allComments(ackedCommenters[i], pc[0]); !strings.Contains(b, pc[1]) {
			lost++
			if lost < 5 {
				t.Errorf("acked comment %s lost", pc[1])
			}
		}
	}
	t.Logf("acked comments=%d", len(ackedComments))
	t.Logf("snapshot keys=%d restart-diff=%d lost=%d posts=%d (acked during kill phase: %d)", len(after1), diff, lost, len(postIDs), len(postIDs)-quiet)
	if lost > 0 {
		t.Errorf("lost %d", lost)
	}

	// initialize で初期状態へ
	if code, b := root.json("POST", "/api/initialize", map[string]any{}); code != 200 {
		t.Fatalf("initialize: %d %s", code, b)
	}
	loginAs(yuma, "yuma_kobayashi_186", "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs")
	_, me := yuma.do("GET", "/api/me", nil, "")
	_, friends := yuma.do("GET", "/api/relationships/friends", nil, "")
	if mdNorm(me) != mdNorm(initialMe) || mdNorm(friends) != mdNorm(initialFriends) {
		t.Errorf("initialize did not restore initial state\n me: %s\n vs %s", trunc(me), trunc(initialMe))
	}
	if code, _ := users[0].c.do("GET", "/api/me", nil, ""); code != 401 {
		t.Errorf("registered user's session survived initialize: %d", code)
	}
}
