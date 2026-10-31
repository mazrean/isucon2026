//go:build memdiff

package isureal

// 論理レプリケーション版メモリモデルの差分テスト (wal_level=logical のローカル PG 用; DB を initialize する)。
//   ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15533 ISUREAL_LOG_LEVEL=error go test -tags memdiff -run TestLogReplDiff -v .
// DB 経路 (sa) と logrepl の 2 台 (sb = lr_b: 書き込み担当, sc = lr_c) の GET 応答を比較する。
// step 1000 で lr_c を kill -9 相当で止め、ローカルのスナップショット + スロットの続きから復帰させる。

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

type lrSwap struct {
	mu sync.RWMutex
	h  http.Handler
}

func (s *lrSwap) set(h http.Handler) { s.mu.Lock(); s.h = h; s.mu.Unlock() }
func (s *lrSwap) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	h := s.h
	s.mu.RUnlock()
	h.ServeHTTP(w, r)
}

func lrAdmin(t *testing.T, config *Config) *sqlx.DB {
	db, err := sqlx.Open("pgx", config.DatabaseDSN())
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// lrResetLocal: 前回のスロット・同期設定を消す。
func lrResetLocal(t *testing.T, config *Config, slots ...string) {
	db := lrAdmin(t, config)
	defer db.Close()
	db.MustExec(`ALTER SYSTEM SET synchronous_standby_names = ''`)
	db.MustExec(`SELECT pg_reload_conf()`)
	for _, s := range slots {
		db.Exec(`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL`, s)
		time.Sleep(200 * time.Millisecond)
		db.Exec(`SELECT pg_drop_replication_slot($1)`, s)
	}
}

// lrSetSync は watchdog の代わり: 同期対象を names にして、blocked を更新する。
func lrSetSync(t *testing.T, config *Config, names ...string) {
	db := lrAdmin(t, config)
	defer db.Close()
	db.MustExec(`SET synchronous_commit = local`)
	db.MustExec(`UPDATE ` + lrMembersTable + ` SET blocked = true WHERE NOT (name = ANY($1))`, names)
	ssn := ""
	if len(names) > 0 {
		ssn = fmt.Sprintf("ANY %d (%s)", len(names), strings.Join(names, ", "))
	}
	db.MustExec(`ALTER SYSTEM SET synchronous_standby_names = '` + ssn + `'`)
	db.MustExec(`SELECT pg_reload_conf()`)
	time.Sleep(300 * time.Millisecond)
	for _, n := range names {
		db.MustExec(`INSERT INTO `+lrMembersTable+` (name, blocked) VALUES ($1, false) ON CONFLICT (name) DO UPDATE SET blocked = false`, n)
	}
}

func lrWaitReady(t *testing.T, rs ...*memLogRepl) {
	deadline := time.Now().Add(60 * time.Second)
	for _, r := range rs {
		for !r.m.on() {
			if time.Now().After(deadline) {
				t.Fatalf("%s not ready (applied=%d insync=%v)", r.slot, r.applied.Load(), r.insync.Load())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func TestLogReplDiff(t *testing.T) {
	config := ConfigFromEnv()
	memEnabled = false
	memLogReplEnabled = false
	sa := httptest.NewServer(NewServer(config))
	defer sa.Close()
	dir := t.TempDir()
	os.Setenv("ISUREAL_MEM_LOGREPL_DIR", dir)
	lrResetLocal(t, config, "lr_b", "lr_c")
	memLogReplEnabled = true
	memCurrentLogRepl = nil
	os.Setenv("ISUREAL_MEM_LOGREPL_SLOT", "lr_b")
	sb := httptest.NewServer(NewServer(config))
	defer sb.Close()
	os.Setenv("ISUREAL_MEM_LOGREPL_SLOT", "lr_c")
	scHandler := &lrSwap{}
	scHandler.set(NewServer(config))
	sc := httptest.NewServer(scHandler)
	defer sc.Close()
	lrB, lrC := memCurrentLogRepl[0], memCurrentLogRepl[1]
	lrSetSync(t, config, "lr_b", "lr_c")
	lrWaitReady(t, lrB, lrC)

	pb := &mdClient{t: t, base: sb.URL}
	time.Sleep(time.Second)
	initStart := time.Now()
	if code, body := pb.json("POST", "/api/initialize", map[string]any{}); code != 200 {
		t.Fatalf("initialize: %d %s", code, body)
	}
	t.Logf("initialize took %v (lr ready=%v/%v)", time.Since(initStart), lrB.m.on(), lrC.m.on())
	admin := &mdClient{t: t, base: sb.URL}
	_, body := admin.json("POST", "/api/admin/auth/login", map[string]string{"username": "admin", "password": "password1234"})
	var al struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(body, &al)
	admin.token = al.Token

	day := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	openAll := func() {
		for {
			ok := true
			for _, rg := range []string{"asia", "europe", "america"} {
				code, b := admin.json("POST", "/api/admin/posting-window", map[string]any{"region": rg, "localDate": day.Format(time.DateOnly), "onTimeSeconds": 2})
				if code == 409 {
					ok = false
					break
				}
				if code != 201 {
					t.Fatalf("open window: %d %s", code, b)
				}
			}
			day = day.AddDate(0, 0, 1)
			if ok {
				return
			}
		}
	}
	openAll()

	var users []*mdUser
	login := func(u *mdUser) {
		c := &mdClient{t: t, base: sb.URL}
		code, b := c.json("POST", "/api/auth/login", map[string]string{"username": u.name, "secretKey": u.secret})
		if code != 200 {
			t.Fatalf("login %s: %d %s", u.name, code, b)
		}
		var lr struct {
			Token string `json:"token"`
			User  struct {
				ID string `json:"id"`
			} `json:"user"`
		}
		_ = json.Unmarshal(b, &lr)
		u.id = lr.User.ID
		u.a = &mdClient{t: t, base: sa.URL, token: lr.Token}
		u.b = &mdClient{t: t, base: sb.URL, token: lr.Token}
		u.c = &mdClient{t: t, base: sc.URL, token: lr.Token}
	}
	users = append(users, &mdUser{name: "yuma_kobayashi_186", secret: "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs"})
	login(users[0])
	run := time.Now().UnixNano() % 100000
	for i := 0; i < 24; i++ {
		region := "asia"
		if i%8 == 7 {
			region = "europe"
		}
		name := fmt.Sprintf("md%d_%d", run, i)
		c := &mdClient{t: t, base: sb.URL}
		code, b := c.json("POST", "/api/auth/register", map[string]string{"username": name, "displayName": "Disp " + name, "region": region})
		if code != 201 && code != 200 {
			t.Fatalf("register: %d %s", code, b)
		}
		var rr struct {
			SecretKey string `json:"secretKey"`
		}
		_ = json.Unmarshal(b, &rr)
		u := &mdUser{name: name, secret: rr.SecretKey}
		login(u)
		users = append(users, u)
	}

	var postIDs, commentIDs, realmojiIDs, requestIDs []string
	rng := rand.New(rand.NewSource(1))
	pick := func() *mdUser { return users[rng.Intn(len(users))] }
	pickID := func(list []string) string {
		if len(list) == 0 {
			return "00000000-0000-0000-0000-000000000000"
		}
		return list[rng.Intn(len(list))]
	}
	recent := func(list []string) string {
		if len(list) > 12 {
			list = list[len(list)-12:]
		}
		return pickID(list)
	}
	vis := []string{"public", "friends", "friends_of_friends"}

	mismatches := 0
	notOn := 0
	seen := map[string]int{}
	compare := func(step int, u *mdUser, path string) {
		ca, ba := u.a.do("GET", path, nil, "")
		cb, bb := u.b.do("GET", path, nil, "")
		if !lrC.m.on() || !lrB.m.on() {
			notOn++
		}
		if step%2 == 1 {
			cb, bb = u.c.do("GET", path, nil, "") // peer
		}
		na, nb := mdNorm(ba), mdNorm(bb)
		if ca != cb || na != nb {
			mismatches++
			key := strings.SplitN(path, "?", 2)[0]
			if len(key) > 20 && strings.HasPrefix(key, "/api/posts/") {
				key = "/api/posts/X" + key[len("/api/posts/")+36:]
			}
			if len(key) > 20 && strings.HasPrefix(key, "/api/users/") {
				key = "/api/users/X"
			}
			if seen[key] < 2 {
				seen[key]++
				i := 0
				for i < len(na) && i < len(nb) && na[i] == nb[i] {
					i++
				}
				lo := max(0, i-200)
				t.Errorf("step %d user %s GET %s\n DB : %d ...%s\n MEM: %d ...%s", step, u.name, path, ca, trunc([]byte(na[lo:])), cb, trunc([]byte(nb[lo:])))
			}
		}
	}
	check := func(step int) {
		for _, u := range users {
			for _, p := range []string{"/api/me", "/api/posting-window/current", "/api/posts/me/current",
				"/api/feeds/friends", "/api/feeds/public", "/api/feeds/following", "/api/feeds/friends-of-friends",
				"/api/relationships/friends", "/api/relationships/following", "/api/relationships/followers",
				"/api/relationships/incoming-requests", "/api/relationships/outgoing-requests",
				"/api/users?q=md", "/api/users?q=a", "/api/users/" + pick().id} {
				compare(step, u, p)
			}
			for i := 0; i < 3; i++ {
				id := pickID(postIDs)
				compare(step, u, "/api/posts/"+id)
				compare(step, u, "/api/posts/"+id+"/comments")
				compare(step, u, "/api/posts/"+id+"/realmojis")
			}
		}
		// pagination on public feed
		u := users[rng.Intn(len(users))]
		_, b := u.b.do("GET", "/api/feeds/public", nil, "")
		var f struct {
			Items []struct {
				ID       string    `json:"id"`
				PostedAt time.Time `json:"postedAt"`
				Score    *int64    `json:"score"`
			} `json:"items"`
		}
		_ = json.Unmarshal(b, &f)
		if len(f.Items) > 2 {
			it := f.Items[len(f.Items)/2]
			compare(step, u, fmt.Sprintf("/api/feeds/public?beforePostedAt=%s&beforeId=%s", it.PostedAt.Format(time.RFC3339Nano), it.ID))
		}
		for _, kind := range []string{"friends", "following", "followers", "incoming-requests"} {
			_, rb := u.b.do("GET", "/api/relationships/"+kind, nil, "")
			var rl struct {
				Items []struct {
					RelationID string    `json:"relationId"`
					UpdatedAt  time.Time `json:"updatedAt"`
				} `json:"items"`
			}
			_ = json.Unmarshal(rb, &rl)
			if len(rl.Items) >= 2 {
				compare(step, u, fmt.Sprintf("/api/relationships/%s?beforeUpdatedAt=%s&beforeId=%s", kind, rl.Items[0].UpdatedAt.Format(time.RFC3339Nano), rl.Items[0].RelationID))
			}
		}
		for _, id := range postIDs[max(0, len(postIDs)-8):] {
			_, cb := u.b.do("GET", "/api/posts/"+id+"/comments", nil, "")
			var cl struct {
				Items []struct {
					ID        string    `json:"id"`
					CreatedAt time.Time `json:"createdAt"`
				} `json:"items"`
			}
			_ = json.Unmarshal(cb, &cl)
			if len(cl.Items) >= 2 {
				compare(step, u, fmt.Sprintf("/api/posts/%s/comments?afterCreatedAt=%s&afterId=%s", id, cl.Items[0].CreatedAt.Format(time.RFC3339Nano), cl.Items[0].ID))
			}
			_, xb := u.b.do("GET", "/api/posts/"+id+"/realmojis", nil, "")
			_ = json.Unmarshal(xb, &cl)
			if len(cl.Items) >= 2 {
				compare(step, u, fmt.Sprintf("/api/posts/%s/realmojis?beforeCreatedAt=%s&beforeId=%s", id, cl.Items[0].CreatedAt.Format(time.RFC3339Nano), cl.Items[0].ID))
			}
		}
		compare(step, u, "/api/users?q=md&beforeUsername="+users[3].name+"&beforeId="+users[3].id)
		_, b = u.b.do("GET", "/api/feeds/friends-of-friends", nil, "")
		_ = json.Unmarshal(b, &f)
		if len(f.Items) > 2 && f.Items[1].Score != nil {
			it := f.Items[1]
			compare(step, u, fmt.Sprintf("/api/feeds/friends-of-friends?beforeScore=%d&beforePostedAt=%s&beforeId=%s", *it.Score, it.PostedAt.Format(time.RFC3339Nano), it.ID))
		}
	}

	extract := func(b []byte, key string) string {
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if v, ok := m[key].(string); ok {
			return v
		}
		return ""
	}

	// peer の同期を待つ
	for i := 0; i < 100; i++ {
		if code, _ := users[0].c.do("GET", "/internal/mem/none", nil, ""); code != 0 {
		}
		time.Sleep(50 * time.Millisecond)
	}
	check(0)
	for step := 1; step <= 1500; step++ {
		u := pick()
		other := pick()
		if step%300 == 0 {
			openAll()
		}
		if step == 1000 {
			// kill -9 相当: lr_c を同期対象から外して止め、ローカルのスナップショットから作り直す
			lrSetSync(t, config, "lr_b")
			lrC.stop()
			lrC.saving.Store(false)
			killAt := time.Now()
			memCurrentLogRepl = nil
			scHandler.set(NewServer(config))
			lrC = memCurrentLogRepl[0]
			lrSetSync(t, config, "lr_b", "lr_c")
			lrWaitReady(t, lrC)
			t.Logf("lr_c restart -> ready in %v (restored=%v)", time.Since(killAt), lrC.saved.Load() > 0)
			check(step)
		}
		switch op := rng.Intn(27); op {
		case 0, 1:
			fields := map[string]string{"caption": fmt.Sprintf("c%d", step), "visibility": vis[rng.Intn(3)], "location_kind": "none"}
			switch rng.Intn(3) {
			case 1:
				fields["location_kind"] = "exact"
				fields["latitude"] = fmt.Sprintf("%.6f", rng.Float64()*170-85)
				fields["longitude"] = fmt.Sprintf("%.7f", rng.Float64()*350-175)
			case 2:
				fields["location_kind"] = "approximate"
				fields["locality"] = "渋谷区"
			}
			code, b := u.b.multipart("/api/posts", fields, mdJPEG(16, 16, step))
			if code == 201 {
				postIDs = append(postIDs, extract(b, "id"))
			}
		case 2:
			code, b := u.b.json("POST", "/api/posts/"+recent(postIDs)+"/comments", map[string]string{"body": fmt.Sprintf("hi%d", step)})
			if code == 201 {
				commentIDs = append(commentIDs, extract(b, "id"))
			}
		case 3:
			code, b := u.b.multipart("/api/posts/"+recent(postIDs)+"/realmojis", map[string]string{"reactionType": "smile"}, mdJPEG(8, 8, step))
			if code == 201 {
				realmojiIDs = append(realmojiIDs, extract(b, "id"))
			}
		case 4, 5:
			code, b := u.b.json("POST", "/api/friend-requests", map[string]string{"recipientId": other.id})
			if code == 201 || code == 200 {
				requestIDs = append(requestIDs, extract(b, "relationId"))
			}
		case 6:
			// 受信者側で承認を試す
			for _, x := range users {
				x.b.json("POST", "/api/friend-requests/"+pickID(requestIDs)+"/accept", nil)
			}
		case 7:
			u.b.json("PUT", "/api/follows/"+other.id, nil)
		case 8:
			u.b.json("DELETE", "/api/follows/"+other.id, nil)
		case 9:
			if rng.Intn(3) == 0 {
				u.b.json("PUT", "/api/blocks/"+other.id, nil)
			} else {
				u.b.json("DELETE", "/api/blocks/"+other.id, nil)
			}
		case 10:
			if rng.Intn(3) == 0 {
				u.b.json("DELETE", "/api/friendships/"+other.id, nil)
			}
		case 11:
			u.b.json("PATCH", "/api/posts/"+pickID(postIDs), map[string]string{"visibility": vis[rng.Intn(3)]})
		case 12:
			if rng.Intn(3) == 0 {
				u.b.json("DELETE", "/api/posts/"+pickID(postIDs), nil)
			}
		case 13:
			if rng.Intn(2) == 0 {
				u.b.json("DELETE", "/api/comments/"+pickID(commentIDs), nil)
			} else {
				u.b.json("DELETE", "/api/realmojis/"+pickID(realmojiIDs), nil)
			}
		case 14:
			u.b.json("PATCH", "/api/me", map[string]string{"bio": fmt.Sprintf("bio %d", step), "displayName": fmt.Sprintf("D%d", step)})
		case 15:
			if rng.Intn(4) == 0 {
				admin.json("PUT", "/api/admin/users/"+u.id+"/certification", nil)
			} else {
				u.b.json("POST", "/api/auth/logout", nil)
				login(u)
			}
		case 16:
			if step%5 == 0 {
				u.b.json("PATCH", "/api/me", map[string]string{"region": []string{"asia", "asia", "europe"}[rng.Intn(3)]})
			}
		case 17, 18, 19:
			code, b := u.b.json("POST", "/api/posts/"+recent(postIDs)+"/comments", map[string]string{"body": fmt.Sprintf("hi%d", step)})
			if code == 201 {
				commentIDs = append(commentIDs, extract(b, "id"))
			}
		case 20, 21:
			code, b := u.b.multipart("/api/posts/"+recent(postIDs)+"/realmojis", map[string]string{"reactionType": "smile"}, mdJPEG(8, 8, step))
			if code == 201 {
				realmojiIDs = append(realmojiIDs, extract(b, "id"))
			}
		case 24:
			if rng.Intn(2) == 0 {
				u.b.do("PUT", "/api/me/profile-image", bytes.NewReader(mdJPEG(32, 32, step)), "image/jpeg")
			} else {
				u.b.json("DELETE", "/api/me/profile-image", nil)
			}
		case 25:
			act := []string{"reject", "cancel"}[rng.Intn(2)]
			for _, x := range users {
				x.b.json("POST", "/api/friend-requests/"+pickID(requestIDs)+"/"+act, nil)
			}
		case 26:
			if step%200 == 0 && u != users[0] {
				u.b.json("POST", "/api/me/account-deletion", nil)
			}
		case 22, 23:
			code, b := u.b.json("POST", "/api/friend-requests", map[string]string{"recipientId": other.id})
			if code == 201 || code == 200 {
				requestIDs = append(requestIDs, extract(b, "relationId"))
			}
			other.b.json("POST", "/api/friend-requests", map[string]string{"recipientId": u.id})
		}
		if step%49 == 0 || step%50 == 0 || step < 20 {
			check(step)
		}
	}
	check(1501)
	t.Logf("posts=%d comments=%d realmojis=%d mismatches=%d notOn=%d", len(postIDs), len(commentIDs), len(realmojiIDs), mismatches, notOn)
}

