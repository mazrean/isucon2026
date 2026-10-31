//go:build memdiff

package isureal

// ISUREAL_MEM=0/1 の応答差分テスト (ローカル PG 用; 実行すると DB を initialize する)。
//   ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error go test -tags memdiff -run TestMemDiff -v .

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type mdClient struct {
	t     *testing.T
	base  string
	token string
}

func (c *mdClient) do(method, path string, body io.Reader, ctype string) (int, []byte) {
	req, _ := http.NewRequest(method, c.base+path, body)
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (c *mdClient) json(method, path string, v any) (int, []byte) {
	var r io.Reader
	if v != nil {
		b, _ := json.Marshal(v)
		r = bytes.NewReader(b)
	}
	return c.do(method, path, r, "application/json")
}

func mdJPEG(w, h int, seed int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for x := 0; x < w; x++ {
		for y := 0; y < h; y++ {
			img.Set(x, y, color.RGBA{uint8(x + seed), uint8(y), uint8(seed), 255})
		}
	}
	var buf bytes.Buffer
	_ = jpeg.Encode(&buf, img, nil)
	return buf.Bytes()
}

func (c *mdClient) multipart(path string, fields map[string]string, img []byte) (int, []byte) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for k, v := range fields {
		_ = w.WriteField(k, v)
	}
	fw, _ := w.CreateFormFile("image", "a.jpg")
	_, _ = fw.Write(img)
	w.Close()
	return c.do("POST", path, &buf, w.FormDataContentType())
}

var mdSigRe = regexp.MustCompile(`(/api/images/[^"?]+)\?[^"]*"`)

func mdNorm(b []byte) string { return mdSigRe.ReplaceAllString(string(b), `$1"`) }

type mdUser struct {
	id, name, secret string
	a, b, c          *mdClient
}

func TestMemDiff(t *testing.T) {
	config := ConfigFromEnv()
	memoriesMem = true
	// export も mem 経路 (export_mem.go) で答え、DB 経路 (sa) と比べる
	defer func(o bool) { exportMem = o }(exportMem)
	exportMem = os.Getenv("MD_EXPORT_MEM") != "0"
	defer func() {
		t.Logf("export from mem: %d served, %d fell (cold)", exportMemServed.Load(), exportMemFell.Load())
	}()
	// MD_RESP_CACHE=1: Memories/export go through mem_respcache.go instead of the mem path
	respCache := os.Getenv("MD_RESP_CACHE") == "1"
	// MD_HOT_EXTRA_DAYS=N: ISUREAL_MEM_HOT_EXTRA_DAYS (mem_hot_extend.go); past months inside the extended range come from memory
	if v, err := strconv.Atoi(os.Getenv("MD_HOT_EXTRA_DAYS")); err == nil && v > 0 {
		defer func(o int) { memHotExtraDays = o }(memHotExtraDays)
		memHotExtraDays = v
	}
	if respCache {
		memoriesMem, memRespCacheOn = false, true
		// MD_CACHE_FINE=1: ISUREAL_MEMCACHE_FINE (mem_respcache_fine.go)
		if os.Getenv("MD_CACHE_FINE") == "1" {
			defer func(o bool) { memCacheFine = o }(memCacheFine)
			memCacheFine = true
		}
		defer func() {
			memRespCacheOn = false
			t.Logf("resp cache: hits %d miss %d puts %d veto %d", memRespHits.Load(), memRespMiss.Load(), memRespPuts.Load(), memRespVeto.Load())
			for i := range rcWhyN {
				t.Logf("  invalidate %s: %d calls, %d removed", rcWhyName[i], memRespInvN[i].Load(), memRespInvRemoved[i].Load())
			}
		}()
	}
	defer func() {
		memoriesMem = false
		t.Logf("memories from mem: %d responses, %d items", memMemoriesServed.Load(), memMemoriesItems.Load())
	}()
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
	sc.Config.Handler = NewServer(config)
	// MD_PEER_DELAY_MS: pump -> peer の push に RTT 相当の遅延を入れる (eu/am の 100ms を模擬)
	if ms, _ := strconv.Atoi(os.Getenv("MD_PEER_DELAY_MS")); ms > 0 {
		inner := sc.Config.Handler
		sc.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/internal/mem/push") {
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
			inner.ServeHTTP(w, r)
		})
	}
	var barrierMu sync.Mutex
	var barrierSamples []time.Duration
	memBarrierObserve = func(d time.Duration) {
		barrierMu.Lock()
		barrierSamples = append(barrierSamples, d)
		barrierMu.Unlock()
	}
	defer func() { memBarrierObserve = nil }()
	os.Unsetenv("ISUREAL_MEM_ROLE")
	os.Unsetenv("ISUREAL_MEM_PUMP")
	sb.Start()
	sc.Start()
	defer sb.Close()
	defer sc.Close()
	sb.URL = "http://" + sb.Listener.Addr().String()
	sc.URL = "http://" + sc.Listener.Addr().String()

	pb := &mdClient{t: t, base: sb.URL}
	time.Sleep(time.Second)
	initStart := time.Now()
	if code, body := pb.json("POST", "/api/initialize", map[string]any{}); code != 200 {
		t.Fatalf("initialize: %d %s", code, body)
	}
	t.Logf("initialize took %v (peer ready=%v)", time.Since(initStart), memCurrentPeerStore.on())
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
		if !memCurrentPeerStore.on() || !memCurrentPump.m.on() {
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
				"/api/users?q=md", "/api/users?q=a", "/api/users/" + pick().id,
				"/api/memories?month=2026-11", "/api/memories?month=2026-10", "/api/memories?month=2026-12",
				"/api/memories/2026-11-01", "/api/memories/2026-11-02", "/api/memories/2026-11-03"} {
				compare(step, u, p)
			}
			if respCache || exportMem {
				compare(step, u, "/api/me/export")
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
		_, mb := u.b.do("GET", "/api/memories?month=2026-11", nil, "")
		var ml struct {
			Items []struct {
				ID       string    `json:"id"`
				PostedAt time.Time `json:"postedAt"`
			} `json:"items"`
		}
		_ = json.Unmarshal(mb, &ml)
		if len(ml.Items) >= 2 {
			it := ml.Items[0]
			compare(step, u, fmt.Sprintf("/api/memories?month=2026-11&beforePostedAt=%s&beforeId=%s", it.PostedAt.Format(time.RFC3339Nano), it.ID))
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
			memCurrentPump.invalidate()
			memCurrentPump.barrier()
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
	barrierMu.Lock()
	p50, p90, pmax := memDurPercentiles(barrierSamples)
	t.Logf("barrier n=%d p50=%v p90=%v max=%v lease_skips=%d", len(barrierSamples), p50, p90, pmax, memLeaseSkipsTotal.Load())
	barrierMu.Unlock()
}

func trunc(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 600 {
		return s[:600] + "..."
	}
	return s
}
