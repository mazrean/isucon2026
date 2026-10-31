//go:build memdiff

package isureal

// 書き込みの応答時間比較 (ローカル PG, RTT ≒ 0 なので PG 往復の差は過小評価になる)。
//   STORE_BENCH=0|1 ISUREAL_DB_NAME=isureal_store ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error \
//     go test -tags memdiff -run TestStoreBench -v .

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestStoreBench(t *testing.T) {
	mode := os.Getenv("STORE_BENCH")
	if mode == "" {
		t.Skip("set STORE_BENCH=0|1")
	}
	memEnabled, storeEnabled = true, mode == "1"
	defer func() { memEnabled, storeEnabled = false, false }()
	sb := httptest.NewServer(NewServer(ConfigFromEnv()))
	defer sb.Close()
	time.Sleep(1500 * time.Millisecond)
	root := &mdClient{t: t, base: sb.URL}
	if code, b := root.json("POST", "/api/initialize", map[string]any{}); code != 200 {
		t.Fatalf("initialize: %d %s", code, b)
	}
	admin := &mdClient{t: t, base: sb.URL}
	_, b := admin.json("POST", "/api/admin/auth/login", map[string]string{"username": "admin", "password": "password1234"})
	var al struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(b, &al)
	admin.token = al.Token
	day := time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)
	for {
		if code, _ := admin.json("POST", "/api/admin/posting-window", map[string]any{"region": "asia", "localDate": day.Format(time.DateOnly), "onTimeSeconds": 600}); code == 201 {
			break
		}
		day = day.AddDate(0, 0, 1)
	}
	type u struct {
		id string
		c  *mdClient
	}
	var users []u
	var posts []string
	run := time.Now().UnixNano() % 100000
	for i := 0; i < 64; i++ {
		c := &mdClient{t: t, base: sb.URL}
		name := fmt.Sprintf("bn%d_%d", run, i)
		_, b := c.json("POST", "/api/auth/register", map[string]string{"username": name, "displayName": name, "region": "asia"})
		var rr struct {
			SecretKey string `json:"secretKey"`
			User      struct {
				ID string `json:"id"`
			} `json:"user"`
		}
		_ = json.Unmarshal(b, &rr)
		_, b = c.json("POST", "/api/auth/login", map[string]string{"username": name, "secretKey": rr.SecretKey})
		var lr struct {
			Token string `json:"token"`
		}
		_ = json.Unmarshal(b, &lr)
		c.token = lr.Token
		users = append(users, u{id: rr.User.ID, c: c})
	}
	// 投稿 (画像込み) を全員同時に
	var mu sync.Mutex
	var lat []time.Duration
	var wg sync.WaitGroup
	img := mdJPEG(256, 256, 1)
	for i := range users {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := time.Now()
			code, pb := users[i].c.multipart("/api/posts", map[string]string{"caption": "x", "visibility": "public", "location_kind": "none"}, img)
			d := time.Since(st)
			var p struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(pb, &p)
			mu.Lock()
			if code == 201 {
				posts = append(posts, p.ID)
			}
			lat = append(lat, d)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	report := func(name string, lat []time.Duration, elapsed time.Duration) {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		q := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)] }
		t.Logf("STORE=%s %-16s n=%d p50=%v p90=%v p99=%v max=%v rps=%.0f", mode, name, len(lat), q(0.5), q(0.9), q(0.99), lat[len(lat)-1], float64(len(lat))/elapsed.Seconds())
	}
	report("posts(burst)", lat, time.Second)

	// コメント + RealMoji + 友達申請 を 64 並列で 5 秒
	lat = nil
	stop := time.Now().Add(5 * time.Second)
	start := time.Now()
	small := mdJPEG(64, 64, 2)
	for w := range users {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for time.Now().Before(stop) {
				st := time.Now()
				switch r.Intn(3) {
				case 0:
					users[w].c.json("POST", "/api/posts/"+posts[r.Intn(len(posts))]+"/comments", map[string]string{"body": "hello"})
				case 1:
					users[w].c.multipart("/api/posts/"+posts[r.Intn(len(posts))]+"/realmojis", map[string]string{"reactionType": "smile"}, small)
				case 2:
					users[w].c.json("POST", "/api/friend-requests", map[string]string{"recipientId": users[r.Intn(len(users))].id})
				}
				d := time.Since(st)
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	report("mixed writes", lat, time.Since(start))
}
