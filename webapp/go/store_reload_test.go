//go:build memdiff

package isureal

// 並行書き込み中に pump のリロード (invalidate) が起きても、書き戻し待ちの書き込みがメモリから消えないこと。
//   ISUREAL_DB_NAME=isureal_store ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error \
//     go test -tags memdiff -run TestStoreReloadRace -v .

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStoreReloadRace(t *testing.T) {
	config := ConfigFromEnv()
	memEnabled = false
	sa := httptest.NewServer(NewServer(config))
	defer sa.Close()
	memEnabled, storeEnabled = true, true
	defer func() { memEnabled, storeEnabled = false, false }()
	sb := httptest.NewServer(NewServer(config))
	defer sb.Close()
	e := storeCurrent
	if os.Getenv("STORE_NO_PRERELOAD") == "1" {
		e.p.preReload = nil
	}
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
	type u struct{ a, b *mdClient }
	var users []u
	var posts []string
	run := time.Now().UnixNano() % 100000
	for i := 0; i < 8; i++ {
		c := &mdClient{t: t, base: sb.URL}
		name := fmt.Sprintf("rr%d_%d", run, i)
		_, b := c.json("POST", "/api/auth/register", map[string]string{"username": name, "displayName": name, "region": "asia"})
		var rr struct {
			SecretKey string `json:"secretKey"`
		}
		_ = json.Unmarshal(b, &rr)
		_, b = c.json("POST", "/api/auth/login", map[string]string{"username": name, "secretKey": rr.SecretKey})
		var lr struct {
			Token string `json:"token"`
		}
		_ = json.Unmarshal(b, &lr)
		c.token = lr.Token
		code, pb := c.multipart("/api/posts", map[string]string{"caption": "x", "visibility": "public", "location_kind": "none"}, mdJPEG(16, 16, i))
		if code != 201 {
			t.Fatalf("post: %d %s", code, pb)
		}
		var p struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(pb, &p)
		posts = append(posts, p.ID)
		users = append(users, u{a: &mdClient{t: t, base: sa.URL, token: lr.Token}, b: c})
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				users[w].b.json("POST", "/api/posts/"+posts[(w+i)%len(posts)]+"/comments", map[string]string{"body": fmt.Sprintf("w%d-%d", w, i)})
			}
		}(w)
	}
	for i := 0; i < 10; i++ {
		time.Sleep(150 * time.Millisecond)
		e.p.invalidate() // 待たない
	}
	close(stop)
	wg.Wait()
	e.p.barrier()
	for i := 0; i < 100 && !e.m.on(); i++ {
		time.Sleep(50 * time.Millisecond)
	}
	mismatch := 0
	for _, p := range posts {
		ba, na := storeAllComments(users[0].a, p)
		bb, nb := storeAllComments(users[0].b, p)
		if ba != bb {
			mismatch++
			t.Errorf("post %s comments differ: DB %d items, MEM %d items", p, na, nb)
		}
	}
	t.Logf("mismatch=%d stats=%s", mismatch, e.statsString())
}

func storeAllComments(c *mdClient, postID string) (string, int) {
	var all strings.Builder
	n := 0
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
		n += len(cl.Items)
		if !cl.HasMore || len(cl.Items) == 0 {
			return all.String(), n
		}
		last := cl.Items[len(cl.Items)-1]
		path = fmt.Sprintf("/api/posts/%s/comments?afterCreatedAt=%s&afterId=%s", postID, url.QueryEscape(last.CreatedAt.Format(time.RFC3339Nano)), last.ID)
	}
}
