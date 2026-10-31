//go:build memdiff

package isureal

// 書き込み応答のパリティ: 元実装 (ISUREAL_MEM=0, DB isureal_orig) と store (ISUREAL_STORE=1, DB isureal_store) に
// 同じ書き込み列を流し、各書き込みのステータスとエラー本文が一致することを確認する (ID は作成順で対応付け)。
//   ISUREAL_DB_NAME=isureal_store ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error \
//     go test -tags memdiff -run TestStoreWriteParity -v .

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type pUser struct {
	ids    [2]string
	names  string
	secret [2]string
	c      [2]*mdClient
}

func TestStoreWriteParity(t *testing.T) {
	cfgS := ConfigFromEnv()
	cfgO := ConfigFromEnv()
	cfgO.DBName = "isureal_orig"
	cmd := exec.Command("../sql/init.sh")
	cmd.Env = append(os.Environ(), "ISUREAL_DB_NAME=isureal_orig")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init orig: %v %s", err, out)
	}
	memEnabled, storeEnabled = false, false
	so := httptest.NewServer(NewServer(cfgO))
	defer so.Close()
	memEnabled, storeEnabled = true, true
	defer func() { memEnabled, storeEnabled = false, false }()
	ss := httptest.NewServer(NewServer(cfgS))
	defer ss.Close()
	time.Sleep(1500 * time.Millisecond)
	if code, b := (&mdClient{t: t, base: ss.URL}).json("POST", "/api/initialize", map[string]any{}); code != 200 {
		t.Fatalf("initialize: %d %s", code, b)
	}
	bases := [2]string{so.URL, ss.URL}

	mismatches := 0
	same := func(step int, what string, ca, cb int, ba, bb []byte) bool {
		ea, eb := "", ""
		if ca >= 400 {
			ea = strings.TrimSpace(string(ba))
		}
		if cb >= 400 {
			eb = strings.TrimSpace(string(bb))
		}
		if ca != cb || ea != eb {
			mismatches++
			if mismatches < 15 {
				t.Errorf("step %d %s: orig %d %s / store %d %s", step, what, ca, trunc(ba), cb, trunc(bb))
			}
			return false
		}
		return true
	}
	var admin [2]*mdClient
	for i := range 2 {
		admin[i] = &mdClient{t: t, base: bases[i]}
		_, b := admin[i].json("POST", "/api/admin/auth/login", map[string]string{"username": "admin", "password": "password1234"})
		var al struct {
			Token string `json:"token"`
		}
		_ = json.Unmarshal(b, &al)
		admin[i].token = al.Token
	}
	day := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	openAll := func(step int) {
		for _, rg := range []string{"asia", "europe", "america"} {
			for k := 0; k < 30; k++ {
				ca, ba := admin[0].json("POST", "/api/admin/posting-window", map[string]any{"region": rg, "localDate": day.Format(time.DateOnly), "onTimeSeconds": 3})
				cb, bb := admin[1].json("POST", "/api/admin/posting-window", map[string]any{"region": rg, "localDate": day.Format(time.DateOnly), "onTimeSeconds": 3})
				same(step, "open window", ca, cb, ba, bb)
				if ca == 201 {
					break
				}
				day = day.AddDate(0, 0, 1)
			}
		}
		day = day.AddDate(0, 0, 1)
	}
	openAll(0)

	login := func(step int, u *pUser) {
		var codes [2]int
		var bodies [2][]byte
		for i := range 2 {
			c := &mdClient{t: t, base: bases[i]}
			codes[i], bodies[i] = c.json("POST", "/api/auth/login", map[string]string{"username": u.names, "secretKey": u.secret[i]})
			var lr struct {
				Token string `json:"token"`
				User  struct {
					ID string `json:"id"`
				} `json:"user"`
			}
			_ = json.Unmarshal(bodies[i], &lr)
			c.token = lr.Token
			if lr.User.ID != "" {
				u.ids[i] = lr.User.ID
			}
			u.c[i] = c
		}
		same(step, "login", codes[0], codes[1], bodies[0], bodies[1])
	}
	users := []*pUser{{names: "yuma_kobayashi_186", secret: [2]string{"0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs", "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs"}}}
	login(0, users[0])
	run := time.Now().UnixNano() % 100000
	for i := 0; i < 20; i++ {
		region := "asia"
		if i%7 == 6 {
			region = "europe"
		}
		u := &pUser{names: fmt.Sprintf("pt%d_%d", run, i)}
		var codes [2]int
		var bodies [2][]byte
		for k := range 2 {
			codes[k], bodies[k] = (&mdClient{t: t, base: bases[k]}).json("POST", "/api/auth/register", map[string]string{"username": u.names, "displayName": "D", "region": region})
			var rr struct {
				SecretKey string `json:"secretKey"`
			}
			_ = json.Unmarshal(bodies[k], &rr)
			u.secret[k] = rr.SecretKey
		}
		same(0, "register", codes[0], codes[1], nil, nil)
		login(0, u)
		users = append(users, u)
	}
	// 重複登録
	ca, ba := (&mdClient{t: t, base: bases[0]}).json("POST", "/api/auth/register", map[string]string{"username": users[1].names, "displayName": "D", "region": "asia"})
	cb, bb := (&mdClient{t: t, base: bases[1]}).json("POST", "/api/auth/register", map[string]string{"username": users[1].names, "displayName": "D", "region": "asia"})
	same(0, "register dup", ca, cb, ba, bb)

	type idPair [2]string
	var posts, comments, realmojis, requests []idPair
	rng := rand.New(rand.NewSource(3))
	pick := func() *pUser { return users[rng.Intn(len(users))] }
	pickID := func(list []idPair, recent bool) idPair {
		if len(list) == 0 {
			z := "00000000-0000-0000-0000-000000000000"
			return idPair{z, z}
		}
		if recent && len(list) > 12 {
			list = list[len(list)-12:]
		}
		return list[rng.Intn(len(list))]
	}
	extract := func(b []byte, key string) string {
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		s, _ := m[key].(string)
		return s
	}
	vis := []string{"public", "friends", "friends_of_friends"}
	// do は両方に同じ書き込みをして比較する。path/body は world ごとに組み立てる
	type req struct {
		method, path string
		json         any
		multipart    map[string]string
		img          []byte
		raw          []byte
		ctype        string
	}
	do := func(step int, what string, u *pUser, mk func(k int) req) (bool, [2][]byte) {
		var codes [2]int
		var bodies [2][]byte
		for k := range 2 {
			r := mk(k)
			c := u.c[k]
			if r.method == "ADMIN" {
				c = admin[k]
				r.method = "PUT"
			}
			switch {
			case r.multipart != nil:
				codes[k], bodies[k] = c.multipart(r.path, r.multipart, r.img)
			case r.raw != nil:
				codes[k], bodies[k] = c.do(r.method, r.path, bytes.NewReader(r.raw), r.ctype)
			default:
				codes[k], bodies[k] = c.json(r.method, r.path, r.json)
			}
		}
		ok := same(step, what, codes[0], codes[1], bodies[0], bodies[1])
		return ok && codes[0] < 300, bodies
	}
	for step := 1; step <= 1500; step++ {
		u, o := pick(), pick()
		if step%250 == 0 {
			openAll(step)
		}
		switch op := rng.Intn(27); op {
		case 0, 1:
			f := map[string]string{"caption": "c", "visibility": vis[rng.Intn(3)], "location_kind": "none"}
			img := mdJPEG(16, 16, step)
			if ok, b := do(step, "post", u, func(k int) req { return req{path: "/api/posts", multipart: f, img: img} }); ok {
				posts = append(posts, idPair{extract(b[0], "id"), extract(b[1], "id")})
			}
		case 2, 17, 18, 19:
			p := pickID(posts, true)
			if ok, b := do(step, "comment", u, func(k int) req {
				return req{method: "POST", path: "/api/posts/" + p[k] + "/comments", json: map[string]string{"body": "hi"}}
			}); ok {
				comments = append(comments, idPair{extract(b[0], "id"), extract(b[1], "id")})
			}
		case 3, 20, 21:
			p := pickID(posts, true)
			img := mdJPEG(8, 8, step)
			if ok, b := do(step, "realmoji", u, func(k int) req {
				return req{path: "/api/posts/" + p[k] + "/realmojis", multipart: map[string]string{"reactionType": "smile"}, img: img}
			}); ok {
				realmojis = append(realmojis, idPair{extract(b[0], "id"), extract(b[1], "id")})
			}
		case 4, 5, 22, 23:
			if ok, b := do(step, "friend request", u, func(k int) req {
				return req{method: "POST", path: "/api/friend-requests", json: map[string]string{"recipientId": o.ids[k]}}
			}); ok {
				requests = append(requests, idPair{extract(b[0], "relationId"), extract(b[1], "relationId")})
			}
			if op >= 22 {
				do(step, "friend request back", o, func(k int) req {
					return req{method: "POST", path: "/api/friend-requests", json: map[string]string{"recipientId": u.ids[k]}}
				})
			}
		case 6, 25:
			act := []string{"accept", "reject", "cancel"}[rng.Intn(3)]
			r := pickID(requests, false)
			for _, x := range users {
				do(step, "transition "+act, x, func(k int) req { return req{method: "POST", path: "/api/friend-requests/" + r[k] + "/" + act} })
			}
		case 7:
			do(step, "follow", u, func(k int) req { return req{method: "PUT", path: "/api/follows/" + o.ids[k]} })
		case 8:
			do(step, "unfollow", u, func(k int) req { return req{method: "DELETE", path: "/api/follows/" + o.ids[k]} })
		case 9:
			m := "DELETE"
			if rng.Intn(3) == 0 {
				m = "PUT"
			}
			do(step, m+" block", u, func(k int) req { return req{method: m, path: "/api/blocks/" + o.ids[k]} })
		case 10:
			do(step, "unfriend", u, func(k int) req { return req{method: "DELETE", path: "/api/friendships/" + o.ids[k]} })
		case 11:
			p := pickID(posts, false)
			v := vis[rng.Intn(3)]
			do(step, "patch post", u, func(k int) req { return req{method: "PATCH", path: "/api/posts/" + p[k], json: map[string]string{"visibility": v}} })
		case 12:
			p := pickID(posts, false)
			do(step, "delete post", u, func(k int) req { return req{method: "DELETE", path: "/api/posts/" + p[k]} })
		case 13:
			if rng.Intn(2) == 0 {
				x := pickID(comments, false)
				do(step, "delete comment", u, func(k int) req { return req{method: "DELETE", path: "/api/comments/" + x[k]} })
			} else {
				x := pickID(realmojis, false)
				do(step, "delete realmoji", u, func(k int) req { return req{method: "DELETE", path: "/api/realmojis/" + x[k]} })
			}
		case 14:
			do(step, "patch me", u, func(k int) req {
				return req{method: "PATCH", path: "/api/me", json: map[string]string{"bio": fmt.Sprintf("b%d", step)}}
			})
		case 15:
			if rng.Intn(3) == 0 {
				do(step, "certify", u, func(k int) req { return req{method: "ADMIN", path: "/api/admin/users/" + u.ids[k] + "/certification"} })
			} else {
				do(step, "logout", u, func(k int) req { return req{method: "POST", path: "/api/auth/logout"} })
				login(step, u)
			}
		case 16:
			if step%5 == 0 {
				rg := []string{"asia", "asia", "europe"}[rng.Intn(3)]
				do(step, "region", u, func(k int) req { return req{method: "PATCH", path: "/api/me", json: map[string]string{"region": rg}} })
			}
		case 24:
			if rng.Intn(2) == 0 {
				img := mdJPEG(32, 32, step)
				do(step, "profile image", u, func(k int) req {
					return req{method: "PUT", path: "/api/me/profile-image", raw: img, ctype: "image/jpeg"}
				})
			} else {
				do(step, "delete profile image", u, func(k int) req { return req{method: "DELETE", path: "/api/me/profile-image"} })
			}
		case 26:
			if step%150 == 0 && u != users[0] {
				do(step, "account deletion", u, func(k int) req { return req{method: "POST", path: "/api/me/account-deletion"} })
			}
		}
	}
	t.Logf("posts=%d comments=%d realmojis=%d requests=%d mismatches=%d stats=%s", len(posts), len(comments), len(realmojis), len(requests), mismatches, storeCurrent.statsString())
}
