package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

//go:embed golden/yuma_memories.json
var yumaGoldenJSON []byte

type goldenPost struct {
	ID           string   `json:"id"`
	LocalDate    string   `json:"local_date"`
	Caption      string   `json:"caption"`
	Visibility   string   `json:"visibility"`
	LocationKind string   `json:"location_kind"`
	ImageSHA256  string   `json:"image_sha256"`
	RealMojiIDs  []string `json:"realmoji_ids"`
	CommentIDs   []string `json:"comment_ids"`
}

const yumaUsername = "yuma_kobayashi_186"
const yumaSecret = "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs"

type checker struct {
	results []checkResult
	verbose bool
}

type checkResult struct {
	name   string
	ok     bool
	detail string
}

func (t *checker) assert(name string, ok bool, detail string, args ...any) bool {
	d := ""
	if !ok {
		d = fmt.Sprintf(detail, args...)
	}
	t.results = append(t.results, checkResult{name: name, ok: ok, detail: d})
	mark := "PASS"
	if !ok {
		mark = "FAIL"
	}
	fmt.Printf("[%s] %3d %s", mark, len(t.results), name)
	if !ok {
		fmt.Printf("  -- %s", d)
	}
	fmt.Println()
	return ok
}

func (t *checker) code(name string, r *Resp, want int) bool {
	return t.assert(name, r.Err == nil && r.Code == want, "want %d got %s", want, r.String())
}

func hasPost(items []PostJSON, id string) bool {
	for _, p := range items {
		if p.ID == id {
			return true
		}
	}
	return false
}

func hasUser[T any](items []T, get func(T) string, id string) bool {
	for _, it := range items {
		if get(it) == id {
			return true
		}
	}
	return false
}

type relItem struct {
	RelationID string   `json:"relationId"`
	User       UserJSON `json:"user"`
	Status     string   `json:"status"`
}

func relUsers(c *Client, kind string) []string {
	r := c.Get("/api/relationships/" + kind)
	var l ListJSON[relItem]
	_ = r.JSON(&l)
	ids := []string{}
	for _, it := range l.Items {
		ids = append(ids, it.User.ID)
	}
	return ids
}

func contains(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

func fetchImage(c *Client, u string) *Resp { return c.GetNoExpect(u) }

func runCheck(base string, doInit bool, verbose bool) int {
	t := &checker{verbose: verbose}
	start := time.Now()
	run := fmt.Sprintf("pb%d%03d", time.Now().Unix()%100000, rand.IntN(1000))

	// ---------- initialize ----------
	if doInit {
		ic := NewClient(base, "asia")
		r := ic.JSONReq("POST", "/api/initialize", nil)
		var v struct {
			Language string `json:"language"`
		}
		_ = r.JSON(&v)
		if !t.assert("POST /api/initialize 200 & language 非空", r.Code == 200 && v.Language != "", "%s", r.String()) {
			return report(t, start)
		}
	}
	{
		bad := NewClient(base, "asia")
		bad.Country = "ZZ"
		r := bad.GetNoExpect("/api/me")
		t.code("不正な X-Country-Code は 400", r, 400)
	}

	// ---------- 初期ユーザー ----------
	yuma := NewClient(base, "asia")
	yuma.Username, yuma.SecretKey = yumaUsername, yumaSecret
	if r := yuma.Login(); t.code("初期ユーザー yuma ログイン 200", r, 200) {
		r := yuma.Get("/api/me")
		var me UserJSON
		_ = r.JSON(&me)
		t.assert("yuma GET /api/me (asia)", r.Code == 200 && me.Username == yumaUsername && me.Region == "asia", "%s", r.String())
		t.code("yuma GET /api/me を europe 経由 → 403 region_mismatch", yuma.Via("europe").GetNoExpect("/api/me"), 403)
		checkYumaHistory(t, yuma)
	}
	{
		wrong := NewClient(base, "asia")
		wrong.Username, wrong.SecretKey = yumaUsername, "wrong-secret"
		t.code("誤った SecretKey のログインは 401", wrong.do("POST", "/api/auth/login", strings.NewReader(`{"username":"`+yumaUsername+`","secretKey":"wrong"}`), "application/json", false), 401)
	}

	// ---------- ユーザー登録 ----------
	mk := func(tag, region string) *Client {
		c := NewClient(base, region)
		c.Register(run+"_"+tag, "PB "+tag)
		return c
	}
	A, B, C := mk("a", "asia"), mk("b", "europe"), mk("c", "america")
	D, E, F := mk("d", "asia"), mk("e", "europe"), mk("f", "america")
	users := []*Client{A, B, C, D, E, F}
	allReg := true
	for _, u := range users {
		allReg = allReg && u.ID != "" && u.SecretKey != ""
	}
	if !t.assert("6 ユーザー登録 (asia/europe/america) 201", allReg, "registration failed") {
		return report(t, start)
	}
	{
		dup := NewClient(base, "asia")
		r := dup.JSONNoExpect("POST", "/api/auth/register", map[string]string{"username": A.Username, "displayName": "dup", "region": "asia"})
		t.code("重複 username 登録は 409", r, 409)
	}
	allLogin := true
	for _, u := range users {
		allLogin = allLogin && u.Login().Code == 200
	}
	if !t.assert("登録ユーザー全員ログイン 200", allLogin, "login failed") {
		return report(t, start)
	}
	t.code("america ユーザーが asia 経由で GET /api/me → 403", C.Via("asia").GetNoExpect("/api/me"), 403)

	// ---------- 友達関係 ----------
	friendReq := func(from, to *Client) (string, string) {
		r := from.JSONReq("POST", "/api/friend-requests", map[string]string{"recipientId": to.ID})
		var v struct {
			RelationID string `json:"relationId"`
			Status     string `json:"status"`
		}
		_ = r.JSON(&v)
		return v.RelationID, v.Status
	}
	transition := func(c *Client, id, action string) *Resp {
		return c.JSONNoExpect("POST", "/api/friend-requests/"+id+"/"+action, nil)
	}
	reqAB, st := friendReq(A, B)
	t.assert("A→B 友達申請 201 pending", reqAB != "" && st == "pending", "status=%q", st)
	t.assert("B(europe) の incoming-requests に A が即時反映", contains(relUsers(B, "incoming-requests"), A.ID), "missing")
	t.assert("A の outgoing-requests に B", contains(relUsers(A, "outgoing-requests"), B.ID), "missing")
	{
		r := transition(B, reqAB, "accept")
		var v struct{ Status string }
		_ = r.JSON(&v)
		t.assert("B が承認 200 accepted", r.Code == 200 && v.Status == "accepted", "%s", r.String())
	}
	t.assert("A(asia) の friends に B, B(europe) の friends に A", contains(relUsers(A, "friends"), B.ID) && contains(relUsers(B, "friends"), A.ID), "missing")
	friendReq(A, C)
	_, st = friendReq(C, A)
	t.assert("相互申請 (C→A) で自動 accepted", st == "accepted", "status=%q", st)
	reqAE, _ := friendReq(A, E)
	transition(E, reqAE, "accept")
	reqBF, _ := friendReq(B, F)
	transition(F, reqBF, "accept")
	t.assert("B-F 友達成立 (F は A の友達の友達)", contains(relUsers(F, "friends"), B.ID), "missing")
	{
		reqDA, _ := friendReq(D, A)
		r := transition(A, reqDA, "reject")
		var v struct{ Status string }
		_ = r.JSON(&v)
		t.assert("D→A 申請を A が拒否 → rejected, D outgoing から消える", r.Code == 200 && v.Status == "rejected" && !contains(relUsers(D, "outgoing-requests"), A.ID), "%s", r.String())
		reqDB, _ := friendReq(D, B)
		t.code("受信者でない C が D→B 申請を accept → 403", transition(C, reqDB, "accept"), 403)
		r = transition(D, reqDB, "cancel")
		t.assert("D が D→B 申請を取り下げ → cancelled, B incoming から消える", r.Code == 200 && !contains(relUsers(B, "incoming-requests"), D.ID), "%s", r.String())
	}

	// ---------- 認定・フォロー ----------
	admin := NewClient(base, "asia")
	if !t.code("管理者ログイン 200", admin.AdminLogin(), 200) {
		return report(t, start)
	}
	t.code("認定前の C をフォロー → 409", A.JSONNoExpect("PUT", "/api/follows/"+C.ID, nil), 409)
	t.code("admin が C を認定 204", admin.JSONReq("PUT", "/api/admin/users/"+C.ID+"/certification", nil), 204)
	t.code("A が C をフォロー 204", A.JSONReq("PUT", "/api/follows/"+C.ID, nil), 204)
	t.assert("A following に C, C(america) followers に A", contains(relUsers(A, "following"), C.ID) && contains(relUsers(C, "followers"), A.ID), "missing")

	// ---------- ブロック ----------
	t.code("A が E をブロック 204", A.JSONReq("PUT", "/api/blocks/"+E.ID, nil), 204)
	t.assert("ブロックで友達解除が E(europe) に即時反映", !contains(relUsers(E, "friends"), A.ID) && !contains(relUsers(A, "friends"), E.ID), "still friends")
	t.code("ブロックされた E→A 友達申請は 409", E.JSONNoExpect("POST", "/api/friend-requests", map[string]string{"recipientId": A.ID}), 409)
	t.code("E が A のプロフィール取得 → 404", E.GetNoExpect("/api/users/"+A.ID), 404)
	{
		r := E.Get("/api/users?q=" + url.QueryEscape(A.Username))
		var l ListJSON[UserJSON]
		_ = r.JSON(&l)
		t.assert("E のユーザー検索に A が出ない", r.Code == 200 && !hasUser(l.Items, func(u UserJSON) string { return u.ID }, A.ID), "%s", r.String())
		r = D.Get("/api/users?q=" + url.QueryEscape(A.Username))
		_ = r.JSON(&l)
		t.assert("D のユーザー検索に A が出る", r.Code == 200 && hasUser(l.Items, func(u UserJSON) string { return u.ID }, A.ID), "%s", r.String())
	}

	// ---------- プロフィール更新 ----------
	{
		r := B.JSONReq("PATCH", "/api/me", map[string]string{"displayName": "PB B renamed", "bio": "hello from europe " + run})
		var me UserJSON
		_ = r.JSON(&me)
		t.assert("B PATCH /api/me 200", r.Code == 200 && me.DisplayName == "PB B renamed", "%s", r.String())
		r = A.Get("/api/users/" + B.ID)
		var u UserJSON
		_ = r.JSON(&u)
		t.assert("友達 A(asia) から B の新表示名・bio が即時見える", r.Code == 200 && u.DisplayName == "PB B renamed" && u.Bio == "hello from europe "+run, "%s", r.String())
		r = D.Get("/api/users/" + B.ID)
		u = UserJSON{}
		_ = r.JSON(&u)
		t.assert("非友達 D からは regular の bio が空", r.Code == 200 && u.DisplayName == "PB B renamed" && u.Bio == "", "%s", r.String())
	}

	// ---------- 投稿期間 + SSE ----------
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sseUsers := []*Client{A, B, C}
	sseChs := make([]<-chan sseResult, len(sseUsers))
	sseOK := true
	for i, u := range sseUsers {
		ch, err := u.SubscribeSSE(ctx)
		if err != nil {
			sseOK = false
		}
		sseChs[i] = ch
	}
	t.assert("SSE /api/notifications 接続 200 (3 リージョン)", sseOK, "subscribe failed")
	time.Sleep(300 * time.Millisecond)
	from, _ := time.Parse(time.DateOnly, initialMaxLocalDate)
	date, windows, err := openWindows(admin, from.AddDate(0, 0, 1), 3600)
	if !t.assert("admin が 3 リージョンで投稿期間を開始 201", err == nil, "%v", err) {
		return report(t, start)
	}
	fmt.Printf("      localDate=%s\n", date)
	{
		got := 0
		detail := ""
		for i, ch := range sseChs {
			if ch == nil {
				continue
			}
			select {
			case res := <-ch:
				var n struct {
					PostingWindowID string `json:"postingWindowId"`
					Region          string `json:"region"`
				}
				_ = json.Unmarshal([]byte(res.Data), &n)
				if res.Err == nil && n.PostingWindowID == windows[sseUsers[i].Region] && n.Region == sseUsers[i].Region {
					got++
				} else {
					detail += fmt.Sprintf("%s:%v/%s ", sseUsers[i].Region, res.Err, res.Data)
				}
			case <-time.After(6 * time.Second):
				detail += sseUsers[i].Region + ":timeout "
			}
		}
		t.assert("SSE で各リージョンの投稿期間通知を受信", got == len(sseUsers), "%s", detail)
	}
	cancel()
	time.Sleep(1 * time.Second) // app clock と DB clock のずれ対策
	{
		r := A.Get("/api/posting-window/current")
		var v struct {
			Window *struct {
				ID        string `json:"id"`
				LocalDate string `json:"localDate"`
			} `json:"window"`
			HasPosted bool `json:"hasPosted"`
			CanPost   bool `json:"canPost"`
		}
		_ = r.JSON(&v)
		t.assert("A posting-window/current: 新 window, canPost", r.Code == 200 && v.Window != nil && v.Window.ID == windows["asia"] && v.Window.LocalDate == date && v.CanPost && !v.HasPosted, "%s", r.String())
	}

	// ---------- 投稿 ----------
	imgA := makeJPEG(320, 240)
	rA, postA := A.Post("friends", "A chair "+run, imgA)
	if !t.assert("A 投稿 201 (onTime, lateSeconds=0)", rA.Code == 201 && postA.OnTime && postA.LateSeconds == 0 && postA.PostingWindowID == windows["asia"], "%s", rA.String()) {
		return report(t, start)
	}
	{
		r := fetchImage(B, postA.ImageURL)
		t.assert("投稿画像 URL 取得 200 & バイト一致", r.Code == 200 && sha(r.Body) == sha(imgA), "%s", r.String())
		r2, _ := A.Post("friends", "dup", makeJPEG(32, 32))
		t.assert("同一期間の 2 回目投稿は 409", r2.Code == 409, "%s", r2.String())
		r = A.Get("/api/posts/me/current")
		var p PostJSON
		_ = r.JSON(&p)
		t.assert("A posts/me/current が投稿を返す", r.Code == 200 && p.ID == postA.ID, "%s", r.String())
	}
	{
		r := B.GetNoExpect("/api/posts/" + postA.ID)
		_, feed := B.Feed("friends")
		t.assert("未投稿の友達 B には A の投稿が見えない (詳細 404 / フィード無し)", r.Code == 404 && !hasPost(feed, postA.ID), "detail=%d", r.Code)
	}
	_, postB := B.Post("friends", "B chair", makeJPEG(200, 200))
	_, postC := C.Post("public", "C public chair", makeJPEG(200, 200))
	_, postD := D.Post("friends", "D chair", makeJPEG(100, 100))
	_, postE := E.Post("public", "E public chair", makeJPEG(100, 100))
	imgF := makeJPEG(100, 100)
	_, postF := F.Post("public", "F chair", imgF)
	t.assert("B,C,D,E,F 投稿 201", postB.ID != "" && postC.ID != "" && postD.ID != "" && postE.ID != "" && postF.ID != "", "B=%q C=%q D=%q E=%q F=%q", postB.ID, postC.ID, postD.ID, postE.ID, postF.ID)
	{
		_, feed := B.Feed("friends")
		r := B.Get("/api/posts/" + postA.ID)
		t.assert("投稿後、別リージョンの友達 B(europe) のフィードに A の投稿が即時出る", hasPost(feed, postA.ID) && r.Code == 200, "feed=%v detail=%d", hasPost(feed, postA.ID), r.Code)
		_, feed = C.Feed("friends")
		t.assert("友達 C(america) のフィードにも A の投稿", hasPost(feed, postA.ID), "missing")
		_, feedA := A.Feed("friends")
		t.assert("A の friends フィードに B,C の投稿 (自分の投稿は含まない)", hasPost(feedA, postB.ID) && hasPost(feedA, postC.ID) && !hasPost(feedA, postA.ID), "B=%v C=%v self=%v", hasPost(feedA, postB.ID), hasPost(feedA, postC.ID), hasPost(feedA, postA.ID))
	}
	{
		_, ff := D.Feed("friends")
		_, fp := D.Feed("public")
		r := D.GetNoExpect("/api/posts/" + postA.ID)
		t.assert("非友達 D には A の friends 投稿が見えない", !hasPost(ff, postA.ID) && !hasPost(fp, postA.ID) && r.Code == 404, "detail=%d", r.Code)
		_, ef := E.Feed("friends")
		_, ep := E.Feed("public")
		r = E.GetNoExpect("/api/posts/" + postA.ID)
		t.assert("ブロックされた E には A の投稿が見えない", !hasPost(ef, postA.ID) && !hasPost(ep, postA.ID) && r.Code == 404, "detail=%d", r.Code)
		_, ap := A.Feed("public")
		r = A.GetNoExpect("/api/posts/" + postE.ID)
		t.assert("ブロックした A にも E の public 投稿が見えない", !hasPost(ap, postE.ID) && r.Code == 404, "detail=%d", r.Code)
		_, fof := F.Feed("friends-of-friends")
		r = F.GetNoExpect("/api/posts/" + postA.ID)
		t.assert("友達の友達 F に friends 公開の A 投稿は見えない", !hasPost(fof, postA.ID) && r.Code == 404, "detail=%d", r.Code)
	}
	{
		r := A.JSONReq("PATCH", "/api/posts/"+postA.ID, map[string]string{"visibility": "friends_of_friends", "caption": "A chair edited"})
		var p PostJSON
		_ = r.JSON(&p)
		t.assert("A 投稿の公開範囲を friends_of_friends に変更 200", r.Code == 200 && p.Visibility == "friends_of_friends" && p.Caption == "A chair edited", "%s", r.String())
		_, fof := F.Feed("friends-of-friends")
		r = F.Get("/api/posts/" + postA.ID)
		t.assert("公開範囲変更が F(america) に即時反映 (FoF フィード/詳細)", hasPost(fof, postA.ID) && r.Code == 200, "feed=%v detail=%d", hasPost(fof, postA.ID), r.Code)
		r = D.GetNoExpect("/api/posts/" + postA.ID)
		t.code("D は依然 A 投稿を取得できない 404", r, 404)
		r = B.Get("/api/posts/" + postA.ID)
		p = PostJSON{}
		_ = r.JSON(&p)
		t.assert("B から見たキャプション変更が即時反映", r.Code == 200 && p.Caption == "A chair edited", "%s", r.String())
		r = A.JSONNoExpect("PATCH", "/api/posts/"+postB.ID, map[string]string{"caption": "hijack"})
		t.code("他人の投稿の編集は 403", r, 403)
	}
	{
		_, fp := D.Feed("public")
		t.assert("D の public フィードに C の public 投稿", hasPost(fp, postC.ID), "missing")
		t.code("C が D をブロック 204", C.JSONReq("PUT", "/api/blocks/"+D.ID, nil), 204)
		_, fp = D.Feed("public")
		r := D.GetNoExpect("/api/posts/" + postC.ID)
		t.assert("ブロック後 D(asia) に C(america) の投稿が即時見えない", !hasPost(fp, postC.ID) && r.Code == 404, "feed=%v detail=%d", hasPost(fp, postC.ID), r.Code)
		_, fl := A.Feed("following")
		t.assert("A の following フィードに認定 C の投稿", hasPost(fl, postC.ID), "missing")
	}

	// ---------- コメント ----------
	comment := func(c *Client, postID, body string) (*Resp, string) {
		r := c.JSONNoExpect("POST", "/api/posts/"+postID+"/comments", map[string]string{"body": body})
		var v struct {
			ID string `json:"id"`
		}
		_ = r.JSON(&v)
		return r, v.ID
	}
	rc, cB := comment(B, postA.ID, "nice chair")
	t.code("友達 B が A 投稿にコメント 201", rc, 201)
	rc, _ = comment(F, postA.ID, "fof comment")
	t.code("友達の友達 F の FoF 投稿へのコメントは 403", rc, 403)
	t.code("F は FoF 投稿のコメント一覧も 403", F.GetNoExpect("/api/posts/"+postA.ID+"/comments"), 403)
	rc, _ = comment(D, postA.ID, "stranger")
	t.code("非友達 D のコメントは 403", rc, 403)
	rc, _ = comment(E, postA.ID, "blocked")
	t.code("ブロックされた E のコメントは 403", rc, 403)
	rc, cA := comment(A, postA.ID, "thanks")
	t.code("投稿者 A 自身のコメント 201", rc, 201)
	rc, _ = comment(D, postF.ID, "public ok")
	t.code("public 投稿には非友達 D もコメント 201", rc, 201)
	{
		r := B.Get("/api/posts/" + postA.ID + "/comments")
		var l ListJSON[struct {
			ID string `json:"id"`
		}]
		_ = r.JSON(&l)
		ok := r.Code == 200 && len(l.Items) == 2 && l.Items[0].ID == cB && l.Items[1].ID == cA
		t.assert("B(europe) のコメント一覧に 2 件が時系列順", ok, "%s", r.String())
		t.code("D は A の投稿コメント削除不可 403", D.JSONNoExpect("DELETE", "/api/comments/"+cA, nil), 403)
		t.code("投稿者 A が B のコメントを削除 204", A.JSONReq("DELETE", "/api/comments/"+cB, nil), 204)
		r = B.Get("/api/posts/" + postA.ID + "/comments")
		l.Items = nil
		_ = r.JSON(&l)
		t.assert("削除コメントが一覧から即時消える", r.Code == 200 && len(l.Items) == 1 && l.Items[0].ID == cA, "%s", r.String())
	}

	// ---------- RealMoji ----------
	realmoji := func(c *Client, postID, typ string) (*Resp, string) {
		r := c.Multipart("/api/posts/"+postID+"/realmojis", map[string]string{"reactionType": typ}, makeJPEG(64, 64), false)
		var v struct {
			ID       string `json:"id"`
			ImageURL string `json:"imageUrl"`
		}
		_ = r.JSON(&v)
		return r, v.ID
	}
	rr, rmB1 := realmoji(B, postA.ID, "like")
	t.code("友達 B の RealMoji 201", rr, 201)
	rr, rmF := realmoji(F, postA.ID, "smile")
	t.code("友達の友達 F の FoF 投稿への RealMoji 201", rr, 201)
	rr, _ = realmoji(D, postA.ID, "heart")
	t.code("非友達 D の RealMoji は 403", rr, 403)
	rr, _ = realmoji(E, postA.ID, "heart")
	t.code("ブロックされた E の RealMoji は 403", rr, 403)
	rr, _ = realmoji(A, postA.ID, "heart")
	t.code("自分の投稿への RealMoji は 403", rr, 403)
	rr, rmB2 := realmoji(B, postA.ID, "laugh")
	t.code("B の RealMoji 置き換え 201", rr, 201)
	type rmItem struct {
		ID       string `json:"id"`
		ImageURL string `json:"imageUrl"`
		Reactor  struct {
			ID string `json:"id"`
		} `json:"reactor"`
	}
	{
		r := A.Get("/api/posts/" + postA.ID + "/realmojis")
		var l ListJSON[rmItem]
		_ = r.JSON(&l)
		ids := []string{}
		for _, it := range l.Items {
			ids = append(ids, it.ID)
		}
		t.assert("RealMoji 一覧: 置き換え後 B は 1 件 (新), F 1 件", r.Code == 200 && len(ids) == 2 && contains(ids, rmB2) && contains(ids, rmF) && !contains(ids, rmB1), "%v", ids)
		t.code("D は RealMoji 一覧取得 403", D.GetNoExpect("/api/posts/"+postA.ID+"/realmojis"), 403)
		t.code("B が自分の RealMoji 削除 204", B.JSONReq("DELETE", "/api/realmojis/"+rmB2, nil), 204)
		r = F.Get("/api/posts/" + postA.ID + "/realmojis")
		l.Items = nil
		_ = r.JSON(&l)
		t.assert("削除 RealMoji が F(america) の一覧から即時消える", r.Code == 200 && len(l.Items) == 1 && l.Items[0].ID == rmF, "%s", r.String())
	}

	// ---------- Memories / export ----------
	{
		r := A.Get("/api/memories/" + date)
		var l ListJSON[PostJSON]
		_ = r.JSON(&l)
		ok := r.Code == 200 && len(l.Items) == 1 && l.Items[0].ID == postA.ID && len(l.Items[0].Comments) == 1 && len(l.Items[0].RealMojis) == 1
		t.assert("A Memories(日付) に当日投稿 + コメント1/RealMoji1", ok, "%s", r.String())
		r = A.Get("/api/memories?month=" + date[:7])
		l = ListJSON[PostJSON]{}
		_ = r.JSON(&l)
		t.assert("A Memories(月) に当日投稿", r.Code == 200 && hasPost(l.Items, postA.ID), "%s", r.String())
		r = F.Get("/api/me/export")
		items, shas, err := parseExport(r.Body)
		ok = r.Code == 200 && err == nil && len(items) == 1 && items[0].ID == postF.ID && shas[items[0].ImagePath] == sha(imgF)
		t.assert("F エクスポート: 投稿 1 件 + 画像バイト一致", ok, "code=%d err=%v n=%d", r.Code, err, len(items))
	}

	// ---------- プロフィール画像 ----------
	{
		img := makeJPEG(128, 128)
		r := B.do("PUT", "/api/me/profile-image", bytes.NewReader(img), "image/jpeg", true)
		t.code("B プロフィール画像設定 204", r, 204)
		r = A.Get("/api/users/" + B.ID)
		var u UserJSON
		_ = r.JSON(&u)
		purl := ""
		if u.ProfileImageURL != nil {
			purl = *u.ProfileImageURL
		}
		ri := fetchImage(A, purl)
		t.assert("A(asia) から B のプロフィール画像 URL が即時取得でき一致", purl != "" && ri.Code == 200 && sha(ri.Body) == sha(img), "url=%q %s", purl, ri.String())
		t.code("B プロフィール画像削除 204", B.JSONReq("DELETE", "/api/me/profile-image", nil), 204)
		if purl != "" {
			t.code("削除したプロフィール画像は 403", fetchImage(A, purl), 403)
		}
	}

	// ---------- 署名 ----------
	{
		u, _ := url.Parse(postA.ImageURL)
		q := u.Query()
		sig := []byte(q.Get("sig"))
		if len(sig) > 0 {
			if sig[0] == 'A' {
				sig[0] = 'B'
			} else {
				sig[0] = 'A'
			}
		}
		q.Set("sig", string(sig))
		u.RawQuery = q.Encode()
		throw := NewClient(base, "asia")
		t.code("改ざん署名の画像 URL は 403", fetchImage(throw, u.String()), 403)
		q = u.Query()
		q.Set("exp", "1700000000")
		u.RawQuery = q.Encode()
		t.code("期限切れ exp の画像 URL は 403", fetchImage(throw, u.String()), 403)
	}

	// ---------- 投稿削除 ----------
	{
		before := fetchImage(B, postA.ImageURL)
		t.code("削除前の A 投稿画像 200", before, 200)
		t.code("A 投稿削除 204", A.JSONReq("DELETE", "/api/posts/"+postA.ID, nil), 204)
		_, feed := B.Feed("friends")
		r := B.GetNoExpect("/api/posts/" + postA.ID)
		t.assert("削除直後 B(europe) のフィード/詳細から消える", !hasPost(feed, postA.ID) && r.Code == 404, "feed=%v detail=%d", hasPost(feed, postA.ID), r.Code)
		r = F.Multipart("/api/posts/"+postA.ID+"/realmojis", map[string]string{"reactionType": "like"}, makeJPEG(16, 16), false)
		t.code("削除済み投稿への RealMoji は 403", r, 403)
		time.Sleep(5500 * time.Millisecond)
		t.code("削除 5.5 秒後の画像取得は 403", fetchImage(B, postA.ImageURL), 403)
		r = A.Get("/api/posting-window/current")
		var v struct {
			CanPost       bool `json:"canPost"`
			HasPosted     bool `json:"hasPosted"`
			DeletionCount int  `json:"deletionCount"`
		}
		_ = r.JSON(&v)
		t.assert("削除後 posting-window/current: canPost, deletionCount=1", r.Code == 200 && v.CanPost && !v.HasPosted && v.DeletionCount == 1, "%s", r.String())
		r2, p2 := A.Post("friends", "A repost", makeJPEG(64, 64))
		t.code("削除後の再投稿 201", r2, 201)
		_, feed = B.Feed("friends")
		t.assert("再投稿が B のフィードに出る", hasPost(feed, p2.ID), "missing")
		postA = p2
	}

	// ---------- リージョン変更 ----------
	{
		r := D.JSONReq("PATCH", "/api/me", map[string]string{"region": "europe"})
		var me UserJSON
		_ = r.JSON(&me)
		t.assert("D のリージョンを europe に変更 200", r.Code == 200 && me.Region == "europe", "%s", r.String())
		t.code("変更後 D は asia 経由で 403", D.GetNoExpect("/api/me"), 403)
		D.Country = "DE"
		t.code("変更後 D は europe 経由で 200", D.Get("/api/me"), 200)
	}

	// ---------- 友達解除 ----------
	{
		t.code("A が C との友達を解除 204", A.JSONReq("DELETE", "/api/friendships/"+C.ID, nil), 204)
		_, feed := C.Feed("friends")
		r := C.GetNoExpect("/api/posts/" + postA.ID)
		t.assert("友達解除が C(america) に即時反映 (A 投稿が見えない)", !hasPost(feed, postA.ID) && r.Code == 404, "feed=%v detail=%d", hasPost(feed, postA.ID), r.Code)
	}

	// ---------- 退会 ----------
	{
		t.code("F 退会 204", F.JSONReq("POST", "/api/me/account-deletion", nil), 204)
		t.code("退会後の F 旧トークンは 401", F.GetNoExpect("/api/me"), 401)
		fc := NewClient(base, "america")
		fc.Username, fc.SecretKey = F.Username, F.SecretKey
		t.code("退会後の F ログインは 401", fc.do("POST", "/api/auth/login", strings.NewReader(`{"username":"`+F.Username+`","secretKey":"`+F.SecretKey+`"}`), "application/json", false), 401)
		t.assert("退会した F が B(europe) の friends から消える", !contains(relUsers(B, "friends"), F.ID), "still present")
		t.code("退会した F の投稿画像は 403", fetchImage(B, postF.ImageURL), 403)
		_, fp := D.Feed("public")
		t.assert("退会した F の投稿が public フィードから消える", !hasPost(fp, postF.ID), "still present")
	}

	// ---------- 次の投稿期間 ----------
	{
		next, _ := time.Parse(time.DateOnly, date)
		date2, w2, err := openWindows(admin, next.AddDate(0, 0, 1), 0)
		t.assert("次の投稿期間を開始 201", err == nil, "%v", err)
		t.code("終了した期間の投稿は編集不可 403", A.JSONNoExpect("PATCH", "/api/posts/"+postA.ID, map[string]string{"caption": "late edit"}), 403)
		time.Sleep(1100 * time.Millisecond)
		r, p := B.Post("friends", "late", makeJPEG(64, 64))
		t.assert("onTimeSeconds=0 の期間の投稿は onTime=false", r.Code == 201 && !p.OnTime && p.PostingWindowID == w2["europe"] && p.LateSeconds >= 1, "%s", r.String())
		r = B.GetNoExpect("/api/posts/" + postA.ID)
		_, feed := B.Feed("friends")
		t.assert("前の期間の投稿は新期間に投稿済みでもフィード/詳細に出ない", !hasPost(feed, postA.ID) && r.Code == 404, "feed=%v detail=%d", hasPost(feed, postA.ID), r.Code)
		r = A.Get("/api/memories?month=" + date[:7])
		var l ListJSON[PostJSON]
		_ = r.JSON(&l)
		t.assert("終了した期間の自分の投稿は Memories に残る", r.Code == 200 && hasPost(l.Items, postA.ID), "%s", r.String())
		_ = date2
	}

	return report(t, start)
}

func checkYumaHistory(t *checker, yuma *Client) {
	var golden []goldenPost
	if err := json.Unmarshal(yumaGoldenJSON, &golden); err != nil {
		t.assert("golden 読み込み", false, "%v", err)
		return
	}
	byMonth := map[string][]goldenPost{}
	for _, g := range golden {
		byMonth[g.LocalDate[:7]] = append(byMonth[g.LocalDate[:7]], g)
	}
	months := make([]string, 0, len(byMonth))
	for m := range byMonth {
		months = append(months, m)
	}
	sort.Strings(months)
	gm := map[string]goldenPost{}
	for _, g := range golden {
		gm[g.ID] = g
	}
	cmp := func(items []PostJSON, want []goldenPost) string {
		if len(items) != len(want) {
			return fmt.Sprintf("count got %d want %d", len(items), len(want))
		}
		// memories は posted_at DESC
		for i, it := range items {
			w := want[len(want)-1-i]
			if it.ID != w.ID {
				return fmt.Sprintf("order[%d] got %s want %s", i, it.ID, w.ID)
			}
			if it.Caption != w.Caption || it.Visibility != w.Visibility || it.Location.Kind != w.LocationKind {
				return fmt.Sprintf("post %s fields mismatch", it.ID)
			}
			rm := []string{}
			for _, x := range it.RealMojis {
				rm = append(rm, x.ID)
			}
			cm := []string{}
			for _, x := range it.Comments {
				cm = append(cm, x.ID)
			}
			if strings.Join(rm, ",") != strings.Join(w.RealMojiIDs, ",") {
				return fmt.Sprintf("post %s realmojis got %v want %v", it.ID, rm, w.RealMojiIDs)
			}
			if strings.Join(cm, ",") != strings.Join(w.CommentIDs, ",") {
				return fmt.Sprintf("post %s comments got %v want %v", it.ID, cm, w.CommentIDs)
			}
		}
		return ""
	}
	var all []PostJSON
	bad := ""
	for _, m := range months {
		r := yuma.Get("/api/memories?month=" + m)
		var l ListJSON[PostJSON]
		if err := r.JSON(&l); err != nil || r.Code != 200 {
			bad = fmt.Sprintf("%s: %s", m, r.String())
			break
		}
		if d := cmp(l.Items, byMonth[m]); d != "" {
			bad = m + ": " + d
			break
		}
		all = append(all, l.Items...)
	}
	t.assert(fmt.Sprintf("yuma Memories 月単位 %d ヶ月 (投稿/RealMoji/コメント) が初期データと一致", len(months)), bad == "", "%s", bad)
	{
		g := golden[len(golden)/2]
		r := yuma.Get("/api/memories/" + g.LocalDate)
		var l ListJSON[PostJSON]
		_ = r.JSON(&l)
		want := []goldenPost{}
		for _, x := range golden {
			if x.LocalDate == g.LocalDate {
				want = append(want, x)
			}
		}
		d := cmp(l.Items, want)
		t.assert("yuma Memories 日付単位が初期データと一致", r.Code == 200 && d == "", "%s %s", d, r.String())
	}
	{
		bad := ""
		for _, p := range all {
			r := yuma.GetNoExpect(p.ImageURL)
			if r.Code != 200 || sha(r.Body) != gm[p.ID].ImageSHA256 {
				bad = fmt.Sprintf("post %s image %d", p.ID, r.Code)
				break
			}
		}
		t.assert(fmt.Sprintf("yuma Memories の画像 %d 枚が初期データとバイト一致", len(all)), len(all) == len(golden) && bad == "", "%s (n=%d)", bad, len(all))
	}
	{
		r := yuma.Get("/api/me/export")
		items, shas, err := parseExport(r.Body)
		bad := ""
		if r.Code != 200 || err != nil {
			bad = fmt.Sprintf("code=%d err=%v", r.Code, err)
		} else if len(items) != len(golden) {
			bad = fmt.Sprintf("count %d want %d", len(items), len(golden))
		} else {
			for i, it := range items {
				if it.ID != golden[i].ID || it.Caption != golden[i].Caption || shas[it.ImagePath] != golden[i].ImageSHA256 || it.ImagePath != fmt.Sprintf("images/%06d.jpg", i+1) {
					bad = fmt.Sprintf("item %d (%s) mismatch", i, it.ID)
					break
				}
			}
		}
		t.assert("yuma エクスポート (posts.json 順序・画像) が初期データと一致", bad == "", "%s", bad)
	}
}

func report(t *checker, start time.Time) int {
	fail := 0
	for _, r := range t.results {
		if !r.ok {
			fail++
		}
	}
	fmt.Println("==================================================")
	fmt.Printf("check: %d items, PASS %d, FAIL %d (%.1fs)\n", len(t.results), len(t.results)-fail, fail, time.Since(start).Seconds())
	if fail > 0 {
		for _, r := range t.results {
			if !r.ok {
				fmt.Printf("  FAIL: %s -- %s\n", r.name, r.detail)
			}
		}
		fmt.Fprintln(os.Stderr, "RESULT: FAIL")
		return 1
	}
	fmt.Println("RESULT: PASS")
	return 0
}
