package isureal

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func feedTestStore() *memStore {
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	d := m.d
	now := time.Now()
	w := &memWindow{}
	w.ID, w.Region, w.StartsAt, w.OnTimeDeadline = "w1", "asia", now.Add(-time.Hour), now.Add(time.Hour)
	d.windows["w1"], d.open["asia"] = w, w
	mk := func(id string) *memUser {
		u := &memUser{friends: map[string]memEdge{}, following: map[string]memEdge{}, followers: map[string]memEdge{},
			blocking: map[string]struct{}{}, blockedBy: map[string]struct{}{}, incoming: map[string]*memReq{}, outgoing: map[string]*memReq{}}
		u.ID, u.Username, u.Region, u.Status, u.AccountType = id, id, "asia", "active", "standard"
		d.users[id] = u
		return u
	}
	v := mk("viewer")
	a := mk("a")
	b := mk("b")
	v.friends["a"], a.friends["viewer"] = memEdge{}, memEdge{}
	a.friends["b"], b.friends["a"] = memEdge{}, memEdge{}
	v.following["b"] = memEdge{}
	for i := range 70 {
		au := a
		if i%2 == 1 {
			au = b
		}
		p := &memPost{author: au, window: w}
		p.ID = fmt.Sprintf("p%03d", i)
		p.AuthorID, p.PostingWindowID, p.Visibility = au.ID, "w1", "public"
		p.ImageID = "img" + p.ID
		p.PostedAt = now.Add(-time.Duration(70-i) * time.Second)
		w.posts = append(w.posts, p)
		d.posts[p.ID] = p
		au.posts = append(au.posts, p)
	}
	// viewer も投稿済み (閲覧条件)
	vp := &memPost{author: v, window: w}
	vp.ID, vp.AuthorID, vp.PostingWindowID, vp.Visibility, vp.PostedAt = "pv", "viewer", "w1", "public", now.Add(-90*time.Second)
	w.posts = append([]*memPost{vp}, w.posts...)
	d.posts["pv"] = vp
	v.posts = append(v.posts, vp)
	return m
}

// ISUREAL_MEM_FEED_UNLOCKED: ロックの外で組み立てても、応答はロック中に組み立てたものと同じ。
func TestMemFeedUnlockedParity(t *testing.T) {
	old := memFeedUnlocked
	t.Cleanup(func() { memFeedUnlocked = old })
	m := feedTestStore()
	for _, feed := range []string{"public", "friends", "following", "friends-of-friends"} {
		in := getFeedRequest{Feed: feed}
		memFeedUnlocked = false
		r0, ok0 := m.buildFeed("viewer", in)
		memFeedUnlocked = true
		r1, ok1 := m.buildFeed("viewer", in)
		j0, _ := json.Marshal(r0)
		j1, _ := json.Marshal(r1)
		if ok0 != ok1 || string(j0) != string(j1) {
			t.Fatalf("%s: mismatch\nlocked=%s\nunlocked=%s", feed, j0, j1)
		}
		t.Logf("%s: items=%d hasMore=%v", feed, len(r0.Items), r0.HasMore)
		if feed == "public" && (len(r0.Items) != 50 || !r0.HasMore) {
			t.Fatalf("public fixture should fill a page: %d %v", len(r0.Items), r0.HasMore)
		}
	}
	// not-on → false
	m.ready.Store(false)
	memFeedUnlocked = true
	if _, ok := m.buildFeed("viewer", getFeedRequest{Feed: "public"}); ok {
		t.Fatal("not-on must fall back")
	}
}
