package isureal

import (
	"testing"
	"time"
)

func TestMemImageValid(t *testing.T) {
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	h := &HandlerContext{Mem: m}
	prof := "img-prof"
	m.d.applyUser(memUserRow{ID: "u1", Username: "u1", SecretKeyHash: "k1", ProfileImageID: &prof, Status: "active"})
	m.d.applyUser(memUserRow{ID: "u2", Username: "u2", SecretKeyHash: "k2", Status: "active"})
	// old post: only in the index, not in d.posts
	imgPostIndex.mu.Lock()
	imgPostIndex.d = m.d
	imgPostIndex.m = map[string]imgPostRef{"img-old": {postID: "p-old", authorID: "u1"}}
	imgPostIndex.mu.Unlock()
	imgPostIndex.lastTry.Store(time.Now().UnixNano()) // no background rebuild in the test

	check := func(img, owner string, want bool) {
		t.Helper()
		v, ok := memImageValid(h, img, owner)
		if ok != want || (ok && (!v.found || !v.valid)) {
			t.Fatalf("%s/%s: ok=%v v=%+v want %v", img, owner, ok, v, want)
		}
	}
	check("img-prof", "u1", true)
	check("img-old", "u1", true)
	check("img-old", "u2", false) // wrong owner
	check("img-none", "u1", false)

	// hot post
	m.d.applyPost(memPostRow{ID: "p-new", AuthorID: "u2", PostingWindowID: "w", ImageID: "img-new", PostedAt: time.Now()})
	check("img-new", "u2", true)

	// deletion through the changelog
	now := time.Now()
	m.d.applyPost(memPostRow{ID: "p-new", AuthorID: "u2", PostingWindowID: "w", ImageID: "img-new", PostedAt: now, DeletedAt: &now})
	check("img-new", "u2", false)
	m.d.applyPost(memPostRow{ID: "p-old", AuthorID: "u1", PostingWindowID: "w", ImageID: "img-old", PostedAt: now, DeletedAt: &now})
	check("img-old", "u1", false)

	// profile change and withdrawal
	m.d.applyUser(memUserRow{ID: "u1", Username: "u1", SecretKeyHash: "k1", Status: "active"})
	check("img-prof", "u1", false)
	m.d.applyUser(memUserRow{ID: "u1", Username: "u1", SecretKeyHash: "k1", ProfileImageID: &prof, Status: "deleted"})
	check("img-prof", "u1", false)

	// reload swaps the pointer: the index is no longer trusted
	imgPostIndex.mu.Lock()
	imgPostIndex.m["img-old2"] = imgPostRef{postID: "p-old2", authorID: "u2"}
	imgPostIndex.mu.Unlock()
	check("img-old2", "u2", true)
	m.mu.Lock()
	m.d = newMemData()
	m.d.applyUser(memUserRow{ID: "u2", Username: "u2", SecretKeyHash: "k2", Status: "active"})
	m.mu.Unlock()
	check("img-old2", "u2", false)

	// not ready
	m.ready.Store(false)
	check("img-prof", "u1", false)
}
