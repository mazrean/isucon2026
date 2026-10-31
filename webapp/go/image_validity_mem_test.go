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
		if ok != want || (ok && (!v.found || !v.valid || v.secretKeyHash == "")) {
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

// ISUREAL_IMG_MEM_ONLY: a known non-active owner is answered found=false by
// memory, matching queryImageValidity (no row for status = 'active').
func TestMemImageValidMemOnlyInactiveOwner(t *testing.T) {
	old := imgMemOnly
	imgMemOnly = true
	defer func() { imgMemOnly = old }()
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	h := &HandlerContext{Mem: m}
	prof := "img-prof"
	m.d.applyUser(memUserRow{ID: "u1", Username: "u1", SecretKeyHash: "k1", ProfileImageID: &prof, Status: "active"})
	if v, ok := memImageValid(h, "img-prof", "u1"); !ok || !v.found || !v.valid {
		t.Fatalf("active: ok=%v v=%+v", ok, v)
	}
	if _, ok := memImageValid(h, "img-x", "u-unknown"); ok {
		t.Fatal("unknown owner must ask the DB")
	}
	m.d.applyUser(memUserRow{ID: "u1", Username: "u1", SecretKeyHash: "k1", ProfileImageID: &prof, Status: "deleted"})
	if v, ok := memImageValid(h, "img-prof", "u1"); !ok || v.found {
		t.Fatalf("deleted: ok=%v v=%+v", ok, v)
	}
	imgMemOnly = false
	if _, ok := memImageValid(h, "img-prof", "u1"); ok {
		t.Fatal("knob off: deleted owner must ask the DB")
	}
}

// ISUREAL_IMG_MEM_ONLY: the realmoji branch of imageValidityQuery, answered
// from the hot post in memory (replacement / deletion / post deletion /
// author withdrawal turn it back to "ask the DB").
func TestMemImageValidMemOnlyRealMoji(t *testing.T) {
	old := imgMemOnly
	imgMemOnly = true
	defer func() { imgMemOnly = old }()
	imgRealMojiHint.mu.Lock()
	imgRealMojiHint.m = map[string]string{"img-rm": "p1", "img-rm2": "p1", "img-cold": "p-cold"}
	imgRealMojiHint.mu.Unlock()
	defer func() {
		imgRealMojiHint.mu.Lock()
		imgRealMojiHint.m = nil
		imgRealMojiHint.mu.Unlock()
	}()
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	h := &HandlerContext{Mem: m}
	d := m.d
	d.applyUser(memUserRow{ID: "author", Username: "author", SecretKeyHash: "ka", Status: "active"})
	d.applyUser(memUserRow{ID: "r", Username: "r", SecretKeyHash: "kr", Status: "active"})
	d.applyUser(memUserRow{ID: "x", Username: "x", SecretKeyHash: "kx", Status: "active"})
	now := time.Now()
	d.applyPost(memPostRow{ID: "p1", AuthorID: "author", PostingWindowID: "w", ImageID: "img-p1", PostedAt: now})
	check := func(img, owner string, want bool) {
		t.Helper()
		v, ok := memImageValid(h, img, owner)
		if ok != want || (ok && (!v.found || !v.valid || v.secretKeyHash == "")) {
			t.Fatalf("%s/%s: ok=%v v=%+v want %v", img, owner, ok, v, want)
		}
	}
	check("img-rm", "r", false) // not yet applied
	d.applyRealMoji(memRealMojiRow{ID: "rm1", PostID: "p1", ReactorID: "r", ImageID: "img-rm", ReactionType: "smile", CreatedAt: now})
	check("img-rm", "r", true)
	check("img-rm", "x", false)   // not the reactor
	check("img-cold", "r", false) // post not in memory
	// replacement: the old one gets replaced_at, the new one is active
	d.applyRealMoji(memRealMojiRow{ID: "rm1", PostID: "p1", ReactorID: "r", ImageID: "img-rm", ReactionType: "smile", CreatedAt: now, ReplacedAt: &now})
	d.applyRealMoji(memRealMojiRow{ID: "rm2", PostID: "p1", ReactorID: "r", ImageID: "img-rm2", ReactionType: "smile", CreatedAt: now})
	check("img-rm", "r", false)
	check("img-rm2", "r", true)
	// post author withdrawn
	d.applyUser(memUserRow{ID: "author", Username: "author", SecretKeyHash: "ka", Status: "deleted"})
	check("img-rm2", "r", false)
	d.applyUser(memUserRow{ID: "author", Username: "author", SecretKeyHash: "ka", Status: "active"})
	check("img-rm2", "r", true)
	// realmoji deleted
	d.applyRealMoji(memRealMojiRow{ID: "rm2", PostID: "p1", ReactorID: "r", ImageID: "img-rm2", ReactionType: "smile", CreatedAt: now, DeletedAt: &now})
	check("img-rm2", "r", false)
	d.applyRealMoji(memRealMojiRow{ID: "rm2", PostID: "p1", ReactorID: "r", ImageID: "img-rm2", ReactionType: "smile", CreatedAt: now})
	check("img-rm2", "r", true)
	// post deleted
	d.applyPost(memPostRow{ID: "p1", AuthorID: "author", PostingWindowID: "w", ImageID: "img-p1", PostedAt: now, DeletedAt: &now})
	check("img-rm2", "r", false)
}
