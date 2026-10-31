package isureal

import "testing"

func TestPushRegionsForPost(t *testing.T) {
	old := imgPushFilterOn
	imgPushFilterOn = true
	defer func() { imgPushFilterOn = old }()

	d := newMemData()
	mk := func(id, region string) *memUser {
		u := &memUser{User: User{ID: id, Region: region}, friends: map[string]memEdge{}}
		d.users[id] = u
		return u
	}
	a := mk("a", "asia")
	b := mk("b", "europe")
	c := mk("c", "america")
	mk("x", "america")
	a.friends["b"] = memEdge{}
	b.friends["a"] = memEdge{}
	b.friends["c"] = memEdge{}
	c.friends["b"] = memEdge{}

	eq := func(got map[string]bool, want ...string) bool {
		if got == nil || len(got) != len(want) {
			return false
		}
		for _, w := range want {
			if !got[w] {
				return false
			}
		}
		return true
	}
	if g := d.pushRegionsForPost("a", "friends", ""); !eq(g, "asia", "europe") {
		t.Fatalf("friends: %v", g)
	}
	if g := d.pushRegionsForPost("a", "friends_of_friends", ""); !eq(g, "asia", "europe", "america") {
		t.Fatalf("fof: %v", g)
	}
	if g := d.pushRegionsForPost("c", "friends", "x"); !eq(g, "america", "europe") {
		t.Fatalf("realmoji: %v", g)
	}
	if g := d.pushRegionsForPost("a", "public", ""); g != nil {
		t.Fatalf("public must push all: %v", g)
	}
	if g := d.pushRegionsForPost("missing", "friends", ""); g != nil {
		t.Fatalf("unknown author must push all: %v", g)
	}
	if imgPeerRegion("192.168.2.10:8080") != "europe" || imgPeerRegion("192.168.3.10:8080") != "america" || imgPeerRegion("10.0.0.1:8080") != "" {
		t.Fatal("peer region mapping")
	}
	imgPushFilterOn = false
	if g := d.pushRegionsForPost("a", "friends", ""); g != nil {
		t.Fatalf("flag off must push all: %v", g)
	}
}
