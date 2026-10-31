package isureal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFriendRegions(t *testing.T) {
	d := newMemData()
	mk := func(id, region string) *memUser {
		u := &memUser{User: User{ID: id, Region: region}, friends: map[string]memEdge{}}
		d.users[id] = u
		return u
	}
	a := mk("a", "asia")
	mk("b", "europe")
	mk("c", "asia")
	mk("lonely", "asia")
	a.friends["b"] = memEdge{}
	a.friends["c"] = memEdge{}
	a.friends["ghost"] = memEdge{} // unknown friend is ignored

	g := d.friendRegions("a")
	if len(g) != 2 || !g["europe"] || !g["asia"] || g["america"] {
		t.Fatalf("a: %v", g)
	}
	if g := d.friendRegions("lonely"); g == nil || len(g) != 0 {
		t.Fatalf("lonely: %v", g)
	}
	if g := d.friendRegions("nobody"); g == nil || len(g) != 0 {
		t.Fatalf("unknown: %v", g)
	}
	var nd *memData
	if g := nd.friendRegions("a"); g == nil || len(g) != 0 {
		t.Fatalf("nil data: %v", g)
	}
}

func newTestFriendPusher(t *testing.T, peers map[string]string) (*imgFriendPusher, *memStore) {
	t.Helper()
	m := &memStore{d: newMemData()}
	m.ready.Store(true)
	add := func(id, region string) *memUser {
		u := &memUser{User: User{ID: id, Region: region}, friends: map[string]memEdge{}}
		m.d.users[id] = u
		return u
	}
	a := add("author", "asia")
	add("eu-friend", "europe")
	add("am-stranger", "america")
	a.friends["eu-friend"] = memEdge{}
	return newImgFriendPusher(m, "asia", imgReplTestSecret, peers), m
}

// enqueue only queues for regions where the owner has friends (ALL off).
func TestFriendPushEnqueueRegions(t *testing.T) {
	old := imgFriendPushAll
	imgFriendPushAll = false
	defer func() { imgFriendPushAll = old }()
	p, _ := newTestFriendPusher(t, map[string]string{"europe": "e:1", "america": "a:1", "asia": "self:1"})
	if len(p.dests) != 2 {
		t.Fatalf("dests %d (own region must be excluded)", len(p.dests))
	}
	id := "0b0c5f3e-1111-4222-8333-944455556666"
	p.enqueue(id, &imageObject{Owner: "author", MediaType: "image/jpeg", Data: []byte("x")}, "")
	p.enqueue("0b0c5f3e-1111-4222-8333-944455550000", &imageObject{Owner: "unknown", MediaType: "image/jpeg", Data: []byte("x")}, "")
	for _, d := range p.dests {
		want := 0
		if d.region == "europe" {
			want = 1
		}
		if len(d.hi) != want || len(d.lo) != 0 {
			t.Fatalf("%s: hi %d lo %d, want hi %d", d.region, len(d.hi), len(d.lo), want)
		}
	}
	imgFriendPushAll = true
	p.enqueue(id, &imageObject{Owner: "author", MediaType: "image/jpeg", Data: []byte("x")}, "")
	for _, d := range p.dests {
		if d.region == "america" && len(d.lo) != 1 {
			t.Fatalf("ALL: america lo %d", len(d.lo))
		}
	}
}

// push -> receiver put, waits (does not drop) while the gate is closed.
func TestFriendPushSendAndReceive(t *testing.T) {
	oldOn, oldSecret := imgFriendPushOn, forwardSecret
	imgFriendPushOn, forwardSecret = true, imgReplTestSecret
	defer func() { imgFriendPushOn, forwardSecret = oldOn, oldSecret }()

	store := newImageStore(t.TempDir())
	e := newImgReplTestEcho(store)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.RemoteAddr = "192.168.1.10:4444" // asia1 (imgReplPeerSource)
		e.ServeHTTP(w, r)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	p, _ := newTestFriendPusher(t, map[string]string{"europe": addr})
	var open atomic.Bool
	p.gate = func(string) bool { return open.Load() }
	p.start()

	id := "0b0c5f3e-1111-4222-8333-9444555577e7"
	data := []byte("\xff\xd8jpeg\nbytes")
	p.enqueue(id, &imageObject{Owner: "author", MediaType: "image/jpeg", Data: data}, "")
	time.Sleep(300 * time.Millisecond)
	if _, ok := store.mem.get(id); ok {
		t.Fatal("sent while the gate was closed")
	}
	open.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if obj, ok := store.mem.get(id); ok {
			if obj.Owner != "author" || string(obj.Data) != string(data) {
				t.Fatalf("got %+v", obj)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("push never arrived")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if obj, err := store.get(context.Background(), nil, nil, id); err != nil || obj == nil {
		t.Fatalf("get after push: %v %v", obj, err)
	}
	before := imgFriendStats.recvHit.Load()
	imgFriendRecvHit(id)
	imgFriendRecvHit(id) // first request only
	if got := imgFriendStats.recvHit.Load() - before; got != 1 {
		t.Fatalf("recvHit +%d, want 1", got)
	}
}

func TestFriendPushQueueBound(t *testing.T) {
	oldQ := imgFriendPushMaxByte
	imgFriendPushMaxByte = 10
	defer func() { imgFriendPushMaxByte = oldQ }()
	p, _ := newTestFriendPusher(t, map[string]string{"europe": "e:1"})
	before := imgFriendStats.dropped.Load()
	for i := range 3 {
		id := "0b0c5f3e-1111-4222-8333-94445555000" + string(rune('0'+i))
		p.enqueue(id, &imageObject{Owner: "author", MediaType: "image/jpeg", Data: []byte("123456")}, "")
	}
	if got := imgFriendStats.dropped.Load() - before; got != 2 {
		t.Fatalf("dropped %d, want 2", got)
	}
}

func TestFriendPushGPGate(t *testing.T) {
	old := imgFriendPushGP
	imgFriendPushGP = false
	defer func() { imgFriendPushGP = old }()
	p, m := newTestFriendPusher(t, map[string]string{"europe": "e:1"})
	if !p.defaultGate("europe") {
		t.Fatal("idle, no window: gate should be open")
	}
	now := time.Now()
	// GP (gp_fair.go) = all three regions opened together, within gpFairActive of the start
	for _, r := range gpRegions {
		m.d.open[r] = &memWindow{PostingWindow: PostingWindow{StartsAt: now.Add(-2 * time.Second), OnTimeDeadline: now.Add(50 * time.Second)}}
	}
	m.gp.Store(nil) // gpWindowDeadline caches its snapshot for 50ms
	if p.defaultGate("europe") {
		t.Fatal("GP open: gate should be closed")
	}
	for _, r := range gpRegions {
		delete(m.d.open, r)
	}
	m.gp.Store(nil)
	p.txBps.Store(imgFriendPushMaxBps)
	if p.defaultGate("europe") {
		t.Fatal("busy NIC: gate should be closed")
	}
}

func TestFriendPushTiers(t *testing.T) {
	old := imgFriendPushAll
	imgFriendPushAll = true
	defer func() { imgFriendPushAll = old }()
	p, _ := newTestFriendPusher(t, map[string]string{"europe": "e:1", "america": "a:1"})
	obj := &imageObject{Owner: "author", MediaType: "image/jpeg", Data: []byte("x")}
	p.enqueue("0b0c5f3e-1111-4222-8333-944455551111", obj, "public")
	p.enqueue("0b0c5f3e-1111-4222-8333-944455552222", obj, "friends")
	for _, d := range p.dests {
		switch d.region {
		case "europe": // friend region: hi for both
			if len(d.hi) != 2 || len(d.mid)+len(d.lo) != 0 {
				t.Fatalf("europe %d/%d/%d", len(d.hi), len(d.mid), len(d.lo))
			}
		case "america": // public -> mid, friends-only -> lo
			if len(d.hi) != 0 || len(d.mid) != 1 || len(d.lo) != 1 {
				t.Fatalf("america %d/%d/%d", len(d.hi), len(d.mid), len(d.lo))
			}
		}
	}
}

// a receiver without a disk cache keeps friend pushes in the separate LRU;
// load() serves them and moves them to the main store.
func TestFriendRecvNoDisk(t *testing.T) {
	oldOn := imgFriendPushOn
	imgFriendPushOn = true
	defer func() { imgFriendPushOn = oldOn }()
	store := newImageStore("-")
	e := newImgReplTestEcho(store)
	id := "0b0c5f3e-1111-4222-8333-944455553333"
	data := []byte("\xff\xd8abc")
	req := imgReplTestRequest(id, "192.168.2.10:1", imgReplTestSecret, imgReplSign(imgReplTestSecret, id, "owner-1", "image/jpeg", data), data)
	req.Header.Set(imgFriendHeader, "1")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status %d", rec.Code)
	}
	if _, ok := store.mem.get(id); ok {
		t.Fatal("friend push went to the main LRU")
	}
	obj, err := store.get(context.Background(), nil, nil, id)
	if err != nil || obj == nil || string(obj.Data) != string(data) {
		t.Fatalf("get: %v %v", obj, err)
	}
	if imgFriendRecvLRU.take(id) != nil {
		t.Fatal("not moved out of the friend LRU")
	}
}

// FRESH: a full queue evicts its oldest job instead of dropping the new one.
func TestFriendPushFreshEvicts(t *testing.T) {
	oldF, oldQ := imgFriendPushFresh, imgFriendPushMaxByte
	imgFriendPushFresh, imgFriendPushMaxByte = true, 10
	defer func() { imgFriendPushFresh, imgFriendPushMaxByte = oldF, oldQ }()
	p, _ := newTestFriendPusher(t, map[string]string{"europe": "e:1"})
	ev := imgFriendStats.evicted.Load()
	p.enqueue("0b0c5f3e-1111-4222-8333-944455550001", &imageObject{Owner: "author", MediaType: "image/jpeg", Data: []byte("123456")}, "")
	p.enqueue("0b0c5f3e-1111-4222-8333-944455550002", &imageObject{Owner: "author", MediaType: "image/jpeg", Data: []byte("123456")}, "")
	d := p.dests[0]
	if len(d.hi) != 1 || imgFriendStats.evicted.Load()-ev != 1 {
		t.Fatalf("hi %d evicted %d", len(d.hi), imgFriendStats.evicted.Load()-ev)
	}
	if j := <-d.hi; j.id != "0b0c5f3e-1111-4222-8333-944455550002" {
		t.Fatalf("kept %s, want the newest", j.id)
	}
}

func TestFriendPushWindowBudget(t *testing.T) {
	old := imgFriendPushWindowBytes
	imgFriendPushWindowBytes = 10
	defer func() { imgFriendPushWindowBytes = old }()
	d := &imgFriendDest{}
	if !d.windowAllow("w1", 6) || d.windowAllow("w1", 6) {
		t.Fatal("w1: first fits, second over")
	}
	if !d.windowAllow("w2", 6) {
		t.Fatal("new window resets the budget")
	}
}

// a push for an image the receiver already has counts as late, not pending.
func TestFriendRecvLate(t *testing.T) {
	oldOn := imgFriendPushOn
	imgFriendPushOn = true
	defer func() { imgFriendPushOn = oldOn }()
	store := newImageStore(t.TempDir())
	e := newImgReplTestEcho(store)
	id := "0b0c5f3e-1111-4222-8333-944455554444"
	data := []byte("\xff\xd8abc")
	store.mem.add(id, &imageObject{Owner: "owner-1", MediaType: "image/jpeg", Data: data}) // pulled already
	req := imgReplTestRequest(id, "192.168.2.10:1", imgReplTestSecret, imgReplSign(imgReplTestSecret, id, "owner-1", "image/jpeg", data), data)
	req.Header.Set(imgFriendHeader, "1")
	before := imgFriendStats.recvLate.Load()
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || imgFriendStats.recvLate.Load()-before != 1 {
		t.Fatalf("status %d late +%d", rec.Code, imgFriendStats.recvLate.Load()-before)
	}
}
