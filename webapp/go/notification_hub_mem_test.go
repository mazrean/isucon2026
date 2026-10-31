package isureal

import (
	"encoding/json"
	"testing"
	"time"
)

func newTestNotificationHub() *notificationHub {
	return &notificationHub{windows: map[string]notificationWindow{}, subs: map[string]map[*notificationSubscriber]struct{}{}}
}

func recvNotification(sub *notificationSubscriber) (string, bool) {
	select {
	case n := <-sub.ch:
		if n == nil {
			return "closed", true
		}
		b, _ := json.Marshal(n)
		return string(b), true
	default:
		return "", false
	}
}

// ISUREAL_NOTIFY_MEM の on (memStore から) と off (PG の行と同じ形の入力) で、届く通知が一致する。
func TestNotificationHubMemMatchesPG(t *testing.T) {
	base := time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)
	ended := base
	type userDef struct{ id, region, status, connRegion string }
	users := []userDef{
		{"u1", "asia", "active", "asia"},
		{"u2", "asia", "active", "asia"},
		{"u3", "europe", "active", "europe"},
		{"u4", "asia", "deleted", "asia"},  // 退会: 閉じる
		{"u5", "europe", "active", "asia"}, // リージョン移動: 閉じる
		{"u6", "america", "active", "america"},
	}
	windows := []memWindowRow{
		{ID: "w-asia", Region: "asia", LocalDate: "2026-10-31", StartsAt: base.Add(time.Minute), OnTimeDeadline: base.Add(3 * time.Minute)},
		{ID: "w-asia2", Region: "asia", LocalDate: "2026-10-31", StartsAt: base.Add(time.Minute), OnTimeDeadline: base.Add(3 * time.Minute)},
		{ID: "w-eu", Region: "europe", LocalDate: "2026-10-31", StartsAt: base.Add(2 * time.Minute), OnTimeDeadline: base.Add(4 * time.Minute)},
		{ID: "w-am-old", Region: "america", LocalDate: "2026-10-30", StartsAt: base.Add(-time.Hour), OnTimeDeadline: base, EndedAt: &ended},
	}

	// off: PG 経路と同じ入力を applyActive + deliverWith に渡す
	pgHub := newTestNotificationHub()
	pgSubs := map[string]*notificationSubscriber{}
	for _, u := range users {
		pgSubs[u.id] = pgHub.subscribe(u.id, u.connRegion, base)
	}
	var active []notificationActiveWindow
	for _, w := range windows {
		if w.EndedAt == nil {
			active = append(active, notificationActiveWindow{ID: w.ID, notificationWindow: notificationWindow{Region: w.Region, StartsAt: memLocal(w.StartsAt)}})
		}
	}
	regions := map[string]string{}
	for _, u := range users {
		if u.status == "active" {
			regions[u.id] = u.region
		}
	}
	pgHub.deliverWith(pgHub.applyActive(active), regions)

	// on: memStore から
	m := &memStore{d: newMemData()}
	for _, w := range windows {
		if err := m.d.applyWindow(w); err != nil {
			t.Fatal(err)
		}
	}
	for _, u := range users {
		m.d.users[u.id] = &memUser{User: User{ID: u.id, Region: u.region, Status: u.status}}
	}
	memHub := newTestNotificationHub()
	memSubs := map[string]*notificationSubscriber{}
	for _, u := range users {
		memSubs[u.id] = memHub.subscribe(u.id, u.connRegion, base)
	}
	if err := memHub.pollMem(m); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{"u4": "closed", "u5": "closed", "u6": ""}
	for _, u := range users {
		pv, pok := recvNotification(pgSubs[u.id])
		mv, mok := recvNotification(memSubs[u.id])
		if pv != mv || pok != mok {
			t.Errorf("%s: pg=%q(%v) mem=%q(%v)", u.id, pv, pok, mv, mok)
		}
		if w, ok := want[u.id]; ok && mv != w {
			t.Errorf("%s: want %q got %q", u.id, w, mv)
		}
		if _, ok := want[u.id]; !ok && (mv == "" || mv == "closed") {
			t.Errorf("%s: want notification got %q", u.id, mv)
		}
	}
}

func TestNotificationHubMemCheckUsersCloses(t *testing.T) {
	base := time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)
	m := &memStore{d: newMemData()}
	m.d.users["a"] = &memUser{User: User{ID: "a", Region: "asia", Status: "active"}}
	m.d.users["b"] = &memUser{User: User{ID: "b", Region: "asia", Status: "active"}}
	h := newTestNotificationHub()
	sa := h.subscribe("a", "asia", base)
	sb := h.subscribe("b", "asia", base)
	if err := h.pollMem(m); err != nil {
		t.Fatal(err)
	}
	if _, ok := recvNotification(sa); ok {
		t.Fatal("a: unexpected event")
	}
	m.d.users["a"].Status = "deleted"
	m.d.users["b"].Region = "europe"
	if err := h.pollMem(m); err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]*notificationSubscriber{"a": sa, "b": sb} {
		if v, ok := recvNotification(s); !ok || v != "closed" {
			t.Errorf("%s: want closed, got %q %v", name, v, ok)
		}
	}
}
