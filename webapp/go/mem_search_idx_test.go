package isureal

import "testing"

func checkSearchIdx(t *testing.T, d *memData) {
	t.Helper()
	list := d.sortedUsers()
	if len(d.searchIdx) != len(list) {
		t.Fatalf("idx len %d != %d", len(d.searchIdx), len(list))
	}
	for i, u := range list {
		e := d.searchIdx[i]
		un := string(d.searchBuf[e.off : e.off+e.ulen])
		dn := string(d.searchBuf[e.off+e.ulen : e.off+e.ulen+e.dlen])
		if un != u.normUsername || dn != u.normDisplay {
			t.Fatalf("%d: idx (%q,%q) != (%q,%q)", i, un, dn, u.normUsername, u.normDisplay)
		}
	}
}

func TestMemSearchIdx(t *testing.T) {
	d := newMemData()
	d.applyUser(memUserRow{ID: "b", Username: "Bob", DisplayName: "ボブ", Status: "active"})
	d.applyUser(memUserRow{ID: "a", Username: "alice", DisplayName: "Ａｌｉｃｅ Ⅸ", Status: "active"})
	d.applyUser(memUserRow{ID: "c", Username: "carol", DisplayName: "", Status: "active"})
	checkSearchIdx(t, d)
	d.applyUser(memUserRow{ID: "b", Username: "Bob", DisplayName: "Robert", Status: "active"})
	if !d.sortedDirty {
		t.Fatal("display change must mark the index stale")
	}
	checkSearchIdx(t, d)
	d.applyUser(memUserRow{ID: "b", Username: "Bob", DisplayName: "Robert", Status: "deleted"})
	if d.sortedDirty {
		t.Fatal("status-only change should not rebuild")
	}
	checkSearchIdx(t, d)
}
