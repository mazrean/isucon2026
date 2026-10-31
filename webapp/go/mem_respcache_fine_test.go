package isureal

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMemRespCacheFineInvalidation(t *testing.T) {
	defer func(v, f bool) { memRespCacheOn, memCacheFine = v, f }(memRespCacheOn, memCacheFine)
	memRespCacheOn, memCacheFine = true, true
	row := func(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
	oct := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	nov := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	kOct := memoriesRespKey("v", oct, oct.AddDate(0, 1, 0), nil, "")
	kNov := memoriesRespKey("v", nov, nov.AddDate(0, 1, 0), nil, "")
	kDay := memoriesRespKey("v", oct.AddDate(0, 0, 4), oct.AddDate(0, 0, 5), nil, "") // 2026-10-05
	kExp := memRespKey{kind: memRespExport, user: "v"}
	type want struct{ oct, nov, day, exp bool }
	cases := []struct {
		name string
		w    want
		do   func(apply func(tbl, op string, v any))
	}{
		{"new viewer post in Nov", want{true, false, true, false}, func(apply func(string, string, any)) {
			apply("posts", "I", memPostRow{ID: "P9", AuthorID: "v", PostingWindowID: "W11", PostedAt: time.Now()})
		}},
		{"viewer post edit in Oct", want{false, true, false, false}, func(apply func(string, string, any)) {
			apply("posts", "U", memPostRow{ID: "P1", AuthorID: "v", PostingWindowID: "W10", Caption: "x", PostedAt: time.Now()})
		}},
		{"viewer post delete in Oct", want{false, true, false, false}, func(apply func(string, string, any)) {
			apply("posts", "D", memPostRow{ID: "P1", AuthorID: "v", PostingWindowID: "W10", PostedAt: time.Now()})
		}},
		{"viewer post, window unknown", want{false, false, false, false}, func(apply func(string, string, any)) {
			apply("posts", "I", memPostRow{ID: "P7", AuthorID: "v", PostingWindowID: "WX", PostedAt: time.Now()})
		}},
		{"viewer display name", want{false, false, false, false}, func(apply func(string, string, any)) {
			apply("users", "U", memUserRow{ID: "v", Username: "v", DisplayName: "new", Status: "active"})
		}},
		{"viewer deleted", want{false, false, false, false}, func(apply func(string, string, any)) {
			apply("users", "U", memUserRow{ID: "v", Username: "v", DisplayName: "V", Status: "deleted"})
		}},
		{"viewer row unchanged", want{true, true, true, true}, func(apply func(string, string, any)) {
			apply("users", "U", memUserRow{ID: "v", Username: "v", DisplayName: "V", Status: "active"})
		}},
		{"comment on Oct post", want{false, true, false, true}, func(apply func(string, string, any)) {
			apply("comments", "I", memCommentRow{ID: "c", PostID: "P1", AuthorID: "x", CreatedAt: time.Now()})
		}},
		{"realmoji on Oct post", want{false, true, false, true}, func(apply func(string, string, any)) {
			apply("realmojis", "I", memRealMojiRow{ID: "r", PostID: "P1", ReactorID: "x", CreatedAt: time.Now()})
		}},
		{"reactor deleted", want{false, true, false, true}, func(apply func(string, string, any)) {
			apply("users", "U", memUserRow{ID: "R", Status: "deleted"})
		}},
		{"window of Oct post", want{false, true, false, false}, func(apply func(string, string, any)) {
			apply("posting_windows", "U", memWindowRow{ID: "W10", Region: "asia", LocalDate: "2026-10-05"})
		}},
		{"other user post in Oct", want{true, true, true, true}, func(apply func(string, string, any)) {
			apply("posts", "I", memPostRow{ID: "P8", AuthorID: "o", PostingWindowID: "W10", PostedAt: time.Now()})
		}},
	}
	for _, cs := range cases {
		for _, race := range []bool{false, true} {
			d := newMemData()
			apply := func(tbl, op string, v any) {
				if err := d.applyChange(memChange{Tbl: tbl, Op: op, Row: row(v)}); err != nil {
					t.Fatal(err)
				}
			}
			apply("users", "I", memUserRow{ID: "v", Username: "v", DisplayName: "V", Status: "active"})
			apply("users", "I", memUserRow{ID: "R", Status: "active"})
			apply("posting_windows", "I", memWindowRow{ID: "W10", Region: "asia", LocalDate: "2026-10-05"})
			apply("posting_windows", "I", memWindowRow{ID: "W11", Region: "asia", LocalDate: "2026-11-02"})
			apply("posts", "I", memPostRow{ID: "P1", AuthorID: "v", PostingWindowID: "W10", PostedAt: time.Now()})
			fill := func() {
				f := d.rc.begin()
				if race {
					cs.do(apply)
				}
				// tags as memoriesTags / exportTags build them (coarse owner tag)
				d.rc.put(kOct, f, []byte("o"), []string{"ov", "uv", "pP1", "wW10", "uR"})
				d.rc.put(kDay, f, []byte("d"), []string{"ov", "uv", "pP1", "wW10", "uR"})
				d.rc.put(kNov, f, []byte("n"), []string{"ov", "uv"})
				d.rc.put(kExp, f, []byte("e"), []string{"ov", "uv", "wW10"})
			}
			fill()
			if !race {
				cs.do(apply)
			}
			got := want{d.rc.has(kOct), d.rc.has(kNov), d.rc.has(kDay), d.rc.has(kExp)}
			if got != cs.w {
				t.Fatalf("%s (race=%v): got %+v want %+v", cs.name, race, got, cs.w)
			}
		}
	}
	if memRespInvN[rcWhyPostMonth].Load() == 0 || memRespInvN[rcWhyUserNoop].Load() == 0 || memRespInvRemoved[rcWhyPostBroad].Load() == 0 {
		t.Fatalf("cause counters not counted")
	}
}

// flag off: the Memories entry keeps the coarse owner tag (any viewer post drops every month).
func TestMemRespCacheFineOff(t *testing.T) {
	defer func(v, f bool) { memRespCacheOn, memCacheFine = v, f }(memRespCacheOn, memCacheFine)
	memRespCacheOn, memCacheFine = true, false
	d := newMemData()
	oct := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	k := memoriesRespKey("v", oct, oct.AddDate(0, 1, 0), nil, "")
	d.rc.put(k, d.rc.begin(), []byte("o"), []string{"ov", "uv"})
	b, _ := json.Marshal(memPostRow{ID: "P9", AuthorID: "v", PostingWindowID: "W11", PostedAt: time.Now()})
	if err := d.applyChange(memChange{Tbl: "posts", Op: "I", Row: b}); err != nil {
		t.Fatal(err)
	}
	if d.rc.has(k) {
		t.Fatal("flag off: entry survived a viewer post")
	}
}
