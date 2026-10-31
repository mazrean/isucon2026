package isureal

import (
	"context"
	"testing"
	"time"
)

// DB 経路との差分は memdiff (MD_HOT_EXTRA_DAYS=45 go test -tags memdiff -run TestMemDiff) で見る。
func TestMemHotLoadFrom(t *testing.T) {
	defer func(o int) { memHotExtraDays = o }(memHotExtraDays)
	d := &memData{windows: map[string]*memWindow{}}
	min := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)

	memHotExtraDays = 0 // off: unchanged, no query (q nil)
	if got := d.hotLoadFrom(context.Background(), nil, &min); got != &min {
		t.Fatalf("off: got %v", got)
	}
	if got := d.hotLoadFrom(context.Background(), nil, nil); got != nil {
		t.Fatalf("off nil: got %v", got)
	}
	memHotExtraDays = 30 // on but no windows and no open window: unknown base, unchanged (no query)
	if got := d.hotLoadFrom(context.Background(), nil, nil); got != nil {
		t.Fatalf("no windows: got %v", got)
	}
	if b, ok := d.hotBase(&min); !ok || !b.Equal(min) {
		t.Fatalf("hotBase(min) = %v %v", b, ok)
	}
	d.windows["w1"] = &memWindow{PostingWindow: PostingWindow{LocalDate: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}}
	d.windows["w2"] = &memWindow{PostingWindow: PostingWindow{LocalDate: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}}
	if b, ok := d.hotBase(nil); !ok || !b.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("hotBase(nil) = %v %v", b, ok)
	}
	// MEM_FULL on as well: extension is skipped (no query, q nil) and MEM_FULL's earliest period wins
	func() {
		defer func(o bool) { memFullLoaders = o }(memFullLoaders)
		memFullLoaders = true
		got := memFullMinDate(d, d.hotLoadFrom(context.Background(), nil, &min))
		if got == nil || !got.Equal(time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("MEM_FULL + extend: got %v", got)
		}
	}()
	// hotBase(nil) must agree with setHotFrom(nil)
	d.setHotFrom(nil)
	if b, _ := d.hotBase(nil); !b.Equal(d.hotFrom) {
		t.Fatalf("hotBase %v != setHotFrom %v", b, d.hotFrom)
	}
}
