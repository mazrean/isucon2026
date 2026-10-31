package isureal

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestImgHotID(t *testing.T) {
	plain := "ada34482-4e05-48ce-8c76-0f8976621688"
	cases := []struct {
		id   string
		want bool
	}{
		{plain, false},
		{newHotImageID(uuid.NewString()), true},
		{newAsiaLocalImageID(), true},
		{newLocalImageID("europe"), true},
		{newLocalImageID("america"), true},
		{"short", false},
		{plain[:30] + "d0d0d1", false},
	}
	for _, c := range cases {
		if got := imgHotID(c.id); got != c.want {
			t.Errorf("imgHotID(%q) = %v, want %v", c.id, got, c.want)
		}
	}
	id := newHotImageID(uuid.NewString())
	if len(id) != 36 || !safeImageID(id) || localImgOriginOf(id) != "" || isAsiaLocalID(id) {
		t.Fatalf("hot id %q must stay a 36-char untagged-origin id", id)
	}
	if !strings.Contains(imageSelectHot, "created_at >= '2026-10-28 06:00:00+00'") {
		t.Fatalf("hot query must carry the partition bound as a literal: %s", imageSelectHot)
	}
}
