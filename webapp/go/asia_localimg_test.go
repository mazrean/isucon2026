package isureal

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

func setupAsiaLocalImgTest(t *testing.T) {
	t.Helper()
	oldOn, oldAddr, oldDir, oldSecret, oldLI := asiaLocalImgOn, asiaLocalImgAddr, asiaStore.dir, forwardSecret, localImgOn
	asiaLocalImgOn, forwardSecret, localImgOn = true, "test-secret", false
	asiaStore.dir = t.TempDir()
	t.Cleanup(func() {
		asiaLocalImgOn, asiaLocalImgAddr, asiaStore.dir, forwardSecret, localImgOn = oldOn, oldAddr, oldDir, oldSecret, oldLI
	})
}

func TestAsiaLocalImgID(t *testing.T) {
	setupAsiaLocalImgTest(t)
	id := newImageID(t.Context())
	if !isAsiaLocalID(id) || id[14] != '4' || localImgOriginOf(id) != "" {
		t.Fatalf("asia id %q", id)
	}
	// an eu/am-forwarded image keeps the origin's ID
	ctx := context.WithValue(t.Context(), localImgMetaKey{}, &localImgMeta{id: newLocalImageID("europe")})
	if got := newImageID(ctx); isAsiaLocalID(got) || localImgOriginOf(got) != "europe" {
		t.Fatalf("forwarded id %q", got)
	}
	asiaLocalImgOn = false
	if isAsiaLocalID(newImageID(t.Context())) {
		t.Fatal("flag off still tags")
	}
}

// Differential: what asia1 serves from its own store and what another host
// pulls are byte-identical to the uploaded image (= what PG used to return).
func TestAsiaLocalImgKeepServePull(t *testing.T) {
	setupAsiaLocalImgTest(t)
	good := testJPEG(t, 120, 90)
	id := newAsiaLocalImageID()
	if !asiaLocalImgKeep(t.Context(), id, "owner-1", good) {
		t.Fatal("not kept")
	}
	own := localImgOwn(id)
	if own == nil || own.Owner != "owner-1" || own.MediaType != "image/jpeg" || !bytes.Equal(own.Data, good) {
		t.Fatalf("own %v", own)
	}
	// restart: a fresh store over the same directory still has it (fsynced files)
	if obj, _ := (&localImgStore{dir: asiaStore.dir}).read(id); obj == nil || !bytes.Equal(obj.Data, good) {
		t.Fatal("not durable on disk")
	}

	// not kept: forwarded (bytes at eu/am), untagged ID, flag off
	ectx := context.WithValue(t.Context(), localImgMetaKey{}, &localImgMeta{id: "x"})
	if asiaLocalImgKeep(ectx, newAsiaLocalImageID(), "o", good) || asiaLocalImgKeep(t.Context(), "00000000-0000-4000-8000-000000000000", "o", good) {
		t.Fatal("kept a non-asia image")
	}

	// asia1 serves /internal/localimg/:id
	asia := echo.New()
	registerLocalImgRoutes(asia)
	srv := httptest.NewServer(asia)
	t.Cleanup(srv.Close)

	// reader host (eu/am/asia2): flag off, pulls on DB miss
	asiaLocalImgOn = false
	asiaLocalImgAddr = strings.TrimPrefix(srv.URL, "http://")
	p, err := localImgPull(t.Context(), id)
	if err != nil || p == nil || p.Owner != "owner-1" || !bytes.Equal(p.Data, good) {
		t.Fatalf("pull %v %v", p, err)
	}
	if localImgOwn(id) != nil {
		t.Fatal("reader host served from its own (empty) store")
	}
	if p, err := localImgPull(t.Context(), newAsiaLocalImageID()); p != nil || err != nil {
		t.Fatalf("unknown asia id: %v %v", p, err)
	}
	if p, err := localImgPull(t.Context(), "00000000-0000-4000-8000-000000000000"); p != nil || err != nil {
		t.Fatalf("untagged id pulled: %v %v", p, err)
	}

	// asia1 never pulls from itself
	asiaLocalImgOn = true
	if p, _ := localImgPull(t.Context(), id); p != nil {
		t.Fatal("asia1 pulled from itself")
	}

	// rolled-back write: dropped
	id2 := newAsiaLocalImageID()
	asiaLocalImgKeep(t.Context(), id2, "owner-2", good)
	asiaLocalImgDrop(id2)
	if localImgOwn(id2) != nil {
		t.Fatal("dropped image still served")
	}

	// initialize: purge empties the store, pulls then miss
	asiaLocalImgPurge()
	if localImgOwn(id) != nil || countFiles(t, asiaStore.dir) != 0 {
		t.Fatal("purge left the image")
	}
	asiaLocalImgOn = false
	if p, err := localImgPull(t.Context(), id); p != nil || err != nil {
		t.Fatalf("after purge: %v %v", p, err)
	}
	// keep still works after purge (dir recreated)
	asiaLocalImgOn = true
	if !asiaLocalImgKeep(t.Context(), newAsiaLocalImageID(), "o", good) {
		t.Fatal("keep after purge")
	}
}
