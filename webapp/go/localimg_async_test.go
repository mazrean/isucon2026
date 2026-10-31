package isureal

import (
	"bytes"
	"testing"
	"time"
)

func withAsiaAsync(t *testing.T) {
	setupAsiaLocalImgTest(t)
	old := asiaLocalImgAsync
	asiaLocalImgAsync = true
	t.Cleanup(func() { asiaAsyncWG.Wait(); asiaLocalImgAsync = old })
}

// ISUREAL_ASIA_LOCALIMG_ASYNC: 書いた画像は、裏の sync を待たずにすぐ読める。
func TestAsiaLocalImgAsyncReadableImmediately(t *testing.T) {
	withAsiaAsync(t)
	good := testJPEG(t, 120, 90)
	id := newAsiaLocalImageID()
	if !asiaLocalImgKeep(t.Context(), id, "owner-1", good) {
		t.Fatal("not kept")
	}
	if own := localImgOwn(id); own == nil || own.Owner != "owner-1" || !bytes.Equal(own.Data, good) {
		t.Fatalf("not readable right after keep: %v", own)
	}
}

// on のとき、裏で sync が走る (syncer の回数が増え、待ちが 0 に戻る)。
func TestAsiaLocalImgAsyncSyncsBehind(t *testing.T) {
	withAsiaAsync(t)
	good := testJPEG(t, 64, 48)
	before, synced := asiaSyncer.batches.Load()+asiaSyncer.members.Load(), asiaAsyncSynced.Load()
	for range 5 {
		if !asiaLocalImgKeep(t.Context(), newAsiaLocalImageID(), "o", good) {
			t.Fatal("not kept")
		}
	}
	done := make(chan struct{})
	go func() { asiaAsyncWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("background sync never finished")
	}
	if asiaAsyncSynced.Load()-synced != 5 || asiaAsyncPending.Load() != 0 || asiaAsyncFailed.Load() != 0 {
		t.Fatalf("synced=%d pending=%d failed=%d", asiaAsyncSynced.Load()-synced, asiaAsyncPending.Load(), asiaAsyncFailed.Load())
	}
	if asiaSyncer.batches.Load()+asiaSyncer.members.Load() <= before {
		t.Fatal("no sync ran")
	}
}
