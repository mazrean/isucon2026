package isureal

import (
	"sync"
	"testing"
	"time"
)

// login (レーン 0) と投稿 (レーン 0 + 画像レーン 1) が同じバッチに入っても、
// STORE_OWN_SEQ の login は画像レーンの書き戻しを待たない。投稿は自分の画像レーンを待つ。
func TestStoreOwnSeqLoginDoesNotWaitPostImage(t *testing.T) {
	lanes := 1 + storeImageLanes
	e := &storeEngine{}
	for i := 0; i < lanes; i++ {
		l := &storeLane{idx: i}
		l.cond = sync.NewCond(&l.mu)
		l.queuedSeq = 1
		e.lanes = append(e.lanes, l)
	}
	batch := make(storeSeqs, lanes)
	batch[0], batch[1] = 1, 1 // バッチ全体: メタデータと画像レーン 1
	own := make([]bool, lanes)
	own[1] = true
	login := storeOwnSeqs(batch, nil, lanes)
	post := storeOwnSeqs(batch, own, lanes)
	if login[0] != 1 || login[1] != 0 || post[0] != 1 || post[1] != 1 {
		t.Fatalf("login=%v post=%v", login, post)
	}
	// WAL の失敗の印はそのまま (全レーンを待つ)
	if m := append(append(storeSeqs{}, batch...), 1); len(storeOwnSeqs(m, nil, lanes)) != lanes+1 {
		t.Fatal("marker dropped")
	}

	// レーン 0 だけ書き戻し済み、画像レーン 1 は未了
	e.lanes[0].mu.Lock()
	e.lanes[0].flushedSeq = 1
	e.lanes[0].mu.Unlock()
	done := make(chan struct{})
	go func() { e.waitFlushed(login); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("login waited for the post's image lane")
	}
	postDone := make(chan struct{})
	go func() { e.waitFlushed(post); close(postDone) }()
	select {
	case <-postDone:
		t.Fatal("post returned before its image lane was flushed")
	case <-time.After(50 * time.Millisecond):
	}
	l := e.lanes[1]
	l.mu.Lock()
	l.flushedSeq = 1
	l.cond.Broadcast()
	l.mu.Unlock()
	select {
	case <-postDone:
	case <-time.After(time.Second):
		t.Fatal("post not released after flush")
	}
	// 画像以外のレーン (メタデータ) は依存を保つため、login も待つ
	if login[0] == 0 {
		t.Fatal("login must still wait for the metadata lane")
	}
}
