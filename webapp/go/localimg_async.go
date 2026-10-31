package isureal

import (
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

// ISUREAL_ASIA_LOCALIMG_ASYNC=1 (既定 0。使うかはユーザーの承認次第): asia1 の画像の正本 (ASIA_LOCALIMG) の書き込みで、
// data と meta をページキャッシュに書いた時点で返る。永続化 (syncfs のグループ / FSYNC モードなら fdatasync) は裏の goroutine。
//
// 耐久性は弱くなる: 201 の後、裏の sync が終わる前に asia1 の OS が落ちると、投稿は PG にあるのに画像のバイトが失われうる
// (プロセスだけが落ちた場合はページキャッシュに残るので失われない)。既定 0 では何も変わらない。
// ASIA_LOCALIMG_SEG=1 のときはセグメント側が従来どおり同期する (このフラグは効かない)。
var (
	asiaLocalImgAsync = os.Getenv("ISUREAL_ASIA_LOCALIMG_ASYNC") == "1"
	asiaAsyncPending  atomic.Int64 // 裏で sync を待っている書き込み
	asiaAsyncSynced   atomic.Int64 // 裏の sync が終わった書き込み
	asiaAsyncFailed   atomic.Int64
	asiaAsyncWG       sync.WaitGroup // テスト用
)

func asiaLocalImgSyncBehind(paths ...string) {
	asiaAsyncPending.Add(1)
	asiaAsyncWG.Go(func() {
		defer asiaAsyncPending.Add(-1)
		if err := asiaSyncer.wait(paths...); err != nil {
			asiaAsyncFailed.Add(1)
			slog.Warn("asia localimg async sync", "error", err)
			return
		}
		asiaAsyncSynced.Add(1)
	})
}
