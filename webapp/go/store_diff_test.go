//go:build memdiff

package isureal

// ISUREAL_STORE=1 の差分テスト: 書き込みは store (sb) が処理し、DB 経路 (sa, 同じ PG = 書き戻し結果) /
// asia1 メモリ (sb) / peer (sc) の GET 応答を比較する。
//   ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error go test -tags memdiff -run TestStoreDiff -v .

import (
	"testing"
	"time"
)

func TestStoreDiff(t *testing.T) {
	storeEnabled = true
	defer func() { storeEnabled = false }()
	TestMemDiff(t)
	if storeCurrent == nil {
		t.Fatal("store was not enabled")
	}
	t.Logf("store handled/fallback: %s", storeCurrent.statsString())
}

// TestStoreDiffWAL: ISUREAL_STORE_WAL 有効 + AWAIT_PG=0 (PG は純粋な write-behind)。DB 経路の読みは PG バリア (store_pgbarrier.go) が待つ。
func TestStoreDiffWAL(t *testing.T) {
	oldDir, oldAwait, oldOn := storeWALDir, storeAwaitPG, storeWALOn
	storeWALDir, storeAwaitPG, storeWALOn = t.TempDir(), false, true
	defer func() {
		storeWALDir, storeAwaitPG, storeWALOn = oldDir, oldAwait, oldOn
		storePGBarrierFn, storeCurrentSeqFn, storePGAppliedFn = nil, nil, nil
	}()
	// 1 process に pump と peer が同居するので、storePGRolePeer (process 全体で 1 つ) が peer に倒れると
	// 全要求が peer の標本待ち (上限まで) になる。ここでは store が 1 つなので asia1 の seq 待ちに揃える。
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				storePGRolePeer.Store(false)
			}
		}
	}()
	TestStoreDiff(t)
	if storeCurrent == nil || storeCurrent.wal == nil {
		t.Fatal("store WAL was not enabled")
	}
	t.Logf("pg applied=%d current=%d", storePGApplied.Load(), storeCurrentSeq())
}
