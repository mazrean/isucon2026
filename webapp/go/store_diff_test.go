//go:build memdiff

package isureal

// ISUREAL_STORE=1 の差分テスト: 書き込みは store (sb) が処理し、DB 経路 (sa, 同じ PG = 書き戻し結果) /
// asia1 メモリ (sb) / peer (sc) の GET 応答を比較する。
//   ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=15433 ISUREAL_LOG_LEVEL=error go test -tags memdiff -run TestStoreDiff -v .

import "testing"

func TestStoreDiff(t *testing.T) {
	storeEnabled = true
	defer func() { storeEnabled = false }()
	TestMemDiff(t)
	if storeCurrent == nil {
		t.Fatal("store was not enabled")
	}
	t.Logf("store handled/fallback: %s", storeCurrent.statsString())
}
