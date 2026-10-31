package isureal

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ISUREAL_MEM_SYNC_VISIBLE=1 (既定 0, asia1 と eu/am で同じ値。ISUREAL_MEM_WATERMARK=1 が前提):
// eu/am の「住人」の GET は wm を待たずにメモリで答える。
//
// pump (asia1): waitAcked が wm モードの peer の ack も待つ (= 同期の複製)。対象は asia1 が 2xx を返す
// すべての書き込み (STORE の commitWait、DB 経路の memWriteBarrier、eu/am から転送された分、login/logout)。
// 閲覧者の地域で絞る判定は入れていない (安全な部分集合: 全地域へ同期)。
//
// peer: 次をすべて満たす GET だけ wm を省く。それ以外は従来の wm の待ち。
//   - dump が「pump は同期モード」と言った (pumpSync)。混在構成では従来どおり。
//   - メモリが on (lease 有効)。lease は seq を欠番なく req.Seq まで適用した push でだけ延びる。
//   - caughtUp: 最後に知った pump の seq まで適用済み (wm 通知が先行 seq を言ったら false)。
//   - 要求の Bearer トークンがメモリのセッションにあり、利用者が active、メモリ上の users.region がこの地域、
//     地域内で無効化中 (marks) でない。トークン無し・未知・判定不能は待つ側。
//
// 正しさ: 読みの到着前に 2xx を返した書き込みは、asia1 がこの peer の ack (= 適用済み) を確認してから返している。
// ack を待ちきれず lease 切れで先へ進んだ書き込みがある間、この peer は lease が切れて not-ready (DB 経路)。
// lease が戻るのはその seq 以上まで適用した push を受けてから。
var memSyncVisible = memWatermark && os.Getenv("ISUREAL_MEM_SYNC_VISIBLE") == "1"

// memLocalRegion: この peer の地域 (setup で設定)。
var memLocalRegion string

type memSyncSkipKey struct{}

// syncReadOK: 読む時点で、wm を省いてよい状態か。
func (m *memStore) syncReadOK() bool {
	return memSyncVisible && m.on() && m.w.pumpSync.Load() && m.w.caughtUp.Load()
}

// syncResident: 要求の利用者がこの地域の住人で、待たない読みに回してよいか。判定できなければ false。
func (m *memStore) syncResident(req *http.Request, region string) bool {
	if m == nil || region == "" || !m.syncReadOK() {
		return false
	}
	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return false
	}
	sum := sha256.Sum256([]byte(strings.TrimPrefix(auth, "Bearer ")))
	th := hex.EncodeToString(sum[:])
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.d == nil {
		return false
	}
	s, ok := m.d.sessions[th]
	if !ok {
		return false
	}
	u := m.d.users[s.UserID]
	if !u.active() || u.Region != region {
		return false
	}
	return !m.marks.marked(s.UserID)
}

// ---- stats: 同期 (waitAcked) にかかった時間 (asia1) と wm を省いた読みの数 (peer)。10 秒ごとに slog ----

type memSyncStatsT struct {
	mu      sync.Mutex
	waits   []time.Duration
	skipped atomic.Int64
	once    sync.Once
}

var memSyncStats memSyncStatsT

func (s *memSyncStatsT) since(start time.Time) {
	d := time.Since(start)
	s.once.Do(func() { go s.loop() })
	s.mu.Lock()
	if len(s.waits) < 1<<16 {
		s.waits = append(s.waits, d)
	}
	s.mu.Unlock()
}

func (s *memSyncStatsT) loop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		w := s.waits
		s.waits = nil
		s.mu.Unlock()
		if len(w) == 0 {
			continue
		}
		p50, p90, p99 := memPercentiles(w)
		slog.Info("mem: sync-visible ack wait", "n", len(w), "p50", p50, "p90", p90, "p99", p99)
	}
}
