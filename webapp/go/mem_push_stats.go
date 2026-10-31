package isureal

import (
	"bytes"
	"compress/gzip"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// ISUREAL_MEM_PUSH_STATS=1 (既定 0): push の遅れを段階ごとに 10 秒ごとに slog ("mem: push stats")。
//   pump 側 (send):  bytes (JSON) / wire (gzip 後) / batches / changes / ack = SentAt から応答受信まで
//   peer 側 (push):  net = 受信 (handler 開始) − SentAt (asia1 のスケジューリング + 回線 + 時計ずれ)
//                    lock = r.mu 取得 + 並び替え待ち / apply = 適用 (m.mu 書きロック待ち + 欠番 pull 込み) / pulls = 欠番 pull 回数
// ISUREAL_MEM_PUSH_GZIP=1 (既定 0): pump → peer の push 本文を gzip (BestSpeed) で送る (memPushGzipMin 以上のとき)。
//   受け側は Content-Encoding: gzip を常に解釈する (フラグに依らない) ので、送り側だけのフラグで安全に切り替えられる。
//   ただし受け側が古いバイナリだと 400 → invalidate になるので、全ホスト同時にデプロイすること。

var (
	memPushStatsOn = os.Getenv("ISUREAL_MEM_PUSH_STATS") == "1"
	memPushGzip    = os.Getenv("ISUREAL_MEM_PUSH_GZIP") == "1"
)

const memPushGzipMin = 1024

var memGzipPool = sync.Pool{New: func() any { w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed); return w }}

// memMaybeGzip: フラグ on で path が push、かつ十分大きいときだけ圧縮する。
func memMaybeGzip(path string, data []byte) ([]byte, bool) {
	if !memPushGzip || path != "/internal/mem/push" || len(data) < memPushGzipMin {
		return data, false
	}
	var buf bytes.Buffer
	zw := memGzipPool.Get().(*gzip.Writer)
	zw.Reset(&buf)
	if _, err := zw.Write(data); err != nil || zw.Close() != nil {
		memGzipPool.Put(zw)
		return data, false
	}
	memGzipPool.Put(zw)
	return buf.Bytes(), true
}

// memRequestBody: Content-Encoding: gzip なら展開する。
func memRequestBody(req *http.Request) (io.Reader, func(), error) {
	if req.Header.Get("Content-Encoding") != "gzip" {
		return req.Body, func() {}, nil
	}
	zr, err := gzip.NewReader(req.Body)
	if err != nil {
		return nil, nil, err
	}
	return zr, func() { zr.Close() }, nil
}

type memDurSet struct{ d []time.Duration }

func (s *memDurSet) add(d time.Duration) {
	if len(s.d) < 1<<15 {
		s.d = append(s.d, d)
	}
}

func (s *memDurSet) attrs(name string) []any {
	p50, p90, p99 := memPercentiles(s.d)
	return []any{name + "50", p50, name + "90", p90, name + "99", p99}
}

type memPushStatsData struct {
	// pump
	n, bytes, wire, batches, changes, maxBytes int
	ack                                        memDurSet
	// peer
	rn, pulls             int
	net, lock, apply, tot memDurSet
}

type memPushStatsT struct {
	mu   sync.Mutex
	once sync.Once
	memPushStatsData
}

var memPushStats memPushStatsT

func (s *memPushStatsT) start() { s.once.Do(func() { go s.loop() }) }

func (s *memPushStatsT) recordSend(bytes, wire int, req *memPushRequest, ack time.Duration) {
	if !memPushStatsOn {
		return
	}
	s.start()
	ch := 0
	for _, b := range req.Batches {
		ch += len(b.Changes)
	}
	s.mu.Lock()
	s.n++
	s.bytes += bytes
	s.wire += wire
	s.batches += len(req.Batches)
	s.changes += ch
	s.maxBytes = max(s.maxBytes, bytes)
	s.ack.add(ack)
	s.mu.Unlock()
}

func (s *memPushStatsT) recordRecv(sentAt int64, recv, locked, done time.Time, pulls int) {
	if !memPushStatsOn {
		return
	}
	s.start()
	if locked.IsZero() { // 並び替え待ちの途中で epoch が変わった等
		locked = done
	}
	s.mu.Lock()
	s.rn++
	s.pulls += pulls
	s.net.add(time.Duration(recv.UnixNano() - sentAt))
	s.lock.add(locked.Sub(recv))
	s.apply.add(done.Sub(locked))
	s.tot.add(time.Duration(done.UnixNano() - sentAt))
	s.mu.Unlock()
}

func (s *memPushStatsT) loop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		cp := s.memPushStatsData
		s.memPushStatsData = memPushStatsData{}
		s.mu.Unlock()
		if cp.n > 0 {
			a := []any{"n", cp.n, "bytesAvg", cp.bytes / cp.n, "bytesMax", cp.maxBytes, "wireAvg", cp.wire / cp.n,
				"batchesAvg", float64(cp.batches) / float64(cp.n), "changesAvg", float64(cp.changes) / float64(cp.n)}
			slog.Info("mem: push stats (pump)", append(a, cp.ack.attrs("ack")...)...)
		}
		if cp.rn > 0 {
			a := []any{"n", cp.rn, "pulls", cp.pulls}
			a = append(a, cp.net.attrs("net")...)
			a = append(a, cp.lock.attrs("lock")...)
			a = append(a, cp.apply.attrs("apply")...)
			a = append(a, cp.tot.attrs("tot")...)
			slog.Info("mem: push stats (peer)", a...)
		}
	}
}

// memIdleConns: peer ごとに保つ keep-alive 接続数。ISUREAL_MEM_WM_HB=1 は並列 push / wm 通知を 16+16 本にするので、
// 16 では溢れた分が毎回 TCP を張り直す (asia1→eu/am で +1 RTT)。
func memIdleConns() int {
	if os.Getenv("ISUREAL_MEM_WM_HB") == "1" {
		return 64
	}
	return 16
}
