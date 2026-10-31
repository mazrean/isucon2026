package isureal

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// 分散モード: asia1 (pump) が changelog バッチに連番を振って europe1/america1 (peer) に配信する。
// peer は DB を読まない。初期状態は pump のメモリのダンプ (/internal/mem/dump) から作る。
// pump の書き込みレスポンスは全 peer の ack (または peer ダウン判定) を待つ。

const memBufferBatches = 8192

type memBatch struct {
	Seq     int64       `json:"q"`
	Changes []memChange `json:"c"`
	at      time.Time   // publish 時刻 (pump 内のみ。MEM_PUSH_STATS の queue)
}

type memPushRequest struct {
	Epoch   string     `json:"e"`
	Seq     int64      `json:"q"` // pump の最新連番 (heartbeat でも送る。peer はこれで欠番を検知)
	Batches []memBatch `json:"b"`
	// pump の送信時刻 (unix ns)。peer の lease は min(受信時刻, SentAt) + memLease まで (遅れて届いた push で延びない)。
	SentAt int64 `json:"t"`
	// probe: down 扱いの peer への送信。peer は反映するが lease は延ばさない (pump が lease の上界を把握するため)。
	Probe bool `json:"p,omitempty"`
	// watermark (ISUREAL_MEM_WATERMARK): Seq までのバッチに、この時刻より前に応答が返ったコミットがすべて含まれる。
	WM int64 `json:"w,omitempty"`
	// ISUREAL_STORE_WAL: asia1 の標本 (時刻, その後に読んだ store の seq, PG に反映済みの seq)。store_pgbarrier.go
	PT int64 `json:"pt,omitempty"`
	PS int64 `json:"ps,omitempty"`
	PA int64 `json:"pa,omitempty"`
	// SS (STORE の PG 読みの待ちが有効なときだけ): ps.mu の下で読んだ storeCurrentSeq。Seq までのバッチに
	// store の seq SS までの変更がすべて含まれる (store は publish の後で seq を進める)。MEM_FULL の冷たい読み (mem_full.go) が使う。
	SS int64 `json:"ss,omitempty"`
}

// peer の lease は「pump が送った push の送信時刻 + memLease」を超えない。
// pump は peer ごとに lease を与えうる push の最終送信時刻 (lastGrantAt) を持ち、
// ack の無い peer は lastGrantAt + memLease + memClockSkew を過ぎれば必ず not-ready なので待たずに進む。
// push は peer ごとに最大 memMaxInflight 本まで並列に送る (stop-and-wait をやめて 1 RTT で ack)。
const (
	memLease          = 1 * time.Second
	memPushTimeout    = 3 * time.Second
	memHeartbeatEvery = 250 * time.Millisecond
	memClockSkew      = 200 * time.Millisecond
	memMaxInflight    = 4
)

type memPushResponse struct {
	OK bool `json:"ok"`
}

// memReorderWait: 先行する push の到着を待つ上限。超えると欠番を pull する (r.mu を保持したまま asia1 へ 1 RTT)。
// ISUREAL_MEM_REORDER_WAIT_MS で延ばせる (既定 50ms)。待ちは cond.Wait で r.mu を手放すので他の push / wm は進む。
// 適用順は変わらず pull も残るので、正しさは同じ (変わるのは遅れの形だけ)。
var memReorderWait = envDurationMS("ISUREAL_MEM_REORDER_WAIT_MS", 50*time.Millisecond)

var memHTTPClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: memIdleConns(),
		IdleConnTimeout:     5 * time.Minute,
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	},
}

func memAuthOK(c *echo.Context) bool {
	req := c.Request()
	if forwardSecret == "" || subtle.ConstantTimeCompare([]byte(req.Header.Get(forwardedSecretHeader)), []byte(forwardSecret)) != 1 {
		return false
	}
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || strings.HasPrefix(ip.String(), "192.168."))
}

func memNewEpoch() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ---------------- pump side ----------------

type memPeer struct {
	addr     string
	pending  []memBatch
	inflight int
	acked    int64
	down     bool
	// lease を与えうる (probe でない) push の最終送信時刻。peer の lease はこれ + memLease (+時計ずれ) より前に必ず切れる。
	lastGrantAt time.Time
	// この epoch で invalidate (not-ready 化) の成功を確認済み
	invalidatedEpoch string
	// dump 取得時に watermark で読みを待つと申告した (ISUREAL_MEM_WATERMARK)。この peer の ack は待たない。
	wmMode     bool
	wmInflight int
}

type memPeers struct {
	p     *memPump
	mu    sync.Mutex
	cond  *sync.Cond
	epoch string
	seq   int64
	buf   []memBatch
	peers []*memPeer
	wm    int64 // seq に対して有効な watermark (unix ns)
}

func newMemPeersFromEnv(p *memPump) *memPeers {
	list := strings.TrimSpace(os.Getenv("ISUREAL_MEM_PEERS"))
	if list == "" {
		return nil
	}
	ps := &memPeers{p: p, epoch: memNewEpoch()}
	ps.cond = sync.NewCond(&ps.mu)
	for _, a := range strings.Split(list, ",") {
		if a = strings.TrimSpace(a); a != "" {
			// 前プロセスが与えた lease を考慮して起動時刻で初期化 (保守的)
			ps.peers = append(ps.peers, &memPeer{addr: a, lastGrantAt: time.Now()})
		}
	}
	return ps
}

// publish は cycleMu 保持中に呼ばれる (連番順)。
func (ps *memPeers) publish(changes []memChange) { ps.publishWM(changes, 0) }

// publishWM: wm > 0 なら、このバッチ (新しい ps.seq) に対して有効な watermark を同じ push に載せる
// (ISUREAL_MEM_WM_HB)。wm は「このバッチを作った poll のスナップショット取得より前の asia1 時刻」であること。
func (ps *memPeers) publishWM(changes []memChange, wm int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.seq++
	if wm > ps.wm {
		ps.wm = wm // 新しい ps.seq に対して有効: x より前に応答したコミットはこのバッチ以前に含まれる
	}
	b := memBatch{Seq: ps.seq, Changes: changes}
	if memPushStatsOn {
		b.at = time.Now()
	}
	ps.buf = append(ps.buf, b)
	if len(ps.buf) > memBufferBatches {
		ps.buf = append([]memBatch(nil), ps.buf[len(ps.buf)-memBufferBatches/2:]...)
	}
	for _, peer := range ps.peers {
		peer.pending = append(peer.pending, b)
		ps.kick(peer, false)
	}
}

// kick は ps.mu 保持中に呼ぶ。未送信分 (または heartbeat) を新しい並列 push として送る。
func (ps *memPeers) kick(peer *memPeer, beat bool) {
	if peer.inflight >= memPushInflight(peer) || (len(peer.pending) == 0 && !beat) {
		return
	}
	batches := peer.pending
	peer.pending = nil
	last := ps.seq
	if len(batches) > 0 {
		last = batches[len(batches)-1].Seq
	}
	now := time.Now()
	if memPushStatsOn && len(batches) > 0 && !batches[0].at.IsZero() {
		memPushStats.recordQueue(now.Sub(batches[0].at))
	}
	req := memPushRequest{Epoch: ps.epoch, Seq: last, Batches: batches, SentAt: now.UnixNano(), Probe: peer.down}
	req.PT, req.PS, req.PA = storePGSample()
	if last == ps.seq && storePGActiveAsia() {
		req.SS = storeCurrentSeqFn()
	}
	if last == ps.seq {
		req.WM = ps.wm // ps.wm は ps.seq に対して有効
	}
	if !req.Probe {
		peer.lastGrantAt = now
	}
	peer.inflight++
	go ps.send(peer, req)
}

func (ps *memPeers) send(peer *memPeer, req memPushRequest) {
	body, err := memHTTPPost(peer.addr, "/internal/mem/push", req, memPushTimeout)
	var resp memPushResponse
	if err == nil {
		err = json.Unmarshal(body, &resp)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	peer.inflight--
	if ps.epoch == req.Epoch {
		if err != nil {
			// 失敗した peer は lease (lastGrantAt + memLease + memClockSkew) が切れるまで待たれる。以後は probe で様子を見る。
			// 欠番は peer が次の push で検知して取り直す。
			if !peer.down {
				slog.Warn("mem: peer push failed; marking down", "peer", peer.addr, "error", err)
			}
			peer.down = true
		} else {
			peer.down = false
			if !req.Probe && len(req.Batches) > 0 {
				memPushLastRTT.Store(time.Now().UnixNano() - req.SentAt) // ISUREAL_ASIA_POST_CAP_LAG_MS
			}
			// ok=false: peer は not-ready (DB 経路) なので待つ必要なし
			if peer.acked < req.Seq {
				peer.acked = req.Seq
			}
		}
	}
	ps.cond.Broadcast()
	ps.kick(peer, false)
}

// heartbeat: 送信中でない peer に空 push を送る (lease 延長 + 欠番検知)。
func (ps *memPeers) heartbeatLoop() {
	t := time.NewTicker(memHeartbeatEvery)
	defer t.Stop()
	for range t.C {
		if !ps.p.m.ready.Load() {
			continue
		}
		ps.mu.Lock()
		for _, peer := range ps.peers {
			if peer.inflight == 0 {
				ps.kick(peer, true)
			}
		}
		ps.mu.Unlock()
	}
}

// waitAcked は現在の連番までを全 peer が反映する (または peer が確実に not-ready になる) まで待つ。
// peer ごとの完了条件:
//   - 同一 epoch で ack 済み (acked >= target、down でない)
//   - epoch が変わった場合は新 epoch での invalidate 成功確認
//   - いずれでもなければ lastGrantAt + memLease + memClockSkew を過ぎるまで待つ (peer は lease 切れで自発的に DB 経路)
func (ps *memPeers) waitAcked() {
	if memSyncVisible {
		defer memSyncStats.since(time.Now())
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	target, epoch := ps.seq, ps.epoch
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	skipped := false
	defer func() {
		if skipped {
			memRecordLeaseSkip()
		}
	}()
	for {
		now := time.Now()
		var wakeAt time.Time
		ok := true
		for _, peer := range ps.peers {
			if memWatermark && peer.wmMode && !memSyncVisible {
				continue // peer の読みが watermark を待つ
			}
			if ps.epoch == epoch && peer.acked >= target {
				continue
			}
			if ps.epoch != epoch && peer.invalidatedEpoch == ps.epoch {
				continue
			}
			leaseEnd := peer.lastGrantAt.Add(memLease + memClockSkew)
			if !now.Before(leaseEnd) {
				skipped = true
				continue
			}
			ok = false
			if wakeAt.IsZero() || leaseEnd.Before(wakeAt) {
				wakeAt = leaseEnd
			}
		}
		if ok {
			return
		}
		if timer != nil {
			timer.Stop()
		}
		timer = time.AfterFunc(time.Until(wakeAt), func() {
			ps.mu.Lock()
			ps.cond.Broadcast()
			ps.mu.Unlock()
		})
		ps.cond.Wait()
	}
}

// newEpoch: pump がリロードした。peer に無効化を通知 (peer はダンプから作り直す)。cycleMu 保持中に呼ばれる。
func (ps *memPeers) newEpoch() {
	ps.mu.Lock()
	ps.epoch = memNewEpoch()
	ps.seq = 0
	ps.wm = 0
	ps.buf = nil
	for _, peer := range ps.peers {
		peer.pending = nil
		peer.acked = 0
	}
	ps.cond.Broadcast()
	ps.mu.Unlock()
	ps.invalidateAll(context.Background(), false)
}

func (ps *memPeers) markInvalidated(addr string) {
	ps.mu.Lock()
	for _, peer := range ps.peers {
		if peer.addr == addr {
			peer.invalidatedEpoch = ps.epoch
		}
	}
	ps.cond.Broadcast()
	ps.mu.Unlock()
}

func (ps *memPeers) invalidate() { ps.invalidateAll(context.Background(), false) }

// invalidateAll は全 peer を not-ready にする。wait=true なら peer の再構築完了まで待つ (initialize 用)。
func (ps *memPeers) invalidateAll(ctx context.Context, wait bool) {
	timeout := 2 * time.Second
	if wait {
		timeout = 45 * time.Second
	}
	var wg sync.WaitGroup
	for _, peer := range ps.peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if _, err := memHTTPPost(addr, "/internal/mem/invalidate?wait="+strconv.FormatBool(wait), struct{}{}, timeout); err != nil {
				slog.Warn("mem: invalidate peer failed", "peer", addr, "error", err)
				// the peer may still be serving the old state: wait until its lease
				// (bounded by our last send + memLease) has surely run out
				time.Sleep(memLease + 300*time.Millisecond)
				return
			}
			ps.markInvalidated(addr)
		}(peer.addr)
	}
	wg.Wait()
}

// initialize does not wait for the peers to rebuild: a not-ready peer answers
// from the DB, and a full dump (15-17s each) would push initialize past 60s.
func (ps *memPeers) resetAll(ctx context.Context) { ps.invalidateAll(ctx, false) }

func memHTTPPost(addr, path string, body any, timeout time.Duration) ([]byte, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	wire, gz := memMaybeGzip(path, data)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if memPushStatsOn && path == "/internal/mem/push" {
		if pr, ok := body.(memPushRequest); ok {
			defer func() {
				memPushStats.recordSend(len(data), len(wire), &pr, time.Duration(time.Now().UnixNano()-pr.SentAt))
			}()
		}
	}
	req.Header.Set(forwardedSecretHeader, forwardSecret)
	resp, err := memHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mem: %s%s status %d", addr, path, resp.StatusCode)
	}
	return out, nil
}

func memGet(addr, path string, timeout time.Duration) (*http.Response, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	req.Header.Set(forwardedSecretHeader, forwardSecret)
	resp, err := memHTTPClient.Do(req)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, nil, fmt.Errorf("mem: %s%s status %d", addr, path, resp.StatusCode)
	}
	return resp, cancel, nil
}

type memDump struct {
	Epoch       string                `json:"epoch"`
	Seq         int64                 `json:"seq"`
	Windows     []memWindowRow        `json:"windows"`
	Users       []memUserRow          `json:"users"`
	Sessions    []memSessionRow       `json:"sessions"`
	Friendships []memFriendshipRow    `json:"friendships"`
	Follows     []memFollowRow        `json:"follows"`
	Blocks      []memBlockRow         `json:"blocks"`
	Requests    []memFriendRequestRow `json:"requests"`
	Posts       []memPostRow          `json:"posts"`
	Comments    []memCommentRow       `json:"comments"`
	RealMojis   []memRealMojiRow      `json:"realmojis"`
	WM          int64                 `json:"wm,omitempty"`      // Seq に対して有効な watermark
	WMMode      bool                  `json:"wmMode,omitempty"`  // pump が ack を待たずに返す (peer は watermark を待つ)
	SyncVisible bool                  `json:"sv,omitempty"`      // ISUREAL_MEM_SYNC_VISIBLE: pump は wm モードの peer の ack も待つ
	HotFrom     string                `json:"hotFrom,omitempty"` // memData.hotFrom (DateOnly); 無ければ不明
}

func (d *memData) dump() *memDump {
	out := &memDump{}
	if d.hotKnown {
		out.HotFrom = d.hotFrom.Format(time.DateOnly)
	}
	for _, w := range d.windows {
		out.Windows = append(out.Windows, memWindowRow{ID: w.ID, Region: w.Region, LocalDate: w.LocalDate.Format(time.DateOnly), StartsAt: w.StartsAt, OnTimeDeadline: w.OnTimeDeadline, EndedAt: w.EndedAt})
	}
	for _, u := range d.users {
		out.Users = append(out.Users, memUserRow{ID: u.ID, Username: u.Username, SecretKeyHash: u.SecretKeyHash, DisplayName: u.DisplayName, Bio: u.Bio, ProfileImageID: u.ProfileImageID, Region: u.Region, AccountType: u.AccountType, Status: u.Status})
		for other, e := range u.friends {
			if u.ID < other {
				out.Friendships = append(out.Friendships, memFriendshipRow{ID: e.ID, UserLowID: u.ID, UserHighID: other, UpdatedAt: e.UpdatedAt})
			}
		}
		for other, e := range u.following {
			out.Follows = append(out.Follows, memFollowRow{ID: e.ID, FollowerID: u.ID, FolloweeID: other, UpdatedAt: e.UpdatedAt})
		}
		for other := range u.blocking {
			out.Blocks = append(out.Blocks, memBlockRow{BlockerID: u.ID, BlockedID: other})
		}
		for _, r := range u.outgoing {
			out.Requests = append(out.Requests, memFriendRequestRow{ID: r.ID, RequesterID: r.RequesterID, RecipientID: r.RecipientID, Status: "pending", UpdatedAt: r.UpdatedAt})
		}
	}
	for hash, s := range d.sessions {
		out.Sessions = append(out.Sessions, memSessionRow{TokenHash: hash, UserID: s.UserID, Active: true, CreatedAt: s.CreatedAt})
	}
	for _, p := range d.posts {
		r := memPostRow{ID: p.ID, AuthorID: p.AuthorID, PostingWindowID: p.PostingWindowID, ImageID: p.ImageID, Caption: p.Caption, Visibility: p.Visibility,
			LocationKind: p.Kind, Latitude: p.Latitude, Longitude: p.Longitude, Locality: p.Locality, PostedAt: p.PostedAt}
		if p.deleted {
			t := p.PostedAt
			r.DeletedAt = &t
		}
		out.Posts = append(out.Posts, r)
		for _, c := range p.comments {
			out.Comments = append(out.Comments, memCommentRow{ID: c.ID, PostID: c.PostID, AuthorID: c.author.ID, Body: c.Body, CreatedAt: c.CreatedAt})
		}
		for _, x := range p.realmojis {
			out.RealMojis = append(out.RealMojis, memRealMojiRow{ID: x.ID, PostID: x.PostID, ReactorID: x.reactor.ID, ImageID: x.ImageID, ReactionType: x.ReactionType, CreatedAt: x.CreatedAt})
		}
	}
	return out
}

func memDataFromDump(dump *memDump) (*memData, error) {
	d := newMemData()
	for _, r := range dump.Windows {
		if err := d.applyWindow(r); err != nil {
			return nil, err
		}
	}
	for _, r := range dump.Users {
		d.applyUser(r)
	}
	for _, r := range dump.Sessions {
		d.applySession(false, r)
	}
	for _, r := range dump.Friendships {
		d.applyFriendship(false, r)
	}
	for _, r := range dump.Follows {
		d.applyFollow(false, r)
	}
	for _, r := range dump.Blocks {
		d.applyBlock(false, r)
	}
	for _, r := range dump.Requests {
		d.applyFriendRequest(false, r)
	}
	for _, r := range dump.Posts {
		d.applyPost(r)
	}
	for _, r := range dump.Comments {
		d.applyComment(r)
	}
	for _, r := range dump.RealMojis {
		d.applyRealMoji(r)
	}
	if dump.HotFrom != "" {
		if t, err := time.Parse(time.DateOnly, dump.HotFrom); err == nil {
			d.hotFrom, d.hotKnown = t, true
		}
	}
	d.sortedUsers()
	return d, nil
}

func registerMemPumpRoutes(e *echo.Echo, p *memPump) {
	e.GET("/internal/mem/dump", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		if p.peers == nil {
			return c.NoContent(http.StatusNotFound)
		}
		p.cycleMu.Lock()
		if !p.m.ready.Load() {
			p.cycleMu.Unlock()
			return c.NoContent(http.StatusServiceUnavailable)
		}
		p.m.mu.RLock()
		dump := p.m.d.dump()
		p.m.mu.RUnlock()
		p.peers.mu.Lock()
		dump.Epoch, dump.Seq = p.peers.epoch, p.peers.seq
		dump.WM, dump.WMMode = p.peers.wm, memWatermark
		dump.SyncVisible = memSyncVisible
		p.peers.mu.Unlock()
		p.peers.markWMMode(c.Request().RemoteAddr, c.QueryParam("wm") == "1")
		p.cycleMu.Unlock()
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		if err := json.NewEncoder(zw).Encode(dump); err != nil {
			return err
		}
		zw.Close()
		slog.Info("mem: dump served", "bytes", buf.Len(), "users", len(dump.Users), "posts", len(dump.Posts))
		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})
	e.GET("/internal/mem/batches", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		if p.peers == nil {
			return c.NoContent(http.StatusNotFound)
		}
		from, _ := strconv.ParseInt(c.QueryParam("from"), 10, 64)
		to, _ := strconv.ParseInt(c.QueryParam("to"), 10, 64)
		ps := p.peers
		ps.mu.Lock()
		if c.QueryParam("e") != ps.epoch || len(ps.buf) == 0 || ps.buf[0].Seq > from || to > ps.seq {
			ps.mu.Unlock()
			return c.NoContent(http.StatusGone)
		}
		first := ps.buf[0].Seq
		out := append([]memBatch(nil), ps.buf[from-first:to-first+1]...)
		ps.mu.Unlock()
		return c.JSON(http.StatusOK, out)
	})
}

// ---------------- peer side ----------------

type memReceiver struct {
	m        *memStore
	pumpAddr string

	mu     sync.Mutex // apply 直列化
	epoch  string
	seq    int64
	gen    uint64 // 無効化ごとに増える
	rsMu   sync.Mutex
	rsCond *sync.Cond
	rsGen  uint64 // 再構築完了済みの gen
	rsRun  bool
	// r.seq が進んだら Broadcast (並列 push の到着順の入れ替わりを短時間待つ)
	seqCond *sync.Cond
	// まだバッチが届いていない seq の watermark (mu 下)
	wmPending []memWMMsg
	// MEM_FULL の冷たい読みの待ち (mem_full.go): 適用済みの pump の seq (-1 = 未同期) と store の seq
	appliedPump, appliedStore atomic.Int64
	coldNotify                memNotify
}

func newMemReceiver(m *memStore) *memReceiver {
	r := &memReceiver{m: m, pumpAddr: envOrDefault("ISUREAL_MEM_PUMP", "192.168.1.10:8080")}
	r.rsCond = sync.NewCond(&r.rsMu)
	r.seqCond = sync.NewCond(&r.mu)
	r.invalidate()
	return r
}

// invalidate: not-ready にして再構築を開始する。返り値は待つべき gen。
func (r *memReceiver) invalidate() uint64 {
	r.mu.Lock()
	r.m.ready.Store(false)
	r.epoch = ""
	r.wmPending = nil
	r.m.w.set(0)
	r.appliedPump.Store(-1)
	r.appliedStore.Store(0)
	r.gen++
	g := r.gen
	r.mu.Unlock()
	r.rsMu.Lock()
	if !r.rsRun {
		r.rsRun = true
		go r.resyncLoop()
	}
	r.rsMu.Unlock()
	return g
}

func (r *memReceiver) resyncLoop() {
	for {
		r.mu.Lock()
		g := r.gen
		r.mu.Unlock()
		err := r.resyncOnce(g)
		r.rsMu.Lock()
		if err == nil {
			if r.rsGen < g {
				r.rsGen = g
			}
			r.rsCond.Broadcast()
			r.mu.Lock()
			latest := r.gen
			r.mu.Unlock()
			if latest == g {
				r.rsRun = false
				r.rsMu.Unlock()
				return
			}
			r.rsMu.Unlock()
			continue
		}
		r.rsMu.Unlock()
		slog.Warn("mem: resync failed; retrying", "error", err)
		time.Sleep(500 * time.Millisecond)
	}
}

func (r *memReceiver) resyncOnce(g uint64) error {
	start := time.Now()
	if memFull && !r.m.ready.Load() {
		// 全期間の memData (~0.9GB) を 2 つ同時に持たない: 作り直す前に古いものを捨てる (not-ready の間は誰も読まない)
		r.m.mu.Lock()
		r.m.d = newMemData()
		r.m.mu.Unlock()
	}
	resp, cancel, err := memGet(r.pumpAddr, "/internal/mem/dump?wm="+map[bool]string{true: "1", false: "0"}[memWatermark || memFull], 40*time.Second)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()
	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	var dump memDump
	if err := json.NewDecoder(zr).Decode(&dump); err != nil {
		return err
	}
	d, err := memDataFromDump(&dump)
	if err != nil {
		return err
	}
	if memFull {
		if err := memLoadCold(d); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen != g {
		return nil // 途中で無効化された → ループが再試行する
	}
	r.m.mu.Lock()
	r.m.d = d
	r.m.mu.Unlock()
	r.epoch, r.seq = dump.Epoch, dump.Seq
	r.setApplied()
	r.wmPending = nil
	r.m.w.pumpWM.Store(memWatermark && dump.WMMode)
	r.m.w.pumpSync.Store(memSyncVisible && dump.SyncVisible)
	r.m.w.knownSeq = 0                 // epoch が変わると seq の系列が変わる
	r.m.w.syncSeen(dump.Seq, dump.Seq) // dump の Seq まで反映済み
	r.m.w.set(dump.WM)
	// lease は次の push/heartbeat (probe 以外) でだけ延長する (pump が lastGrantAt で上界を把握するため)
	r.m.ready.Store(true)
	go memPrebuildLoaded(r.m)
	slog.Info("mem: synced from pump", "seq", dump.Seq, "users", len(d.users), "posts", len(d.posts), "elapsed", time.Since(start))
	return nil
}

func (r *memReceiver) waitResynced(g uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		r.rsMu.Lock()
		r.rsCond.Broadcast()
		r.rsMu.Unlock()
	})
	defer timer.Stop()
	r.rsMu.Lock()
	for r.rsGen < g && time.Now().Before(deadline) {
		r.rsCond.Wait()
	}
	r.rsMu.Unlock()
}

// pushTimed: push に段階ごとの計測を付ける (ISUREAL_MEM_PUSH_STATS)。
func (r *memReceiver) pushTimed(req memPushRequest, recv time.Time) bool {
	if !memPushStatsOn {
		return r.push(req)
	}
	var st memPushTiming
	ok := r.pushT(req, &st)
	memPushStats.recordRecv(req.SentAt, recv, st.locked, time.Now(), st.pulls)
	return ok
}

type memPushTiming struct {
	locked time.Time // r.mu 取得 + 並び替え待ちの完了
	pulls  int
}

func (r *memReceiver) push(req memPushRequest) bool { return r.pushT(req, nil) }

func (r *memReceiver) pushT(req memPushRequest, st *memPushTiming) bool {
	r.mu.Lock()
	if !r.m.ready.Load() || req.Epoch != r.epoch {
		r.mu.Unlock()
		if r.m.ready.Load() {
			r.invalidate()
		}
		return false
	}
	// 並列 push は到着順が入れ替わりうる: 先行分が届くのを少しだけ待ってから欠番 pull に回る
	need := req.Seq
	if len(req.Batches) > 0 {
		need = req.Batches[0].Seq - 1
	}
	if need > r.seq {
		deadline := time.Now().Add(memReorderWait)
		timer := time.AfterFunc(memReorderWait, func() {
			r.mu.Lock()
			r.seqCond.Broadcast()
			r.mu.Unlock()
		})
		for need > r.seq && req.Epoch == r.epoch && r.m.ready.Load() && time.Now().Before(deadline) {
			r.seqCond.Wait()
		}
		timer.Stop()
		if !r.m.ready.Load() || req.Epoch != r.epoch {
			r.mu.Unlock()
			return false
		}
	}
	defer r.mu.Unlock()
	defer r.seqCond.Broadcast()
	if st != nil {
		st.locked = time.Now()
	}
	for _, b := range req.Batches {
		if b.Seq <= r.seq {
			continue
		}
		if b.Seq > r.seq+1 {
			if st != nil {
				st.pulls++
			}
			if err := r.fillGap(req.Epoch, b.Seq-1); err != nil {
				if errors.Is(err, errMemGapStale) {
					return false // 無効化 / epoch 切り替えが割り込んだ: 後始末は割り込んだ側が済ませている
				}
				slog.Warn("mem: gap recovery failed", "error", err)
				r.m.ready.Store(false)
				go r.invalidate()
				return false
			}
			if b.Seq <= r.seq {
				continue // ロックを手放している間に、先行の push がこのバッチまで適用した
			}
		}
		if err := r.m.applyBatchChunked(b.Changes, memApplyChunk); err != nil {
			slog.Error("mem: apply failed", "error", err)
			r.m.ready.Store(false)
			go r.invalidate()
			return false
		}
		r.seq = b.Seq
		r.setApplied()
	}
	if req.Seq > r.seq {
		if st != nil {
			st.pulls++
		}
		if err := r.fillGap(req.Epoch, req.Seq); err != nil {
			if errors.Is(err, errMemGapStale) {
				return false
			}
			slog.Warn("mem: gap recovery failed", "error", err)
			r.m.ready.Store(false)
			go r.invalidate()
			return false
		}
	}
	r.m.w.syncSeen(req.Seq, r.seq)
	if req.SS > 0 && r.seq >= req.Seq && req.SS > r.appliedStore.Load() {
		r.appliedStore.Store(req.SS)
		r.coldNotify.wake()
	}
	if !req.Probe {
		r.m.extendLeaseFrom(req.SentAt)
		// 片道遅延 (時計ずれ込み) が大きいと lease が短くなる / RYW の前提が崩れるので警告 (10 秒に 1 回)
		if d := time.Duration(time.Now().UnixNano() - req.SentAt); d > 500*time.Millisecond || d < -memClockSkew {
			if now := time.Now().Unix(); memSkewWarnAt.Load()+10 <= now {
				memSkewWarnAt.Store(now)
				slog.Warn("mem: large push delay or clock skew", "delay", d)
			}
		}
	}
	return true
}

var memSkewWarnAt atomic.Int64

// ISUREAL_MEM_PULL_UNLOCKED=1 (既定 0): 欠番の pull (asia1 への 1 往復) の間、r.mu を手放す。
// 従来は pull の間 r.mu を持ち続けるので、その間に届いた push (多くは欠番そのもの) と wm 通知が
// 1 RTT 以上止まり、wm の push の尾 (500ms 超) になっていた。
//
// 正しさ:
//   - 適用は従来どおり r.mu の下で、連番どおりに 1 つずつ (mb.Seq == r.seq+1 のものだけ) 行う。
//     ロックを手放している間に他の push が先に進めた分 (mb.Seq <= r.seq) は捨てる。同じ epoch の同じ seq の
//     バッチは pump のバッファ上で同一なので、どちらの経路で適用しても中身は同じ。
//   - 再取得の後に gen / epoch / ready を確認し、無効化や resync が割り込んでいたら何も適用しない
//     (errMemGapStale。メモリは割り込んだ側が作り直す)。
//   - 目標 to に届かなければ従来どおり失敗 (呼び出し側が invalidate)。
//   - wm は r.seq を見て進める (wmAfterPush / wmMsg) ので、適用前の seq に対して wm が進むことはない。
//     ロックを手放している間に wmMsg が来ても、r.seq が足りなければ保留される。
var memPullUnlocked = os.Getenv("ISUREAL_MEM_PULL_UNLOCKED") == "1"

var errMemGapStale = errors.New("mem: receiver invalidated during gap pull")

// fillGap は r.mu 保持中に呼ぶ。r.seq >= to になるまで pump から取り寄せて適用する (戻るときも r.mu 保持)。
func (r *memReceiver) fillGap(epoch string, to int64) error {
	if r.seq >= to {
		return nil
	}
	from := r.seq + 1
	var missing []memBatch
	var err error
	if memPullUnlocked {
		gen := r.gen
		r.mu.Unlock()
		missing, err = r.pull(epoch, from, to)
		r.mu.Lock()
		if r.gen != gen || r.epoch != epoch || !r.m.ready.Load() {
			return errMemGapStale
		}
	} else {
		missing, err = r.pull(epoch, from, to)
	}
	if err != nil {
		return err
	}
	for _, mb := range missing {
		if mb.Seq <= r.seq {
			continue
		}
		if mb.Seq != r.seq+1 {
			return fmt.Errorf("mem: pulled batch %d does not follow %d", mb.Seq, r.seq)
		}
		if err := r.m.applyBatchChunked(mb.Changes, memApplyChunk); err != nil {
			return err
		}
		r.seq = mb.Seq
		r.setApplied()
		r.seqCond.Broadcast()
	}
	if r.seq < to {
		return fmt.Errorf("mem: gap pull reached %d, want %d", r.seq, to)
	}
	return nil
}

func (r *memReceiver) pull(epoch string, from, to int64) ([]memBatch, error) {
	resp, cancel, err := memGet(r.pumpAddr, fmt.Sprintf("/internal/mem/batches?e=%s&from=%d&to=%d", epoch, from, to), 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	var out []memBatch
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}

func registerMemPeerRoutes(e *echo.Echo, r *memReceiver) {
	e.POST("/internal/mem/push", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		recv := time.Now()
		var req memPushRequest
		body, closeBody, err := memRequestBody(c.Request())
		if err == nil {
			err = json.NewDecoder(body).Decode(&req)
			closeBody()
		}
		if err != nil {
			r.m.ready.Store(false)
			go r.invalidate()
			return c.NoContent(http.StatusBadRequest)
		}
		storePGPeer.update(req.PT, req.PS, req.PA)
		ok := r.pushTimed(req, recv)
		if ok {
			r.wmAfterPush(req)
		}
		return c.JSON(http.StatusOK, memPushResponse{OK: ok})
	})
	e.POST("/internal/mem/wm", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		var msg memWMMsg
		if err := json.NewDecoder(c.Request().Body).Decode(&msg); err != nil {
			return c.NoContent(http.StatusBadRequest)
		}
		storePGPeer.update(msg.PT, msg.PS, msg.PA)
		r.wmMsg(msg)
		return c.NoContent(http.StatusOK)
	})
	e.POST("/internal/mem/invalidate", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		g := r.invalidate()
		if c.QueryParam("wait") == "true" {
			r.waitResynced(g, 44*time.Second)
		}
		return c.JSON(http.StatusOK, memPushResponse{OK: r.m.ready.Load()})
	})
}
