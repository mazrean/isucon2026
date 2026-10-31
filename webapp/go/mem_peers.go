package isureal

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
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
}

type memPushRequest struct {
	Epoch   string     `json:"e"`
	Seq     int64      `json:"q"` // pump の最新連番 (heartbeat でも送る。peer はこれで欠番を検知)
	Batches []memBatch `json:"b"`
	// watermark (ISUREAL_MEM_WATERMARK): Seq までのバッチに、この時刻より前に応答が返ったコミットがすべて含まれる。
	WM int64 `json:"w,omitempty"`
}

// peer は最後に push/heartbeat を受け取ってから memLease を過ぎたら自発的にメモリ読みをやめる (DB 経路)。
// pump の push タイムアウト (memPushTimeout) > memLease なので、pump が ack 無しで先に進む時点で
// その peer は必ず not-ready になっている。
const (
	memLease          = 1 * time.Second
	memPushTimeout    = 3 * time.Second
	memHeartbeatEvery = 250 * time.Millisecond
)

type memPushResponse struct {
	OK bool `json:"ok"`
}

var memHTTPClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 16,
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
	addr    string
	pending []memBatch
	sending bool
	acked   int64
	down    bool
	beat    bool
	// 最後に ok ack を受けた時刻。peer の lease はこれ + memLease より前に必ず切れる。
	lastAckAt time.Time
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
			ps.peers = append(ps.peers, &memPeer{addr: a, lastAckAt: time.Now()})
		}
	}
	return ps
}

// publish は cycleMu 保持中に呼ばれる (連番順)。
func (ps *memPeers) publish(changes []memChange) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.seq++
	b := memBatch{Seq: ps.seq, Changes: changes}
	ps.buf = append(ps.buf, b)
	if len(ps.buf) > memBufferBatches {
		ps.buf = append([]memBatch(nil), ps.buf[len(ps.buf)-memBufferBatches/2:]...)
	}
	for _, peer := range ps.peers {
		peer.pending = append(peer.pending, b)
		if !peer.sending {
			peer.sending = true
			go ps.sender(peer, ps.epoch)
		}
	}
}

func (ps *memPeers) sender(peer *memPeer, epoch string) {
	for {
		ps.mu.Lock()
		if ps.epoch != epoch && len(peer.pending) > 0 {
			epoch = ps.epoch // 新しい epoch の送信を引き継ぐ
		}
		if (len(peer.pending) == 0 && !peer.beat) || ps.epoch != epoch {
			peer.sending = false
			ps.mu.Unlock()
			return
		}
		peer.beat = false
		batches := peer.pending
		peer.pending = nil
		last := ps.seq
		if len(batches) > 0 {
			last = batches[len(batches)-1].Seq
		}
		var wm int64
		if last == ps.seq && epoch == ps.epoch {
			wm = ps.wm // ps.wm は ps.seq に対して有効
		}
		ps.mu.Unlock()

		body, err := memHTTPPost(peer.addr, "/internal/mem/push", memPushRequest{Epoch: epoch, Seq: last, Batches: batches, WM: wm}, memPushTimeout)
		var resp memPushResponse
		if err == nil {
			err = json.Unmarshal(body, &resp)
		}

		ps.mu.Lock()
		if ps.epoch == epoch {
			if err != nil {
				// 失敗した peer は待たないが、その peer の lease (最後の ok ack + memLease) が切れるまでは
				// 書き込みレスポンスを保留する (waitAcked)。欠番は peer が次の push で検知して取り直す。
				if !peer.down {
					slog.Warn("mem: peer push failed; marking down", "peer", peer.addr, "error", err)
				}
				peer.down = true
			} else {
				peer.down = false
				if resp.OK {
					// peer は応答前に lease を延長している → lastAckAt は延長時刻の上界
					peer.lastAckAt = time.Now()
				}
				// ok=false: peer は not-ready (DB 経路) なので待つ必要なし
				if peer.acked < last {
					peer.acked = last
				}
			}
		}
		ps.cond.Broadcast()
		ps.mu.Unlock()
	}
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
			if !peer.sending {
				peer.beat = true
				peer.sending = true
				go ps.sender(peer, ps.epoch)
			}
		}
		ps.mu.Unlock()
	}
}

// waitAcked は現在の連番までを全 peer が反映する (または peer が確実に not-ready になる) まで待つ。
// peer ごとの完了条件:
//   - 同一 epoch で ack 済み (acked >= target、down でない)
//   - epoch が変わった場合は新 epoch での invalidate 成功確認
//   - いずれでもなければ lastAckAt + memLease を過ぎるまで待つ (peer は lease 切れで自発的に DB 経路)
func (ps *memPeers) waitAcked() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	target, epoch := ps.seq, ps.epoch
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		now := time.Now()
		var wakeAt time.Time
		ok := true
		for _, peer := range ps.peers {
			if memWatermark && peer.wmMode {
				continue // peer の読みが watermark を待つ
			}
			if ps.epoch == epoch && !peer.down && peer.acked >= target {
				continue
			}
			if ps.epoch != epoch && peer.invalidatedEpoch == ps.epoch {
				continue
			}
			leaseEnd := peer.lastAckAt.Add(memLease + 50*time.Millisecond)
			if !now.Before(leaseEnd) {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
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
	WM          int64                 `json:"wm,omitempty"`     // Seq に対して有効な watermark
	WMMode      bool                  `json:"wmMode,omitempty"` // pump が ack を待たずに返す (peer は watermark を待つ)
}

func (d *memData) dump() *memDump {
	out := &memDump{}
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
	// まだバッチが届いていない seq の watermark (mu 下)
	wmPending []memWMMsg
}

func newMemReceiver(m *memStore) *memReceiver {
	r := &memReceiver{m: m, pumpAddr: envOrDefault("ISUREAL_MEM_PUMP", "192.168.1.10:8080")}
	r.rsCond = sync.NewCond(&r.rsMu)
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
	resp, cancel, err := memGet(r.pumpAddr, "/internal/mem/dump?wm="+map[bool]string{true: "1", false: "0"}[memWatermark], 40*time.Second)
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen != g {
		return nil // 途中で無効化された → ループが再試行する
	}
	r.m.mu.Lock()
	r.m.d = d
	r.m.mu.Unlock()
	r.epoch, r.seq = dump.Epoch, dump.Seq
	r.wmPending = nil
	r.m.w.pumpWM.Store(memWatermark && dump.WMMode)
	r.m.w.set(dump.WM)
	// lease は次の push/heartbeat の ok 応答時にだけ延長する (pump が lastAckAt で上界を把握するため)
	r.m.ready.Store(true)
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

func (r *memReceiver) push(req memPushRequest) bool {
	r.mu.Lock()
	if !r.m.ready.Load() || req.Epoch != r.epoch {
		r.mu.Unlock()
		if r.m.ready.Load() {
			r.invalidate()
		}
		return false
	}
	defer r.mu.Unlock()
	for _, b := range req.Batches {
		if b.Seq <= r.seq {
			continue
		}
		if b.Seq > r.seq+1 {
			missing, err := r.pull(req.Epoch, r.seq+1, b.Seq-1)
			if err == nil {
				for _, mb := range missing {
					if err = r.m.applyBatch(mb.Changes); err != nil {
						break
					}
					r.seq = mb.Seq
				}
			}
			if err != nil || r.seq != b.Seq-1 {
				slog.Warn("mem: gap recovery failed", "error", err)
				r.m.ready.Store(false)
				go r.invalidate()
				return false
			}
		}
		if err := r.m.applyBatch(b.Changes); err != nil {
			slog.Error("mem: apply failed", "error", err)
			r.m.ready.Store(false)
			go r.invalidate()
			return false
		}
		r.seq = b.Seq
	}
	if req.Seq > r.seq {
		missing, err := r.pull(req.Epoch, r.seq+1, req.Seq)
		if err == nil {
			for _, mb := range missing {
				if err = r.m.applyBatch(mb.Changes); err != nil {
					break
				}
				r.seq = mb.Seq
			}
		}
		if err != nil || r.seq != req.Seq {
			slog.Warn("mem: gap recovery failed", "error", err)
			r.m.ready.Store(false)
			go r.invalidate()
			return false
		}
	}
	r.m.extendLease()
	return true
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
		var req memPushRequest
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			r.m.ready.Store(false)
			go r.invalidate()
			return c.NoContent(http.StatusBadRequest)
		}
		ok := r.push(req)
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
