package isureal

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/gob"
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
}

type memPushRequest struct {
	Epoch   string     `json:"e"`
	Seq     int64      `json:"q"` // pump の最新連番 (heartbeat でも送る。peer はこれで欠番を検知)
	Batches []memBatch `json:"b"`
	// pump の送信時刻 (unix ns)。peer の lease は min(受信時刻, SentAt) + memLease まで (遅れて届いた push で延びない)。
	SentAt int64 `json:"t"`
	// probe: down 扱いの peer への送信。peer は反映するが lease は延ばさない (pump が lease の上界を把握するため)。
	Probe bool `json:"p,omitempty"`
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
	memReorderWait    = 50 * time.Millisecond
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
	addr     string
	pending  []memBatch
	inflight int
	acked    int64
	down     bool
	// lease を与えうる (probe でない) push の最終送信時刻。peer の lease はこれ + memLease (+時計ずれ) より前に必ず切れる。
	lastGrantAt time.Time
	// この epoch で invalidate (not-ready 化) の成功を確認済み
	invalidatedEpoch string
}

type memPeers struct {
	// soft reset (initialize) 直前の epoch と最終連番。peer がこれに一致すれば初期データ行を引き継げる。
	prevEpoch string
	prevSeq   int64
	p         *memPump
	mu        sync.Mutex
	cond      *sync.Cond
	epoch     string
	seq       int64
	buf       []memBatch
	peers     []*memPeer
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
		ps.kick(peer, false)
	}
}

// kick は ps.mu 保持中に呼ぶ。未送信分 (または heartbeat) を新しい並列 push として送る。
func (ps *memPeers) kick(peer *memPeer, beat bool) {
	if peer.inflight >= memMaxInflight || (len(peer.pending) == 0 && !beat) {
		return
	}
	batches := peer.pending
	peer.pending = nil
	last := ps.seq
	if len(batches) > 0 {
		last = batches[len(batches)-1].Seq
	}
	now := time.Now()
	req := memPushRequest{Epoch: ps.epoch, Seq: last, Batches: batches, SentAt: now.UnixNano(), Probe: peer.down}
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
			if ps.epoch == epoch && peer.acked >= target {
				continue
			}
			if ps.epoch != epoch && peer.invalidatedEpoch == ps.epoch {
				continue
			}
			leaseEnd := peer.lastGrantAt.Add(memLease + memClockSkew)
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
func (ps *memPeers) newEpoch() { ps.newEpochKeep(false) }

// newEpochKeep(true): 直前の epoch で同期済みの peer は初期データ行を引き継げる (initialize の soft reset)。
func (ps *memPeers) newEpochKeep(keep bool) {
	ps.mu.Lock()
	if keep {
		ps.prevEpoch, ps.prevSeq = ps.epoch, ps.seq
	} else {
		ps.prevEpoch, ps.prevSeq = "", 0
	}
	ps.epoch = memNewEpoch()
	ps.seq = 0
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
}

func (d *memData) dump(includeCold bool) *memDump {
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
		if includeCold || !p.cold {
			r := memPostRow{ID: p.ID, AuthorID: p.AuthorID, PostingWindowID: p.PostingWindowID, ImageID: p.ImageID, Caption: p.Caption, Visibility: p.Visibility,
				LocationKind: p.Kind, Latitude: p.Latitude, Longitude: p.Longitude, Locality: p.Locality, PostedAt: p.PostedAt, Cold: p.cold}
			if p.deleted {
				t := p.PostedAt
				r.DeletedAt = &t
			}
			out.Posts = append(out.Posts, r)
		}
		for _, c := range p.comments {
			if includeCold || !c.cold {
				out.Comments = append(out.Comments, memCommentRow{ID: c.ID, PostID: c.PostID, AuthorID: c.author.ID, Body: c.Body, CreatedAt: c.CreatedAt, Cold: c.cold})
			}
		}
		for _, x := range p.realmojis {
			if includeCold || !x.cold {
				out.RealMojis = append(out.RealMojis, memRealMojiRow{ID: x.ID, PostID: x.PostID, ReactorID: x.reactor.ID, ImageID: x.ImageID, ReactionType: x.ReactionType, CreatedAt: x.CreatedAt, Cold: x.cold})
			}
		}
	}
	return out
}

// memDataFromDump: old != nil なら old の初期データ行を引き継ぐ (呼び出し側は m.mu 書きロック中)。
func memDataFromDump(dump *memDump, old *memData) (*memData, error) {
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
	if old != nil {
		d.adoptCold(old)
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
		ce := c.QueryParam("ce")
		cs, _ := strconv.ParseInt(c.QueryParam("cs"), 10, 64)
		p.cycleMu.Lock()
		if !p.m.ready.Load() {
			p.cycleMu.Unlock()
			return c.NoContent(http.StatusServiceUnavailable)
		}
		p.peers.mu.Lock()
		// peer が直前の epoch の最終連番まで同期していたなら、初期データ行は peer 手元のものを使わせる
		keep := ce != "" && ce == p.peers.prevEpoch && cs == p.peers.prevSeq
		epoch, seq := p.peers.epoch, p.peers.seq
		p.peers.mu.Unlock()
		p.m.mu.RLock()
		dump := p.m.d.dump(!keep)
		p.m.mu.RUnlock()
		dump.Epoch, dump.Seq = epoch, seq
		p.cycleMu.Unlock()
		start := time.Now()
		c.Response().Header().Set("X-Mem-Cold", map[bool]string{true: "keep", false: "full"}[keep])
		c.Response().Header().Set("Content-Type", "application/octet-stream")
		c.Response().WriteHeader(http.StatusOK)
		zw, _ := gzip.NewWriterLevel(c.Response(), gzip.BestSpeed)
		if err := gob.NewEncoder(zw).Encode(dump); err != nil {
			return nil
		}
		zw.Close()
		slog.Info("mem: dump served", "keep", keep, "users", len(dump.Users), "posts", len(dump.Posts), "elapsed", time.Since(start))
		return nil
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
	farm     *sigFarm

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
	// 直前に同期していた (epoch, seq)。pump が initialize 前の最終状態と一致すると判断すれば初期データ行を引き継ぐ。
	coldEpoch string
	coldSeq   int64
}

func newMemReceiver(m *memStore, farm *sigFarm) *memReceiver {
	r := &memReceiver{m: m, farm: farm, pumpAddr: envOrDefault("ISUREAL_MEM_PUMP", "192.168.1.10:8080")}
	r.rsCond = sync.NewCond(&r.rsMu)
	r.seqCond = sync.NewCond(&r.mu)
	r.invalidate()
	return r
}

// invalidate: not-ready にして再構築を開始する (手元の初期データ行は引き継がない)。返り値は待つべき gen。
func (r *memReceiver) invalidate() uint64 { return r.invalidateKeep(false) }

// invalidateKeep(true): 同期済みなら現在の (epoch, seq) を初期データ行の引き継ぎ候補として記録する。
func (r *memReceiver) invalidateKeep(keepCold bool) uint64 {
	r.mu.Lock()
	if !keepCold {
		r.coldEpoch = ""
	} else if r.m.ready.Load() && r.epoch != "" {
		r.coldEpoch, r.coldSeq = r.epoch, r.seq
	}
	r.m.ready.Store(false)
	r.epoch = ""
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
	r.mu.Lock()
	ce, cs := r.coldEpoch, r.coldSeq
	r.mu.Unlock()
	resp, cancel, err := memGet(r.pumpAddr, fmt.Sprintf("/internal/mem/dump?ce=%s&cs=%d", ce, cs), 180*time.Second)
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()
	keep := resp.Header.Get("X-Mem-Cold") == "keep"
	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	var dump memDump
	if err := gob.NewDecoder(zr).Decode(&dump); err != nil {
		return err
	}
	var d *memData
	if !keep {
		if d, err = memDataFromDump(&dump, nil); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen != g {
		return nil // 途中で無効化された → ループが再試行する
	}
	r.m.mu.Lock()
	if keep {
		if r.coldEpoch != ce || r.coldSeq != cs {
			r.m.mu.Unlock()
			return fmt.Errorf("mem: cold base changed during resync")
		}
		if d, err = memDataFromDump(&dump, r.m.d); err != nil {
			r.m.mu.Unlock()
			r.coldEpoch = ""
			return err
		}
	}
	r.m.d = d
	r.m.mu.Unlock()
	r.coldEpoch = ""
	r.epoch, r.seq = dump.Epoch, dump.Seq
	// lease は次の push/heartbeat (probe 以外) でだけ延長する (pump が lastGrantAt で上界を把握するため)
	r.m.ready.Store(true)
	if r.farm != nil {
		go r.farm.onReset()
	}
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
	var applied []memChange
	defer func() {
		r.seqCond.Broadcast()
		if r.farm != nil && len(applied) > 0 {
			go r.farm.onApplied(applied)
		}
	}()
	for _, b := range req.Batches {
		if b.Seq <= r.seq {
			continue
		}
		if b.Seq > r.seq+1 {
			missing, err := r.pull(req.Epoch, r.seq+1, b.Seq-1)
			if err == nil {
				for _, mb := range missing {
					if err = r.applyTracked(&applied, mb.Changes); err != nil {
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
		if err := r.applyTracked(&applied, b.Changes); err != nil {
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
				if err = r.applyTracked(&applied, mb.Changes); err != nil {
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

func (r *memReceiver) applyTracked(applied *[]memChange, changes []memChange) error {
	if err := r.m.applyBatch(changes); err != nil {
		return err
	}
	*applied = append(*applied, changes...)
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
		var req memPushRequest
		if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
			r.m.ready.Store(false)
			go r.invalidate()
			return c.NoContent(http.StatusBadRequest)
		}
		return c.JSON(http.StatusOK, memPushResponse{OK: r.push(req)})
	})
	e.POST("/internal/mem/invalidate", func(c *echo.Context) error {
		if !memAuthOK(c) {
			return c.NoContent(http.StatusForbidden)
		}
		g := r.invalidateKeep(true)
		if c.QueryParam("wait") == "true" {
			r.waitResynced(g, 44*time.Second)
		}
		return c.JSON(http.StatusOK, memPushResponse{OK: r.m.ready.Load()})
	})
}
