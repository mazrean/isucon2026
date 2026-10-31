package isureal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ISUREAL_IMG_FRIEND_PUSH=1 (asia1 = sender, every app host = receiver for the
// hit counter): after the commit of an image on asia1, push the bytes ahead of
// any request to the eu/am app hosts — but only while asia1's NIC is not busy
// (tx rate < ISUREAL_IMG_FRIEND_PUSH_MAX_MBPS, sampled every 100ms) and never
// during a GP window. Busy = wait, not drop; only a full queue drops.
//
// Priority: regions where the owner has friends (hi) > the rest (lo, only with
// ISUREAL_IMG_FRIEND_PUSH_ALL=1). The receiver is the existing
// POST /internal/images/:id (imageStore.put: memory LRU + imgcache disk, which
// has its own budget and is wiped on deploy). Validity is untouched: the
// receiver's getImageHandler still runs memImageValid / checkImageValidity; a
// push that has not arrived falls back to the pull path.
var (
	imgFriendPushOn      = os.Getenv("ISUREAL_IMG_FRIEND_PUSH") == "1"
	imgFriendPushGP      = os.Getenv("ISUREAL_IMG_FRIEND_PUSH_GP") == "1" // 1 = also send during GP windows
	imgFriendPushAll     = os.Getenv("ISUREAL_IMG_FRIEND_PUSH_ALL") == "1"
	imgFriendPushMaxBps  = int64(envIntOrDefault("ISUREAL_IMG_FRIEND_PUSH_MAX_MBPS", 300)) * 1_000_000
	imgFriendPushConc    = envIntOrDefault("ISUREAL_IMG_FRIEND_PUSH_CONC", 2)
	imgFriendPushQueue   = envIntOrDefault("ISUREAL_IMG_FRIEND_PUSH_QUEUE", 512)
	imgFriendPushMaxByte = int64(envIntOrDefault("ISUREAL_IMG_FRIEND_PUSH_QUEUE_MB", 96)) << 20
	imgFriendPushIface   = os.Getenv("ISUREAL_IMG_FRIEND_PUSH_IF")
	// ISUREAL_IMG_FRIEND_PUSH_REV=1: europe1/america1 also push their LOCALIMG
	// images (to asia1 and to each other) by the same rule.
	imgFriendPushRev = os.Getenv("ISUREAL_IMG_FRIEND_PUSH_REV") == "1"
	// receivers without an image disk cache (asia1, ISUREAL_IMAGE_CACHE_DIR=-)
	// keep pushed images in this separate LRU, so they never grow the main one.
	imgFriendRecvLRU = newImageLRU(int64(envIntOrDefault("ISUREAL_IMG_FRIEND_PUSH_RECV_MB", 32)) << 20)
)

const imgFriendHeader = "X-Isureal-Img-Friend"

var imgFriendStats struct {
	queuedHi, queuedMid, queuedLo, sent, sentBytes, dropped, errs, gpWait, busyWait, recv, recvHit atomic.Int64
}

// friendPusher is non-nil only on asia1 with the flag on.
var friendPusher *imgFriendPusher

type imgFriendJob struct {
	id  string
	obj *imageObject
	sig *imgReplSigOnce
}

type imgFriendDest struct {
	region string
	addr   string
	hi, mid, lo chan imgFriendJob
}

type imgFriendPusher struct {
	mem    *memStore
	region string
	secret string
	dests  []*imgFriendDest
	client *http.Client
	queued atomic.Int64 // bytes pinned by queued jobs
	// gate returns true when sending is allowed now (idle NIC, no GP).
	gate func(dest string) bool
	txBps atomic.Int64
}

func newImgFriendPusher(mem *memStore, region, secret string, peers map[string]string) *imgFriendPusher {
	p := &imgFriendPusher{mem: mem, region: region, secret: secret}
	p.client = &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: imgFriendPushConc * 2,
			IdleConnTimeout:     5 * time.Minute,
			DisableCompression:  true,
		},
	}
	for r, a := range peers {
		if r == region || a == "" {
			continue
		}
		p.dests = append(p.dests, &imgFriendDest{region: r, addr: a,
			hi: make(chan imgFriendJob, imgFriendPushQueue), mid: make(chan imgFriendJob, imgFriendPushQueue), lo: make(chan imgFriendJob, imgFriendPushQueue)})
	}
	p.gate = p.defaultGate
	return p
}

func (p *imgFriendPusher) start() {
	go p.txSampler()
	for _, d := range p.dests {
		for range imgFriendPushConc {
			go p.worker(d)
		}
	}
	go imgFriendStatsLogger()
}

// startImgFriendPush is called by registerLocalImg.
func startImgFriendPush(h *HandlerContext) {
	region := h.Config.Region
	sender := region == "asia" || (imgFriendPushRev && localImgOn && localImgTags[region] != "")
	if !imgFriendPushOn || !sender || forwardSecret == "" || h.Mem == nil {
		if imgFriendPushOn {
			go imgFriendStatsLogger() // receivers: recv / hit counters
		}
		return
	}
	peers := map[string]string{"asia": asiaLocalImgAddr}
	for r, a := range localImgOrigins {
		peers[r] = a
	}
	friendPusher = newImgFriendPusher(h.Mem, region, forwardSecret, peers)
	friendPusher.start()
	slog.Info("img friend push on", "dests", len(friendPusher.dests), "max_mbps", imgFriendPushMaxBps/1_000_000, "gp", imgFriendPushGP, "all", imgFriendPushAll)
}

// friendRegions returns the regions of userID's friends (non-nil; empty when
// the user is unknown or mem is not ready).
func (d *memData) friendRegions(userID string) map[string]bool {
	out := map[string]bool{}
	if d == nil {
		return out
	}
	u := d.users[userID]
	if u == nil {
		return out
	}
	for id := range u.friends {
		if f := d.users[id]; f != nil && f.Region != "" {
			out[f.Region] = true
		}
	}
	return out
}

func (p *imgFriendPusher) friendRegionsOf(userID string) map[string]bool {
	if p.mem == nil || !p.mem.ready.Load() {
		return map[string]bool{}
	}
	p.mem.mu.RLock()
	defer p.mem.mu.RUnlock()
	return p.mem.d.friendRegions(userID)
}

// enqueue never blocks (called right after the commit).
// visibility ranks the non-friend pushes (ALL): public / fof posts (feeds) first.
func (p *imgFriendPusher) enqueue(id string, obj *imageObject, visibility string) {
	if p == nil || !safeImageID(id) || obj == nil || obj.Owner == "" || len(obj.Data) == 0 {
		return
	}
	regions := p.friendRegionsOf(obj.Owner)
	sig := &imgReplSigOnce{}
	for _, d := range p.dests {
		tier := 2
		if regions[d.region] {
			tier = 0
		} else if !imgFriendPushAll {
			continue
		} else if visibility == "public" || visibility == "friends_of_friends" {
			tier = 1
		}
		p.submit(d, imgFriendJob{id: id, obj: obj, sig: sig}, tier)
	}
}

func (p *imgFriendPusher) submit(d *imgFriendDest, j imgFriendJob, tier int) {
	n := int64(len(j.obj.Data))
	if p.queued.Add(n) > imgFriendPushMaxByte {
		p.queued.Add(-n)
		imgFriendStats.dropped.Add(1)
		return
	}
	ch, ctr := d.lo, &imgFriendStats.queuedLo
	switch tier {
	case 0:
		ch, ctr = d.hi, &imgFriendStats.queuedHi
	case 1:
		ch, ctr = d.mid, &imgFriendStats.queuedMid
	}
	select {
	case ch <- j:
		ctr.Add(1)
	default:
		p.queued.Add(-n)
		imgFriendStats.dropped.Add(1)
	}
}

func (p *imgFriendPusher) worker(d *imgFriendDest) {
	for {
		// wait for an open gate first, then take the highest-priority job
		for !p.gate(d.region) {
			time.Sleep(100 * time.Millisecond)
		}
		var j imgFriendJob
		select {
		case j = <-d.hi:
		default:
			select {
			case j = <-d.hi:
			case j = <-d.mid:
			default:
				select {
				case j = <-d.hi:
				case j = <-d.mid:
				case j = <-d.lo:
				}
			}
		}
		for !p.gate(d.region) { // the gate may have closed while idle
			time.Sleep(100 * time.Millisecond)
		}
		p.queued.Add(-int64(len(j.obj.Data)))
		if err := p.send(d.addr, j); err != nil {
			imgFriendStats.errs.Add(1) // no retry: the pull path still works
			continue
		}
		imgFriendStats.sent.Add(1)
		imgFriendStats.sentBytes.Add(int64(len(j.obj.Data)))
	}
}

func (p *imgFriendPusher) defaultGate(dest string) bool {
	if !imgFriendPushGP && p.mem != nil {
		now := time.Now()
		if _, gp := p.mem.gpWindowDeadline(p.region, now); gp {
			imgFriendStats.gpWait.Add(1)
			return false
		}
		if _, gp := p.mem.gpWindowDeadline(dest, now); gp {
			imgFriendStats.gpWait.Add(1)
			return false
		}
	}
	if p.txBps.Load() >= imgFriendPushMaxBps {
		imgFriendStats.busyWait.Add(1)
		return false
	}
	return true
}

func (p *imgFriendPusher) send(addr string, j imgFriendJob) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/internal/images/"+j.id, bytes.NewReader(j.obj.Data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(forwardedSecretHeader, p.secret)
	req.Header.Set(imgReplOwnerHeader, j.obj.Owner)
	req.Header.Set(imgReplMediaTypeHeader, j.obj.MediaType)
	req.Header.Set(imgReplSigHeader, j.sig.get(p.secret, j.id, j.obj))
	req.Header.Set(imgFriendHeader, "1")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("img friend push %s: status %d", addr, resp.StatusCode)
	}
	return nil
}

// ---------- NIC tx rate ----------

func imgFriendPickIface() string {
	if imgFriendPushIface != "" {
		return imgFriendPushIface
	}
	ents, _ := os.ReadDir("/sys/class/net")
	best, bestTx := "", int64(-1)
	for _, e := range ents {
		n := e.Name()
		if n == "lo" || strings.HasPrefix(n, "docker") || strings.HasPrefix(n, "veth") {
			continue
		}
		if tx, ok := readTxBytes(n); ok && tx > bestTx {
			best, bestTx = n, tx
		}
	}
	return best
}

func readTxBytes(iface string) (int64, bool) {
	b, err := os.ReadFile(filepath.Join("/sys/class/net", iface, "statistics", "tx_bytes"))
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	return v, err == nil
}

// txSampler keeps txBps (bits/s over the last 100ms). Unreadable = 0 (idle).
func (p *imgFriendPusher) txSampler() {
	iface := imgFriendPickIface()
	slog.Info("img friend push tx iface", "iface", iface)
	last, ok := readTxBytes(iface)
	lastT := time.Now()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for now := range t.C {
		cur, ok2 := readTxBytes(iface)
		if ok && ok2 {
			if dt := now.Sub(lastT).Seconds(); dt > 0 {
				p.txBps.Store(int64(float64(cur-last) * 8 / dt))
			}
		}
		last, ok, lastT = cur, ok2, now
	}
}

// ---------- receiver hit counter ----------

// imgFriendRecvIDs: IDs received by push and not yet requested (bounded by a reset).
var (
	imgFriendRecvIDs sync.Map
	imgFriendRecvN   atomic.Int64
)

const imgFriendRecvMaxIDs = 200000

func imgFriendRecvMark(id string) {
	if !imgFriendPushOn {
		return
	}
	imgFriendStats.recv.Add(1)
	if imgFriendRecvN.Add(1) > imgFriendRecvMaxIDs {
		imgFriendRecvIDs.Clear()
		imgFriendRecvN.Store(0)
	}
	imgFriendRecvIDs.Store(id, struct{}{})
}

// imgFriendRecvPut stores a friend push on a host without an image disk cache
// in imgFriendRecvLRU (bounded) instead of the main LRU. false = caller puts.
func imgFriendRecvPut(s *imageStore, req *http.Request, id string, obj *imageObject) bool {
	if !imgFriendPushOn || s.dir != "" || req.Header.Get(imgFriendHeader) != "1" {
		return false
	}
	imgFriendRecvLRU.add(id, obj)
	return true
}

// imgFriendRecvTake is consulted by imageStore.load before the disk / DB.
func imgFriendRecvTake(id string) *imageObject {
	if !imgFriendPushOn {
		return nil
	}
	return imgFriendRecvLRU.take(id)
}

// imgFriendRecvHit counts the first request for a pushed image.
func imgFriendRecvHit(id string) {
	if !imgFriendPushOn || imgFriendRecvN.Load() == 0 {
		return
	}
	if _, ok := imgFriendRecvIDs.LoadAndDelete(id); ok {
		imgFriendStats.recvHit.Add(1)
	}
}

func imgFriendStatsLogger() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	s := &imgFriendStats
	ctrs := []*atomic.Int64{&s.queuedHi, &s.queuedMid, &s.queuedLo, &s.sent, &s.sentBytes, &s.dropped, &s.errs, &s.gpWait, &s.busyWait, &s.recv, &s.recvHit}
	for range t.C {
		v := make([]int64, len(ctrs))
		var sum int64
		for i, c := range ctrs {
			v[i] = c.Swap(0)
			sum += v[i]
		}
		if sum == 0 {
			continue
		}
		var txMbps, qBytes int64
		if p := friendPusher; p != nil {
			txMbps, qBytes = p.txBps.Load()/1_000_000, p.queued.Load()
		}
		slog.Info("imgfriendpush", "queued_hi", v[0], "queued_mid", v[1], "queued_lo", v[2], "sent", v[3], "sent_bytes", v[4],
			"dropped", v[5], "err", v[6], "gp_wait_ticks", v[7], "busy_wait_ticks", v[8],
			"recv", v[9], "recv_hit", v[10], "tx_mbps", txMbps, "queued_bytes", qBytes)
	}
}
