package isureal

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// 画像バイト列を他リージョンのアプリへ push 複製する。画像は ID ごとに不変で、
// 受信側に無ければ DB から取るので、複製は非同期・ベストエフォートでよい。

const (
	imgReplOwnerHeader     = "X-Isureal-Img-Owner"
	imgReplMediaTypeHeader = "X-Isureal-Img-Media-Type"
	imgReplSigHeader       = "X-Isureal-Img-Sig"
	imgReplWorkers         = 4
	imgReplMaxAttempts     = 3
	imgReplMaxBody         = 32 << 20
)

type imgReplJob struct {
	peer    string
	id      string
	obj     *imageObject
	attempt int
}

type imgReplicator struct {
	peers   []string
	secret  string
	jobs    chan imgReplJob
	client  *http.Client
	dropped atomic.Int64
	queued  atomic.Int64 // bytes referenced by queued jobs (kept out of the GC'd heap budget)
}

// imgReplMaxQueuedBytes bounds the memory pinned by unsent images; beyond it new
// jobs are dropped and peers fall back to fetching on a cache miss.
const imgReplMaxQueuedBytes = 128 << 20

var imageReplicator = newImgReplicator(os.Getenv("ISUREAL_IMG_PEERS"), forwardSecret)

func newImgReplicator(peersEnv, secret string) *imgReplicator {
	r := &imgReplicator{secret: secret}
	for _, p := range strings.Split(peersEnv, ",") {
		if p = strings.TrimSpace(p); p != "" {
			r.peers = append(r.peers, p)
		}
	}
	if len(r.peers) == 0 || secret == "" {
		r.peers = nil
		return r
	}
	r.jobs = make(chan imgReplJob, 16384)
	r.client = &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: imgReplWorkers * 2,
			IdleConnTimeout:     5 * time.Minute,
		},
	}
	workers := imgReplWorkers
	if imgDedupOn {
		workers = 2 * imgReplWorkers // have 確認で 1 往復増えるぶん、並列を増やして送出量を保つ
	}
	for range workers {
		go r.worker()
	}
	return r
}

// replicateImage は、画像 INSERT の commit 後に呼ぶ。自ホストのキャッシュへ格納し、
// ピアへの送信をキューに積んで即座に戻る。
func replicateImage(id, owner, mediaType string, data []byte) {
	obj := &imageObject{Owner: owner, MediaType: mediaType, Data: data}
	images.put(id, obj)
	imageReplicator.enqueue(id, obj)
}

func (r *imgReplicator) enqueue(id string, obj *imageObject) {
	if len(r.peers) == 0 || !safeImageID(id) {
		return
	}
	for _, p := range r.peers {
		r.submit(imgReplJob{peer: p, id: id, obj: obj})
	}
}

func (r *imgReplicator) submit(j imgReplJob) {
	n := int64(len(j.obj.Data))
	if r.queued.Add(n) > imgReplMaxQueuedBytes {
		r.queued.Add(-n)
		if d := r.dropped.Add(1); d%100 == 1 {
			log.Printf("imgrepl: queue over %d bytes, dropped %d", imgReplMaxQueuedBytes, d)
		}
		return
	}
	select {
	case r.jobs <- j:
	default:
		r.queued.Add(-n)
		if n := r.dropped.Add(1); n%100 == 1 {
			log.Printf("imgrepl: queue full, dropped %d", n)
		}
	}
}

func (r *imgReplicator) worker() {
	for j := range r.jobs {
		r.queued.Add(-int64(len(j.obj.Data)))
		if err := r.send(j); err != nil {
			j.attempt++
			if j.attempt >= imgReplMaxAttempts {
				continue
			}
			// ワーカー枠を塞がないよう、待ち時間はタイマーで再投入する。
			delay := time.Duration(j.attempt*j.attempt) * 300 * time.Millisecond
			time.AfterFunc(delay, func() { r.submit(j) })
		}
	}
}

func imgReplSign(secret, id, owner, mediaType string, data []byte) string {
	sum := sha256.Sum256(data)
	m := hmac.New(sha256.New, []byte(secret))
	io.WriteString(m, "imgrepl\n"+id+"\n"+owner+"\n"+mediaType+"\n")
	m.Write(sum[:])
	return hex.EncodeToString(m.Sum(nil))
}

func (r *imgReplicator) send(j imgReplJob) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if imgDedupOn && j.attempt == 0 {
		have, err := r.imgHave(ctx, j.peer, j.id)
		if err != nil {
			imgStats.haveErr.Add(1)
		} else if have {
			imgStats.pushSkip.Add(1)
			imgStats.pushSkipBytes.Add(int64(len(j.obj.Data)))
			return nil
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+j.peer+"/internal/images/"+j.id, bytes.NewReader(j.obj.Data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(forwardedSecretHeader, r.secret)
	req.Header.Set(imgReplOwnerHeader, j.obj.Owner)
	req.Header.Set(imgReplMediaTypeHeader, j.obj.MediaType)
	req.Header.Set(imgReplSigHeader, imgReplSign(r.secret, j.id, j.obj.Owner, j.obj.MediaType, j.obj.Data))
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("imgrepl: %s status %d", j.peer, resp.StatusCode)
	}
	imgStats.pushSent.Add(1)
	imgStats.pushBytes.Add(int64(len(j.obj.Data)))
	return nil
}

func imgReplPeerSource(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.Equal(net.IPv4(192, 168, 1, 10)) || ip.Equal(net.IPv4(192, 168, 2, 10)) || ip.Equal(net.IPv4(192, 168, 3, 10)))
}

// imageReplReceiveHandler は POST /internal/images/:imageId を受ける。
// 共有秘密・送信元 IP に加え、ピアの nginx 経由で外部クライアントが送り込めないよう
// 本文を含む HMAC 署名を検証する。
func imageReplReceiveHandler(store *imageStore, secret string) echo.HandlerFunc {
	return func(c *echo.Context) error {
		req := c.Request()
		if secret == "" || !imgReplPeerSource(req.RemoteAddr) ||
			subtle.ConstantTimeCompare([]byte(req.Header.Get(forwardedSecretHeader)), []byte(secret)) != 1 {
			return c.NoContent(http.StatusForbidden)
		}
		id := c.Param("imageId")
		owner := req.Header.Get(imgReplOwnerHeader)
		mediaType := req.Header.Get(imgReplMediaTypeHeader)
		if !safeImageID(id) || owner == "" || mediaType == "" {
			return c.NoContent(http.StatusBadRequest)
		}
		if imgDedupOn {
			if store.has(id) {
				imgStats.recvDup.Add(1)
			}
			done := store.beginRecv(id)
			defer done()
		}
		data, err := io.ReadAll(http.MaxBytesReader(c.Response(), req.Body, imgReplMaxBody))
		if err != nil {
			return c.NoContent(http.StatusBadRequest)
		}
		if !hmac.Equal([]byte(req.Header.Get(imgReplSigHeader)), []byte(imgReplSign(secret, id, owner, mediaType, data))) {
			return c.NoContent(http.StatusForbidden)
		}
		store.put(id, &imageObject{Owner: owner, MediaType: mediaType, Data: data})
		imgStats.recvN.Add(1)
		imgStats.recvBytes.Add(int64(len(data)))
		return c.NoContent(http.StatusNoContent)
	}
}

func registerImageReplicationRoutes(e *echo.Echo) {
	e.POST("/internal/images/:imageId", imageReplReceiveHandler(images, forwardSecret))
	if imgDedupOn {
		registerImgDedupRoutes(e, images, forwardSecret)
	}
}

func subtleEq(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
