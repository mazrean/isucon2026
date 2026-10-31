package isureal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_MEMORIES_CACHE=1 (needs ISUREAL_MEM=1): the DB-path answers of
// GET /api/memories, /api/memories/:date and /api/me/export are kept per
// (viewer, period, cursor) as the exact response bytes, next to the in-memory
// read model (memData.rc), and dropped by the change stream that feeds the
// model (applyChange). A response depends on:
//   - the viewer's posts (any posts row of that author)     -> tag "o"+author
//   - the viewer's user row (author fields, status)         -> tags "o"+id, "u"+id
//   - the windows of the posts shown (deadline / ended_at)  -> tag "w"+window
//   - RealMojis / comments of the posts shown               -> tag "p"+post
//   - reactor / commenter user rows                         -> tag "u"+id
//
// Users only go active -> deleted; an inactive user who was left out of a
// response cannot come back (a deleted -> active row still clears all).
// Signed image URLs are deterministic per 10-minute exp bucket
// (signedImageURL); an entry is served only in the bucket it was built in,
// so the bytes are the ones the DB path would build now.
//
// Fill race: begin() takes the change counter before the DB query; put()
// drops the entry if any of its tags (or a clear) was invalidated after that.
// A change committed before the query is in the result; one applied after
// begin() vetoes the put; one applied later removes the stored entry.
// Reads and fills only happen while the model is on and the request's
// watermark is reached (onCtx), as for the other memory reads (RYW).

var (
	memRespCacheOn       = os.Getenv("ISUREAL_MEMORIES_CACHE") == "1"
	memRespCacheMaxBytes = envIntDefault("ISUREAL_MEMORIES_CACHE_MB", 256) << 20
	memRespCacheMaxEntry = 16 << 20

	memRespHits, memRespMiss, memRespPuts, memRespVeto atomic.Int64 // テスト / 観測用
)

func envIntDefault(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

const (
	memRespMemories byte = 'm'
	memRespExport   byte = 'e'
)

type memRespKey struct {
	kind       byte
	user       string
	start, end int64
	hasBefore  bool
	beforeAt   int64
	beforeID   string
}

func memoriesRespKey(userID string, start, end time.Time, beforeAt *time.Time, beforeID string) memRespKey {
	k := memRespKey{kind: memRespMemories, user: userID, start: start.UnixNano(), end: end.UnixNano(), beforeID: beforeID}
	if beforeAt != nil {
		k.hasBefore, k.beforeAt = true, beforeAt.UnixNano()
	}
	return k
}

type memRespEntry struct {
	body   []byte
	tags   []string
	bucket int64
}

type memRespCache struct {
	mu      sync.Mutex
	seq     uint64
	floor   uint64 // fills begun at or before floor are not stored
	entries map[memRespKey]*memRespEntry
	byTag   map[string]map[memRespKey]struct{}
	tagSeq  map[string]uint64
	bytes   int
}

// memURLBucket is the exp signedImageURL uses now.
func memURLBucket() int64 {
	exp := time.Now().Add(imageURLLifetime).Unix()
	return exp - exp%int64((imageURLLifetime-imageURLReuseMargin)/time.Second)
}

func (c *memRespCache) get(k memRespKey) []byte {
	b := memURLBucket()
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[k]; e != nil && e.bucket == b {
		memRespHits.Add(1)
		return e.body
	}
	memRespMiss.Add(1)
	return nil
}

type memRespFill struct {
	seq    uint64
	bucket int64
}

func (c *memRespCache) begin() memRespFill {
	b := memURLBucket()
	c.mu.Lock()
	defer c.mu.Unlock()
	return memRespFill{seq: c.seq, bucket: b}
}

func (c *memRespCache) put(k memRespKey, f memRespFill, body []byte, tags []string) {
	if len(body) > memRespCacheMaxEntry || memURLBucket() != f.bucket {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.seq < c.floor {
		memRespVeto.Add(1)
		return
	}
	for _, t := range tags {
		if c.tagSeq[t] > f.seq {
			memRespVeto.Add(1)
			return
		}
	}
	if c.entries == nil {
		c.entries = map[memRespKey]*memRespEntry{}
		c.byTag = map[string]map[memRespKey]struct{}{}
	}
	if old := c.entries[k]; old != nil {
		c.removeLocked(k, old)
	}
	if c.bytes+len(body) > memRespCacheMaxBytes {
		c.entries = map[memRespKey]*memRespEntry{}
		c.byTag = map[string]map[memRespKey]struct{}{}
		c.bytes = 0
	}
	c.entries[k] = &memRespEntry{body: body, tags: tags, bucket: f.bucket}
	c.bytes += len(body)
	for _, t := range tags {
		s := c.byTag[t]
		if s == nil {
			s = map[memRespKey]struct{}{}
			c.byTag[t] = s
		}
		s[k] = struct{}{}
	}
	memRespPuts.Add(1)
}

func (c *memRespCache) removeLocked(k memRespKey, e *memRespEntry) {
	delete(c.entries, k)
	c.bytes -= len(e.body)
	for _, t := range e.tags {
		if s := c.byTag[t]; s != nil {
			delete(s, k)
			if len(s) == 0 {
				delete(c.byTag, t)
			}
		}
	}
}

func (c *memRespCache) invalidate(tags ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	if len(c.tagSeq) > 1<<18 {
		// forget old tag versions; fills begun before now are dropped instead
		c.tagSeq = nil
		c.floor = c.seq
	}
	if c.tagSeq == nil {
		c.tagSeq = map[string]uint64{}
	}
	for _, t := range tags {
		c.tagSeq[t] = c.seq
		for k := range c.byTag[t] {
			if e := c.entries[k]; e != nil {
				c.removeLocked(k, e)
			}
		}
	}
}

func (c *memRespCache) clearAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	c.floor = c.seq
	c.entries, c.byTag, c.tagSeq, c.bytes = nil, nil, nil, 0
}

// respCache returns the cache to use for this request, or nil (DB path only).
func (m *memStore) respCache(ctx context.Context) *memRespCache {
	if !memRespCacheOn || m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.onCtx(ctx) {
		return nil
	}
	return &m.d.rc
}

// memChangeInvalidate is called by applyChange with the parsed row, before it is applied.
func (d *memData) respInvalidateUser(id, status string) {
	if !memRespCacheOn {
		return
	}
	if old := d.users[id]; old != nil && old.Status != "active" && status == "active" {
		d.rc.clearAll()
		return
	}
	d.rc.invalidate("o"+id, "u"+id)
}

func (d *memData) respInvalidatePost(postID string) {
	if !memRespCacheOn {
		return
	}
	if postID == "" {
		d.rc.clearAll()
		return
	}
	d.rc.invalidate("p" + postID)
}

// memoriesTags lists what a Memories response depends on.
func memoriesTags(viewerID string, r getMemoriesResponse) []string {
	tags := []string{"o" + viewerID, "u" + viewerID}
	seen := map[string]bool{tags[0]: true, tags[1]: true}
	add := func(t string) {
		if !seen[t] {
			seen[t] = true
			tags = append(tags, t)
		}
	}
	for _, it := range r.Items {
		add("p" + it.ID)
		add("w" + it.PostingWindowID)
		add("u" + it.Author.ID)
		for _, x := range it.RealMojis {
			add("u" + x.Reactor.ID)
		}
		for _, x := range it.Comments {
			add("u" + x.Author.ID)
		}
	}
	return tags
}

// encodeJSONLikeEcho is echo's DefaultJSONSerializer (json.Encoder, trailing newline).
func encodeJSONLikeEcho(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// serveMemories is the DB path of the two Memories handlers, through the cache when it is usable.
func serveMemories(h *HandlerContext, c *echo.Context, userID string, start, end time.Time, beforeAt *time.Time, beforeID string, wrap func(getMemoriesResponse) any) error {
	ctx := c.Request().Context()
	rc := h.Mem.respCache(ctx)
	var key memRespKey
	var fill memRespFill
	if rc != nil {
		key = memoriesRespKey(userID, start, end, beforeAt, beforeID)
		if b := rc.get(key); b != nil {
			return c.JSONBlob(http.StatusOK, b)
		}
		fill = rc.begin()
	}
	defer heavyAcquire(ctx)()
	response, err := listMemories(ctx, h.Reader(), userID, start, end, beforeAt, beforeID)
	if err != nil {
		return err
	}
	if rc == nil {
		return c.JSON(http.StatusOK, wrap(response))
	}
	b, err := encodeJSONLikeEcho(wrap(response))
	if err != nil {
		return err
	}
	rc.put(key, fill, b, memoriesTags(userID, response))
	return c.JSONBlob(http.StatusOK, b)
}
