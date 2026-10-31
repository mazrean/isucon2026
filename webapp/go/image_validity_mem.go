package isureal

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Positive image-validity answers from the in-memory read model (ISUREAL_MEM=1),
// so GET /api/images/:id does not query PostgreSQL on the hot path.
//
// Only "valid" is answered here; anything else falls back to the DB path
// (checkImageValidity), so a 403 is always decided by the DB as before.
//
//   - owner active + secret key: memData.users (every user, changelog-fed)
//   - profile image: memUser.ProfileImageID
//   - own post image (hot posts): memUser.posts
//   - own post image (old posts, e.g. Memories): imgPostIndex, an
//     image_id -> (post, author) index of non-deleted posts read from the
//     primary after capturing the current *memData (d0). Every post change
//     committed after d0's snapshot reaches d0.posts through the changelog
//     (applyPost inserts unknown posts too), so a post absent from d0.posts is
//     unchanged since the index query, which saw it not deleted. The index is
//     used only while the live memData is still d0 (a reload/initialize swaps
//     the pointer and the index is rebuilt in the background).
//
// Deletion / withdrawal / profile change therefore stop being "valid" as soon
// as the changelog is applied (before the write's response, via the barrier),
// which is no slower than the previous 1 s TTL.

var imgvMemOn = os.Getenv("ISUREAL_IMGV_MEM") != "0"

// ISUREAL_IMGV_PEER_INDEX=1: peers (europe1/america1) build imgPostIndex too.
// Without it every old-post image (the validator's past images) asks the DB
// for validity, which on a peer is cross-region (ISUREAL_DB_READ_HOST = asia2).
// The argument above holds unchanged on a peer: d0 is the peer's live model,
// fed in place by the pump's pushes (applyBatch); a resync swaps the pointer.
var imgvPeerIndex = os.Getenv("ISUREAL_IMGV_PEER_INDEX") == "1"

// ISUREAL_IMG_MEM_ONLY=1 (default 0): the read model also answers "owner not
// active" (found=false -> 403) when it holds the owner row with a non-active
// status. users rows are never reactivated and the model is fed before PG
// (STORE) or from the same changelog, so this equals the PG answer
// (no row for u.status = 'active'). Unknown owners still ask the DB.
var imgMemOnly = os.Getenv("ISUREAL_IMG_MEM_ONLY") == "1"

type imgPostRef struct {
	postID   string
	authorID string
}

var imgPostIndex struct {
	mu      sync.RWMutex
	d       *memData
	m       map[string]imgPostRef
	loading atomic.Bool
	lastTry atomic.Int64
}

// memImageValid reports (validity, true) when the read model proves the image
// valid for owner; ok=false means "ask the DB".
func memImageValid(h *HandlerContext, imageID, owner string) (imageValidity, bool) {
	m := h.Mem
	if !imgvMemOn || m == nil || !m.on() {
		return imageValidity{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.ready.Load() {
		return imageValidity{}, false
	}
	d := m.d
	u := d.users[owner]
	if imgMemOnly && u != nil && !u.active() {
		return imageValidity{found: false}, true
	}
	if !u.active() || u.SecretKeyHash == "" {
		return imageValidity{}, false
	}
	ok := u.ProfileImageID != nil && *u.ProfileImageID == imageID
	if !ok {
		for _, p := range u.posts {
			if p.ImageID == imageID && !p.deleted && p.AuthorID == owner {
				ok = true
				break
			}
		}
	}
	if !ok {
		imgPostIndex.mu.RLock()
		var ref imgPostRef
		var hit bool
		if imgPostIndex.d == d {
			ref, hit = imgPostIndex.m[imageID]
		}
		stale := imgPostIndex.d != d
		imgPostIndex.mu.RUnlock()
		// pump (asia1) only: peers re-sync often and are cross-region from the DB.
		if stale && (h.memPump != nil || imgvPeerIndex) && h.BG != nil && !imgPostIndex.loading.Load() {
			go rebuildImgPostIndex(h)
		}
		if hit && ref.authorID == owner {
			if p := d.posts[ref.postID]; p == nil {
				ok = true // unchanged since the index query (see above)
			} else {
				ok = !p.deleted && p.ImageID == imageID && p.AuthorID == owner
			}
		}
	}
	if !ok && imgMemOnly {
		if ok = memRealMojiImageValid(d, imageID, owner); !ok && h.BG != nil && !imgRealMojiHint.loading.Load() &&
			time.Now().UnixNano()-imgRealMojiHint.lastTry.Load() >= int64(time.Second) {
			go loadImgRealMojiHint(h)
		}
	}
	if !ok {
		return imageValidity{}, false
	}
	return imageValidity{found: true, secretKeyHash: u.SecretKeyHash, valid: true}, true
}

// imgRealMojiHint (ISUREAL_IMG_MEM_ONLY): image_id -> post_id of active
// realmojis. Only a hint: the answer is decided by the post held in memory.
var imgRealMojiHint struct {
	mu      sync.RWMutex
	m       map[string]string
	since   time.Time
	loading atomic.Bool
	lastTry atomic.Int64
}

// memRealMojiImageValid: the realmoji branch of imageValidityQuery, answered
// only when the post is in d.posts (hot posts carry every active realmoji,
// changelog-fed, so replacement / deletion is reflected). Caller holds m.mu.
func memRealMojiImageValid(d *memData, imageID, owner string) bool {
	imgRealMojiHint.mu.RLock()
	postID, hit := imgRealMojiHint.m[imageID]
	imgRealMojiHint.mu.RUnlock()
	if !hit {
		return false
	}
	p := d.posts[postID]
	if p == nil || p.deleted || !p.author.active() {
		return false
	}
	for _, x := range p.realmojis {
		if x.ImageID == imageID && x.reactor != nil && x.reactor.ID == owner {
			return true
		}
	}
	return false
}

// loadImgRealMojiHint merges active realmojis created at or after the last
// load (all of them the first time). Throttled to one load per second; a
// miss in memRealMojiImageValid kicks it, so new realmojis become hints once
// PG has them. Replaced / deleted ones stay as hints (verified in memory).
func loadImgRealMojiHint(h *HandlerContext) {
	now := time.Now().UnixNano()
	if now-imgRealMojiHint.lastTry.Load() < int64(time.Second) || h.BG == nil {
		return
	}
	if !imgRealMojiHint.loading.CompareAndSwap(false, true) {
		return
	}
	defer imgRealMojiHint.loading.Store(false)
	imgRealMojiHint.lastTry.Store(now)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	since := imgRealMojiHint.since
	rows, err := h.BG.QueryContext(ctx, `SELECT image_id, post_id, created_at FROM realmojis WHERE created_at >= $1 AND replaced_at IS NULL AND deleted_at IS NULL`, since)
	if err != nil {
		slog.Warn("imgv: realmoji hint load failed", "error", err)
		return
	}
	defer rows.Close()
	type ent struct{ img, post string }
	var add []ent
	maxAt := since
	for rows.Next() {
		var e ent
		var at time.Time
		if err := rows.Scan(&e.img, &e.post, &at); err != nil {
			slog.Warn("imgv: realmoji hint scan failed", "error", err)
			return
		}
		add = append(add, e)
		if at.After(maxAt) {
			maxAt = at
		}
	}
	if rows.Err() != nil {
		return
	}
	imgRealMojiHint.mu.Lock()
	if imgRealMojiHint.m == nil {
		imgRealMojiHint.m = make(map[string]string, len(add))
	}
	for _, e := range add {
		imgRealMojiHint.m[e.img] = e.post
	}
	imgRealMojiHint.mu.Unlock()
	// re-read the last 10 s: STORE writes rows back to PG after created_at.
	if maxAt.After(since) {
		imgRealMojiHint.since = maxAt.Add(-10 * time.Second) // only the loader touches since
	}
}

func rebuildImgPostIndex(h *HandlerContext) {
	now := time.Now().UnixNano()
	if last := imgPostIndex.lastTry.Load(); now-last < int64(2*time.Second) {
		return
	}
	if !imgPostIndex.loading.CompareAndSwap(false, true) {
		return
	}
	defer imgPostIndex.loading.Store(false)
	imgPostIndex.lastTry.Store(now)

	m := h.Mem
	if m == nil || !m.on() {
		return
	}
	// capture d0 BEFORE reading posts (see the file comment).
	m.mu.RLock()
	d0 := m.d
	m.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	// background pool: keep the request pool free right after initialize.
	rows, err := h.BG.QueryContext(ctx, `SELECT image_id, id, author_id FROM posts WHERE deleted_at IS NULL`)
	if err != nil {
		slog.Warn("imgv: post index load failed", "error", err)
		return
	}
	defer rows.Close()
	idx := make(map[string]imgPostRef, 1<<16)
	authors := make(map[string]string) // intern: few authors, many posts
	for rows.Next() {
		var img, id, author string
		if err := rows.Scan(&img, &id, &author); err != nil {
			slog.Warn("imgv: post index scan failed", "error", err)
			return
		}
		a, ok := authors[author]
		if !ok {
			authors[author], a = author, author
		}
		idx[img] = imgPostRef{postID: id, authorID: a}
	}
	if err := rows.Err(); err != nil {
		slog.Warn("imgv: post index load failed", "error", err)
		return
	}
	imgPostIndex.mu.Lock()
	imgPostIndex.d, imgPostIndex.m = d0, idx
	imgPostIndex.mu.Unlock()
	slog.Info("imgv: post index loaded", "posts", len(idx), "elapsed", time.Since(start))
}
