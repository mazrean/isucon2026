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
	if !ok {
		return imageValidity{}, false
	}
	return imageValidity{found: true, secretKeyHash: u.SecretKeyHash, valid: true}, true
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
