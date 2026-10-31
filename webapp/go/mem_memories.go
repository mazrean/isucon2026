package isureal

import (
	"net/http"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// ISUREAL_MEMORIES_MEM=1 (needs ISUREAL_MEM=1): GET /api/memories and
// /api/memories/:date are answered from the in-memory store when the asked
// period starts at or after memData.hotFrom (every post on those dates is in
// memory). Otherwise, or when the store is not usable, the DB path runs.
// The rows passed through are the same as listMemories': the viewer's own
// non-deleted posts (author active) with window local_date in [start, end),
// newest first, 51 fetched / 50 kept; current RealMojis by active reactors,
// newest first, and non-deleted comments by active authors, oldest first, 50
// per post (attachMemoryInteractions).

var memoriesMem = os.Getenv("ISUREAL_MEMORIES_MEM") == "1"

var memMemoriesServed, memMemoriesItems atomic.Int64 // diff test の確認用

func (m *memStore) memories(c *echo.Context, userID string, start, end time.Time, beforeAt *time.Time, beforeID string) (bool, getMemoriesResponse, error) {
	var response getMemoriesResponse
	if !memoriesMem {
		return false, response, nil
	}
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, response, nil
	}
	d := m.d
	if !d.hotKnown || start.Before(d.hotFrom) {
		m.mu.RUnlock()
		return false, response, nil
	}
	u := d.users[userID]
	if u == nil {
		m.mu.RUnlock()
		return false, response, nil
	}
	if (beforeAt == nil) != (beforeID == "") {
		m.mu.RUnlock()
		return true, response, NewAPIError(http.StatusBadRequest, "invalid_pagination")
	}
	var cursorAt time.Time
	if beforeAt != nil {
		cursorAt = memRoundCursor(*beforeAt)
	}
	var page []*memPost
	if u.active() {
		for _, p := range u.posts {
			if p.deleted {
				continue
			}
			ld := p.window.LocalDate
			if ld.Before(start) || !ld.Before(end) {
				continue
			}
			if beforeAt != nil && !memTimeIDLess(p.PostedAt, p.ID, cursorAt, beforeID) {
				continue
			}
			page = append(page, p)
		}
	}
	// posted_at DESC, id DESC
	slices.SortFunc(page, func(a, b *memPost) int {
		if memTimeIDLess(b.PostedAt, b.ID, a.PostedAt, a.ID) {
			return -1
		}
		if memTimeIDLess(a.PostedAt, a.ID, b.PostedAt, b.ID) {
			return 1
		}
		return 0
	})
	if len(page) > 51 {
		page = page[:51]
	}
	posts := make([]PostWithAuthor, len(page))
	var reactions []RealMojiWithReactor
	var comments []CommentWithAuthor
	for i, p := range page {
		posts[i] = PostWithAuthor{Post: p.full(), Author: p.author.User}
		if i >= 50 {
			continue
		}
		n := 0
		for j := len(p.realmojis) - 1; j >= 0 && n < 50; j-- {
			r := p.realmojis[j]
			if !r.reactor.active() {
				continue
			}
			n++
			reactions = append(reactions, RealMojiWithReactor{
				RealMoji: RealMoji{ID: r.ID, PostID: r.PostID, ReactorID: r.reactor.ID, ImageID: r.ImageID, ReactionType: r.ReactionType, CreatedAt: r.CreatedAt},
				Reactor:  r.reactor.User,
			})
		}
		n = 0
		for _, cm := range p.comments {
			if n >= 50 {
				break
			}
			if !cm.author.active() {
				continue
			}
			n++
			comments = append(comments, CommentWithAuthor{
				Comment: Comment{ID: cm.ID, PostID: cm.PostID, AuthorID: cm.author.ID, Body: cm.Body, CreatedAt: cm.CreatedAt},
				Author:  cm.author.User,
			})
		}
	}
	m.mu.RUnlock()

	reqs := make([]imageURLReq, 0, 2*len(posts))
	for _, post := range posts {
		reqs = appendImageURLReq(reqs, post.Author, post.ImageID)
		reqs = appendUserProfileImageReq(reqs, post.Author)
	}
	prewarmImageURLs(reqs)
	items := make([]memoryPostResponse, 0, len(posts))
	for _, post := range posts {
		items = append(items, memoryPostResponse{
			createPostResponse: newPostResponse(post.Post, post.Author),
			RealMojis:          make([]replaceRealMojiResponse, 0),
			Comments:           make([]createCommentResponse, 0),
		})
	}
	response.HasMore = len(items) > 50
	if response.HasMore {
		items = items[:50]
	}
	attachMemoryInteractions(items, reactions, comments)
	response.Items = items
	memMemoriesServed.Add(1)
	memMemoriesItems.Add(int64(len(items)))
	return true, response, nil
}

// setHotFrom is called by the loaders after the windows are applied, with the
// earliest open window's date (posts are loaded from that date on). With no
// open window no posts are loaded; every window then on file is closed (no
// new posts), so the hot range starts the day after the latest one.
func (d *memData) setHotFrom(minDate *time.Time) {
	if minDate != nil {
		d.hotFrom, d.hotKnown = *minDate, true
		return
	}
	var last time.Time
	for _, w := range d.windows {
		if w.LocalDate.After(last) {
			last = w.LocalDate
		}
	}
	if !last.IsZero() {
		last = last.AddDate(0, 0, 1)
	}
	d.hotFrom, d.hotKnown = last, true
}
