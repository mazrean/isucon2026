package isureal

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/labstack/echo/v5"
)

// ---- memories / export (ISUREAL_MEM=1) ----

// memListMemoriesOr はメモリが使えればメモリから、そうでなければ元の listMemories で返す。
func memListMemoriesOr(h *HandlerContext, ctx context.Context, userID string, start, end time.Time, beforeAt *time.Time, beforeID string) (getMemoriesResponse, error) {
	if h.Mem.onCtx(ctx) {
		if resp, ok, err := h.Mem.listMemories(userID, start, end, beforeAt, beforeID); ok {
			return resp, err
		}
	}
	return listMemories(ctx, h.Reader(), userID, start, end, beforeAt, beforeID)
}

func (m *memStore) listMemories(userID string, start, end time.Time, beforeAt *time.Time, beforeID string) (getMemoriesResponse, bool, error) {
	var response getMemoriesResponse
	if (beforeAt == nil) != (beforeID == "") {
		return response, true, NewAPIError(http.StatusBadRequest, "invalid_pagination")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.on() {
		return response, false, nil
	}
	u := m.d.users[userID]
	if u == nil {
		return response, false, nil
	}
	var cursor time.Time
	if beforeAt != nil {
		cursor = memRoundCursor(*beforeAt)
	}
	var posts []*memPost
	if u.active() {
		for _, p := range u.posts {
			if p.deleted {
				continue
			}
			ld := p.window.LocalDate
			if ld.Before(start) || !ld.Before(end) {
				continue
			}
			if beforeAt != nil && !memTimeIDLess(p.PostedAt, p.ID, cursor, beforeID) {
				continue
			}
			posts = append(posts, p)
		}
	}
	sort.Slice(posts, func(i, j int) bool { return memPostLess(posts[j], posts[i]) })
	if len(posts) > 51 {
		posts = posts[:51]
	}
	items := make([]memoryPostResponse, 0, len(posts))
	for _, p := range posts {
		items = append(items, memoryPostResponse{
			createPostResponse: newPostResponse(p.full(), u.User),
			RealMojis:          make([]replaceRealMojiResponse, 0),
			Comments:           make([]createCommentResponse, 0),
		})
	}
	response.HasMore = len(items) > 50
	if response.HasMore {
		items = items[:50]
		posts = posts[:50]
	}
	for i, p := range posts {
		item := &items[i]
		for j := len(p.realmojis) - 1; j >= 0 && len(item.RealMojis) < 50; j-- {
			r := p.realmojis[j]
			if !r.reactor.active() {
				continue
			}
			item.RealMojis = append(item.RealMojis, replaceRealMojiResponse{
				ID: r.ID, Reactor: newRelationshipUserResponse(r.reactor.User), ReactionType: r.ReactionType,
				ImageURL: signedImageURL(r.reactor.User, r.ImageID), CreatedAt: r.CreatedAt,
			})
		}
		for _, cm := range p.comments {
			if len(item.Comments) >= 50 {
				break
			}
			if !cm.author.active() {
				continue
			}
			item.Comments = append(item.Comments, createCommentResponse{ID: cm.ID, Author: newRelationshipUserResponse(cm.author.User), Body: cm.Body, CreatedAt: cm.CreatedAt})
		}
	}
	response.Items = items
	return response, true, nil
}

// exportPostsIfOn は export 用の投稿一覧 (posted_at, id 昇順) と作者を返す。
// ver はそのユーザーの投稿・ユーザー行の版 (変更のたびに全体で一意な値になる)。
func (m *memStore) exportPostsIfOn(userID string) ([]Post, User, uint64, bool) {
	if !m.on() {
		return nil, User{}, 0, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.on() {
		return nil, User{}, 0, false
	}
	u := m.d.users[userID]
	if !u.active() {
		return nil, User{}, 0, false
	}
	var list []*memPost
	for _, p := range u.posts {
		if !p.deleted {
			list = append(list, p)
		}
	}
	sort.Slice(list, func(i, j int) bool { return memPostLess(list[i], list[j]) })
	posts := make([]Post, len(list))
	for i, p := range list {
		posts[i] = p.full()
	}
	return posts, u.User, u.exportVer, true
}

func memExportSend(c *echo.Context, body []byte) error {
	c.Response().Header().Set(echo.HeaderContentDisposition, `attachment; filename="isureal-posts.zip"`)
	c.Response().Header().Set(echo.HeaderCacheControl, "no-store")
	return c.Blob(http.StatusOK, "application/zip", body)
}

// export zip のユーザー単位キャッシュ。(版, 署名 exp) が一致する間だけ使う。容量を超えたら全消去。
type memExportEntry struct {
	ver  uint64
	exp  int64
	body []byte
}

var (
	memExportMu     sync.Mutex
	memExportCache  = map[string]*memExportEntry{}
	memExportSize   int64
	memExportBudget = int64(envIntOrDefault("ISUREAL_MEM_EXPORT_CACHE_MB", 512)) << 20
)

func memExportCacheGet(userID string, ver uint64, exp int64) ([]byte, bool) {
	memExportMu.Lock()
	defer memExportMu.Unlock()
	e := memExportCache[userID]
	if e == nil || e.ver != ver || e.exp != exp {
		return nil, false
	}
	return e.body, true
}

func memExportCachePut(userID string, ver uint64, exp int64, body []byte) {
	if int64(len(body)) > memExportBudget/8 {
		return
	}
	memExportMu.Lock()
	defer memExportMu.Unlock()
	if old := memExportCache[userID]; old != nil {
		memExportSize -= int64(len(old.body))
	}
	if memExportSize+int64(len(body)) > memExportBudget {
		memExportCache = map[string]*memExportEntry{}
		memExportSize = 0
	}
	memExportCache[userID] = &memExportEntry{ver: ver, exp: exp, body: body}
	memExportSize += int64(len(body))
}
