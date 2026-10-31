package isureal

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// 各ハンドラは handled=false のとき元の DB 経路にフォールバックする。
// レスポンスはロック内で組み立て、書き出しはロック外で行う。

// pgx は timestamptz パラメータをマイクロ秒に切り捨てて送る
func memRoundCursor(t time.Time) time.Time { return t.Truncate(time.Microsecond) }

// sessionFromHeader: (session, handled)。メモリに無いトークンは DB で確認する (安全側)。
func (m *memStore) sessionFromHeader(c *echo.Context) (*Session, bool) {
	if !m.onCtx(c.Request().Context()) {
		return nil, false
	}
	authHeader := c.Request().Header.Get(echo.HeaderAuthorization)
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, false
	}
	token := strings.TrimPrefix(authHeader, "Bearer ")
	sum := sha256.Sum256([]byte(token))
	tokenHash := hex.EncodeToString(sum[:])
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.onCtx(c.Request().Context()) {
		return nil, false
	}
	s, ok := m.d.sessions[tokenHash]
	if !ok {
		return nil, false
	}
	u := m.d.users[s.UserID]
	if !u.active() {
		return nil, false
	}
	return &Session{UserID: s.UserID, TokenHash: tokenHash, CreatedAt: s.CreatedAt, Region: u.Region}, true
}

// viewer は loadViewerContext と同じ内容をメモリから作る。
func (d *memData) viewer(id string) (viewerContext, bool) {
	u := d.users[id]
	if !u.active() {
		return viewerContext{}, false
	}
	v := viewerContext{
		User:             u.User,
		Friends:          make(map[string]bool, len(u.friends)),
		FriendsOfFriends: map[string]bool{},
		MutualFriends:    map[string]int{},
		Blocked:          make(map[string]bool, len(u.blocking)+len(u.blockedBy)),
	}
	v.HasPostedToday = d.hasPostedToday(u)
	for b := range u.blocking {
		v.Blocked[b] = true
	}
	for b := range u.blockedBy {
		v.Blocked[b] = true
	}
	for f := range u.friends {
		v.Friends[f] = true
	}
	for f := range u.friends {
		fu := d.users[f]
		if fu == nil {
			continue
		}
		for g := range fu.friends {
			v.FriendsOfFriends[g] = true
			if g != id {
				v.MutualFriends[g]++
			}
		}
	}
	return v, true
}

func (d *memData) postsOnOpenDate(u *memUser) (*memWindow, []*memPost) {
	w := d.open[u.Region]
	if w == nil {
		return nil, nil
	}
	var res []*memPost
	for _, p := range u.posts {
		if p.window.LocalDate.Equal(w.LocalDate) {
			res = append(res, p)
		}
	}
	return w, res
}

func (d *memData) hasPostedToday(u *memUser) bool {
	_, posts := d.postsOnOpenDate(u)
	for _, p := range posts {
		if !p.deleted {
			return true
		}
	}
	return false
}

// visiblePost は getPostWithAuthor 相当 (未削除 + 作者 active)。
func (d *memData) visiblePost(id string) (*memPost, bool) {
	p := d.posts[id]
	if p == nil {
		return nil, false
	}
	return p, true
}

// ---- feeds ----

func (m *memStore) feed(c *echo.Context, input getFeedRequest) (bool, error) {
	if !m.wmOK(c.Request().Context()) {
		return false, nil
	}
	resp, ok := m.buildFeed(GetSession(c).UserID, input)
	if !ok {
		return false, nil
	}
	return true, c.JSON(http.StatusOK, resp)
}

func (m *memStore) buildFeed(viewerID string, input getFeedRequest) (getFeedResponse, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.on() {
		return getFeedResponse{}, false
	}
	d := m.d
	viewer, ok := d.viewer(viewerID)
	if !ok {
		return getFeedResponse{Items: []feedItemResponse{}}, true
	}
	vu := d.users[viewerID]
	ranked := input.Feed == "friends-of-friends"
	var beforeAt *time.Time
	beforeID := input.BeforeID
	if !ranked && input.BeforePostedAt != nil {
		t := memRoundCursor(*input.BeforePostedAt)
		beforeAt = &t
	}
	// 開いている投稿期間の投稿を (posted_at DESC, id DESC) で併合
	var lists [][]*memPost
	for _, w := range d.open {
		if len(w.posts) > 0 {
			lists = append(lists, w.posts)
		}
	}
	idx := make([]int, len(lists))
	for i := range lists {
		idx[i] = len(lists[i]) - 1
	}
	next := func() *memPost {
		best := -1
		for i := range lists {
			if idx[i] < 0 {
				continue
			}
			if best < 0 || memPostLess(lists[best][idx[best]], lists[i][idx[i]]) {
				best = i
			}
		}
		if best < 0 {
			return nil
		}
		p := lists[best][idx[best]]
		idx[best]--
		return p
	}
	matches := func(p *memPost) bool {
		if p.deleted || !p.author.active() || p.AuthorID == viewer.ID {
			return false
		}
		if !viewer.canView(p.full()) {
			return false
		}
		switch input.Feed {
		case "public":
			return p.Visibility == "public"
		case "following":
			_, ok := vu.following[p.AuthorID]
			return ok
		case "friends":
			return viewer.Friends[p.AuthorID]
		case "friends-of-friends":
			return !viewer.Friends[p.AuthorID] && viewer.FriendsOfFriends[p.AuthorID]
		}
		return false
	}
	items := make([]feedItemResponse, 0)
	if !ranked {
		for p := next(); p != nil; p = next() {
			if beforeAt != nil && !memTimeIDLess(p.PostedAt, p.ID, *beforeAt, beforeID) {
				continue
			}
			if !matches(p) {
				continue
			}
			items = append(items, feedItemResponse{createPostResponse: newPostResponse(p.full(), p.author.User)})
			if len(items) == 51 {
				break
			}
		}
	} else {
		var candidates []*memPost
		for p := next(); p != nil; p = next() {
			if matches(p) {
				candidates = append(candidates, p)
			}
		}
		scores := make(map[*memPost]int64, len(candidates))
		connections := map[string]int64{}
		for _, p := range candidates {
			if _, ok := connections[p.AuthorID]; !ok {
				var sum int64
				for f := range p.author.friends {
					sum += int64(viewer.MutualFriends[f])
				}
				connections[p.AuthorID] = sum
			}
			var engaged []*memUser
			add := func(u *memUser) {
				if !u.active() || !viewer.Friends[u.ID] {
					return
				}
				for _, e := range engaged {
					if e == u {
						return
					}
				}
				engaged = append(engaged, u)
			}
			for _, r := range p.realmojis {
				add(r.reactor)
			}
			for _, cm := range p.comments {
				add(cm.author)
			}
			scores[p] = 10*int64(len(engaged)) + 3*int64(viewer.MutualFriends[p.AuthorID]) + min(connections[p.AuthorID], 30)
		}
		sort.SliceStable(candidates, func(i, j int) bool { return scores[candidates[i]] > scores[candidates[j]] })
		var bAt time.Time
		if input.BeforePostedAt != nil {
			bAt = *input.BeforePostedAt // 元実装も Go 側で比較 (丸めなし)
		}
		for _, p := range candidates {
			score := scores[p]
			if input.BeforeScore != nil && !rankedBefore(score, p.Post, *input.BeforeScore, bAt, input.BeforeID) {
				continue
			}
			s := score
			items = append(items, feedItemResponse{createPostResponse: newPostResponse(p.full(), p.author.User), Score: &s})
			if len(items) == 51 {
				break
			}
		}
	}
	hasMore := len(items) > 50
	if hasMore {
		items = items[:50]
	}
	return getFeedResponse{Items: items, HasMore: hasMore}, true
}

// ---- posts ----

func (m *memStore) getPost(c *echo.Context, postID string) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	d := m.d
	p, ok := d.visiblePost(postID)
	if !ok {
		m.mu.RUnlock()
		return false, nil // ホット外 (終了済み期間) or 不存在 → DB
	}
	if p.deleted || !p.author.active() {
		m.mu.RUnlock()
		return true, NewAPIError(http.StatusNotFound, "post_not_found")
	}
	viewer, ok := d.viewer(GetSession(c).UserID)
	if !ok || !viewer.canView(p.full()) {
		m.mu.RUnlock()
		return true, NewAPIError(http.StatusNotFound, "post_not_found")
	}
	resp := getPostResponse(newPostResponse(p.full(), p.author.User))
	m.mu.RUnlock()
	return true, c.JSON(http.StatusOK, resp)
}

func (m *memStore) myCurrentPost(c *echo.Context) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	d := m.d
	u := d.users[GetSession(c).UserID]
	if u == nil {
		m.mu.RUnlock()
		return false, nil
	}
	_, posts := d.postsOnOpenDate(u)
	var found *memPost
	for _, p := range posts {
		if !p.deleted && (found == nil || memPostLess(found, p)) {
			found = p
		}
	}
	if found == nil {
		m.mu.RUnlock()
		return true, NewAPIError(http.StatusNotFound, "post_not_found")
	}
	if !u.active() {
		m.mu.RUnlock()
		return false, nil
	}
	resp := getMyCurrentPostResponse(newPostResponse(found.full(), u.User))
	m.mu.RUnlock()
	return true, c.JSON(http.StatusOK, resp)
}

func (m *memStore) currentPostingWindow(c *echo.Context) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	d := m.d
	u := d.users[GetSession(c).UserID]
	if u == nil {
		m.mu.RUnlock()
		return false, nil
	}
	w, posts := d.postsOnOpenDate(u)
	if w == nil {
		m.mu.RUnlock()
		return true, c.JSON(http.StatusOK, getCurrentPostingWindowResponse{})
	}
	active, deleted := 0, 0
	for _, p := range posts {
		if p.deleted {
			deleted++
		} else {
			active++
		}
	}
	resp := getCurrentPostingWindowResponse{
		Window: &currentPostingWindowResponse{
			ID: w.ID, Region: w.Region, LocalDate: w.LocalDate.Format(time.DateOnly),
			StartsAt: w.StartsAt, OnTimeDeadline: w.OnTimeDeadline,
		},
		HasPosted:     active > 0,
		CanPost:       active == 0 && deleted < 2 && !time.Now().Before(w.StartsAt),
		DeletionCount: deleted,
	}
	m.mu.RUnlock()
	return true, c.JSON(http.StatusOK, resp)
}

// ---- comments / realmojis ----

// canComment と同じ判定。p == nil (ホット外) のときは呼ばない。
func (d *memData) canComment(viewerID string, p *memPost) bool {
	if p.deleted || !p.author.active() {
		return false
	}
	viewer, ok := d.viewer(viewerID)
	if !ok {
		return false
	}
	post := p.full()
	if !viewer.canView(post) || post.WindowEndedAt != nil {
		return false
	}
	if viewer.ID == post.AuthorID {
		return viewer.HasPostedToday
	}
	if post.Visibility == "public" {
		return true
	}
	return viewer.Friends[post.AuthorID]
}

func (m *memStore) comments(c *echo.Context, input getCommentsRequest) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	d := m.d
	p := d.posts[input.PostID]
	if p == nil {
		m.mu.RUnlock()
		return false, nil
	}
	if !d.canComment(GetSession(c).UserID, p) {
		m.mu.RUnlock()
		return true, NewAPIError(http.StatusForbidden, "interaction_forbidden")
	}
	var afterAt time.Time
	if input.AfterCreatedAt != nil {
		afterAt = memRoundCursor(*input.AfterCreatedAt)
	}
	items := make([]createCommentResponse, 0)
	for _, cm := range p.comments {
		if !cm.author.active() {
			continue
		}
		if input.AfterCreatedAt != nil && !memTimeIDLess(afterAt, input.AfterID, cm.CreatedAt, cm.ID) {
			continue
		}
		items = append(items, createCommentResponse{ID: cm.ID, Author: newRelationshipUserResponse(cm.author.User), Body: cm.Body, CreatedAt: cm.CreatedAt})
		if len(items) == 51 {
			break
		}
	}
	m.mu.RUnlock()
	hasMore := len(items) > 50
	if hasMore {
		items = items[:50]
	}
	return true, c.JSON(http.StatusOK, getCommentsResponse{Items: items, HasMore: hasMore})
}

func (m *memStore) realmojis(c *echo.Context, input getRealMojisRequest) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	d := m.d
	p := d.posts[input.PostID]
	if p == nil {
		m.mu.RUnlock()
		return false, nil
	}
	allowed := false
	if !p.deleted && p.author.active() {
		if viewer, ok := d.viewer(GetSession(c).UserID); ok {
			allowed = viewer.canView(p.full())
		}
	}
	if !allowed {
		m.mu.RUnlock()
		return true, NewAPIError(http.StatusForbidden, "interaction_forbidden")
	}
	var beforeAt time.Time
	if input.BeforeCreatedAt != nil {
		beforeAt = memRoundCursor(*input.BeforeCreatedAt)
	}
	items := make([]replaceRealMojiResponse, 0)
	for i := len(p.realmojis) - 1; i >= 0; i-- {
		r := p.realmojis[i]
		if !r.reactor.active() {
			continue
		}
		if input.BeforeCreatedAt != nil && !memTimeIDLess(r.CreatedAt, r.ID, beforeAt, input.BeforeID) {
			continue
		}
		items = append(items, replaceRealMojiResponse{
			ID: r.ID, Reactor: newRelationshipUserResponse(r.reactor.User), ReactionType: r.ReactionType,
			ImageURL: signedImageURL(r.reactor.User, r.ImageID), CreatedAt: r.CreatedAt,
		})
		if len(items) == 51 {
			break
		}
	}
	m.mu.RUnlock()
	hasMore := len(items) > 50
	if hasMore {
		items = items[:50]
	}
	return true, c.JSON(http.StatusOK, getRealMojisResponse{Items: items, HasMore: hasMore})
}

// ---- users ----

func (m *memStore) getMe(c *echo.Context) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	u := m.d.users[GetSession(c).UserID]
	if !u.active() {
		m.mu.RUnlock()
		return true, NewAPIError(http.StatusNotFound, "account_not_found")
	}
	resp := newGetMeResponse(u.User)
	m.mu.RUnlock()
	return true, c.JSON(http.StatusOK, resp)
}

func (m *memStore) getUser(c *echo.Context, userID string) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	d := m.d
	viewerID := GetSession(c).UserID
	target := d.users[userID]
	if !target.active() {
		m.mu.RUnlock()
		return true, NewAPIError(http.StatusNotFound, "relationship_not_found")
	}
	if vu := d.users[viewerID]; vu != nil {
		_, b1 := vu.blocking[userID]
		_, b2 := vu.blockedBy[userID]
		if b1 || b2 {
			m.mu.RUnlock()
			return true, NewAPIError(http.StatusNotFound, "relationship_not_found")
		}
	}
	user := target.User
	isFriend := false
	if vu := d.users[viewerID]; vu != nil {
		_, isFriend = vu.friends[userID]
	}
	if user.AccountType != "certified_public" && !isFriend {
		user.Bio = ""
	}
	resp := getUserResponse(newRelationshipUserResponse(user))
	m.mu.RUnlock()
	return true, c.JSON(http.StatusOK, resp)
}

func (m *memStore) searchUsers(c *echo.Context, input searchUsersRequest) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	d := m.d
	viewerID := GetSession(c).UserID
	vu := d.users[viewerID]
	query := normalizeSearchText(input.Q)
	items := make([]relationshipUserResponse, 0)
	list := d.sorted
	start := 0
	if input.BeforeUsername != "" {
		start = sort.Search(len(list), func(i int) bool {
			u := list[i]
			if u.Username != input.BeforeUsername {
				return u.Username > input.BeforeUsername
			}
			return u.ID > input.BeforeID
		})
	}
	for _, u := range list[start:] {
		if !u.active() || u.ID == viewerID {
			continue
		}
		if !strings.Contains(u.normUsername, query) && !strings.Contains(u.normDisplay, query) {
			continue
		}
		if vu != nil {
			_, b1 := vu.blocking[u.ID]
			_, b2 := vu.blockedBy[u.ID]
			if b1 || b2 {
				continue
			}
		}
		user := u.User
		if user.AccountType != "certified_public" {
			user.Bio = ""
		}
		items = append(items, newRelationshipUserResponse(user))
		if len(items) == 51 {
			break
		}
	}
	m.mu.RUnlock()
	hasMore := len(items) > 50
	if hasMore {
		items = items[:50]
	}
	return true, c.JSON(http.StatusOK, searchUsersResponse{Items: items, HasMore: hasMore})
}

func (m *memStore) relationships(c *echo.Context, input getRelationshipsRequest) (bool, error) {
	m.mu.RLock()
	if !m.onCtx(c.Request().Context()) {
		m.mu.RUnlock()
		return false, nil
	}
	d := m.d
	u := d.users[GetSession(c).UserID]
	if u == nil {
		m.mu.RUnlock()
		return false, nil
	}
	type rec struct {
		id        string
		updatedAt time.Time
		user      *memUser
	}
	var recs []rec
	addEdges := func(edges map[string]memEdge) {
		for other, e := range edges {
			recs = append(recs, rec{id: e.ID, updatedAt: e.UpdatedAt, user: d.users[other]})
		}
	}
	switch input.Kind {
	case "friends":
		addEdges(u.friends)
	case "following":
		addEdges(u.following)
	case "followers":
		addEdges(u.followers)
	case "incoming-requests":
		for _, r := range u.incoming {
			recs = append(recs, rec{id: r.ID, updatedAt: r.UpdatedAt, user: d.users[r.RequesterID]})
		}
	case "outgoing-requests":
		for _, r := range u.outgoing {
			recs = append(recs, rec{id: r.ID, updatedAt: r.UpdatedAt, user: d.users[r.RecipientID]})
		}
	}
	sort.Slice(recs, func(i, j int) bool {
		return memTimeIDLess(recs[j].updatedAt, recs[j].id, recs[i].updatedAt, recs[i].id)
	})
	var beforeAt time.Time
	if input.BeforeUpdatedAt != nil {
		beforeAt = memRoundCursor(*input.BeforeUpdatedAt)
	}
	status := input.Kind
	if input.Kind == "incoming-requests" || input.Kind == "outgoing-requests" {
		status = "pending"
	}
	items := make([]relationshipResponse, 0)
	for _, r := range recs {
		if !r.user.active() {
			continue
		}
		if input.BeforeUpdatedAt != nil && !memTimeIDLess(r.updatedAt, r.id, beforeAt, input.BeforeID) {
			continue
		}
		user := r.user.User
		if user.AccountType != "certified_public" && input.Kind != "friends" {
			user.Bio = ""
		}
		items = append(items, relationshipResponse{RelationID: r.id, Status: status, UpdatedAt: r.updatedAt, User: newRelationshipUserResponse(user)})
		if len(items) == 51 {
			break
		}
	}
	m.mu.RUnlock()
	hasMore := len(items) > 50
	if hasMore {
		items = items[:50]
	}
	return true, c.JSON(http.StatusOK, getRelationshipsResponse{Items: items, HasMore: hasMore})
}
