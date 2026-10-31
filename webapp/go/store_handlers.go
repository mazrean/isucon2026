package isureal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// 各 store ハンドラは (handled, err) を返す。handled=false なら呼び出し側は元の DB 経路で処理する
// (その前に syncPG 済み)。fn は応答を書く関数を返し、それはバリア (PG 書き戻し + peer ack) の後に呼ばれる。
func (e *storeEngine) run(c *echo.Context, fn func(t *storeTx) (func() error, error)) (bool, error) {
	name := "?"
	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
		name = name[strings.LastIndexByte(name, '.')+1:]
	}
	var respond func() error
	var after []func()
	timed := postTimingOn && name == "createPost"
	applyStart := time.Now()
	seq, ack, err := e.writePrio(func(t *storeTx) error {
		var err error
		respond, err = fn(t)
		after = t.after
		return err
	}, name != "createPost")
	if opsTraceOn {
		opsAdd(c.Request().Context(), opsApply, time.Since(applyStart))
	}
	if err == errStoreFallback {
		e.count(name, 1)
		e.syncPG()
		return false, nil
	}
	e.count(name, 0)
	if err == nil {
		// 画像の複製などはバリア待ちと並行に始める (元実装もコミット直後・応答前に開始していた)
		for _, f := range after {
			f()
		}
	}
	if timed {
		postTimingAdd("apply", time.Since(applyStart))
		if seq != nil && storeAwaitPG {
			fwdSlotYield(c.Request().Context())
			fs := time.Now()
			e.waitFlushed(seq) // commitWait と同じ待ち (先に済ませて時間を分けるだけ。commitWait の中の待ちは即座に返る)
			postTimingAdd("pgflush", time.Since(fs))
		}
		as := time.Now()
		e.commitWait(c, seq, ack)
		postTimingAdd("ack", time.Since(as))
	} else {
		e.commitWait(c, seq, ack)
	}
	if err != nil {
		return true, err
	}
	return true, respond()
}

func (d *memData) userByUsername(name string) *memUser {
	list := d.sortedUsers()
	i := sort.Search(len(list), func(i int) bool { return list[i].Username >= name })
	if i < len(list) && list[i].Username == name {
		return list[i]
	}
	return nil
}

func sessionRow(tokenHash string, s memSession, active bool) memSessionRow {
	return memSessionRow{TokenHash: tokenHash, UserID: s.UserID, Active: active, CreatedAt: s.CreatedAt}
}

// ---- auth ----

func (e *storeEngine) login(c *echo.Context, input loginRequest) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		u := d.userByUsername(input.Username)
		if u == nil {
			return nil, NewAPIError(http.StatusUnauthorized, "invalid_credentials")
		}
		hash := sha256.Sum256([]byte(input.SecretKey))
		if u.Status != "active" || u.SecretKeyHash != hex.EncodeToString(hash[:]) {
			return nil, NewAPIError(http.StatusUnauthorized, "invalid_credentials")
		}
		// 文は再実行しても同じ結果になるよう対象を ID で指定する (書き戻しの再送対策)
		var old []string
		for th, s := range t.sessionsOf(u.ID) {
			{
				t.emit("login_sessions", "U", sessionRow(th, s, false))
				old = append(old, th)
			}
		}
		if len(old) > 0 {
			t.exec(`UPDATE login_sessions SET active = false, invalidated_at = $2, reason = 'replaced' WHERE user_id = $1 AND active = true AND token_hash = ANY($3)`, u.ID, t.now, old)
		}
		token := uuid.NewString()
		sum := sha256.Sum256([]byte(token))
		tokenHash := hex.EncodeToString(sum[:])
		t.emit("login_sessions", "I", memSessionRow{TokenHash: tokenHash, UserID: u.ID, Active: true, CreatedAt: t.now})
		t.exec(`INSERT INTO login_sessions (id, user_id, token_hash, created_at) VALUES ($1, $2, $3, $4)`, uuid.NewString(), u.ID, tokenHash, t.now)
		resp := loginResponse{Token: token, User: newSessionUserResponse(u.User)}
		return func() error { return c.JSON(http.StatusOK, resp) }, nil
	})
}

func (e *storeEngine) logout(c *echo.Context, session *Session) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		if s, ok := t.d.sessions[session.TokenHash]; ok && s.UserID == session.UserID {
			t.emit("login_sessions", "U", sessionRow(session.TokenHash, s, false))
			t.exec(`UPDATE login_sessions SET active = false, invalidated_at = $3, reason = 'logout' WHERE user_id = $1 AND active = true AND token_hash = $2`, session.UserID, session.TokenHash, t.now)
		}
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

// ---- posts ----

func postRow(p *memPost) memPostRow {
	return memPostRow{
		ID: p.ID, AuthorID: p.AuthorID, PostingWindowID: p.PostingWindowID, ImageID: p.ImageID,
		Caption: p.Caption, Visibility: p.Visibility, LocationKind: p.Kind, Latitude: p.Latitude,
		Longitude: p.Longitude, Locality: p.Locality, PostedAt: p.PostedAt,
	}
}

// storePreKeepImage: ISUREAL_ASIA_LOCALIMG の画像 fsync を store の排他の外で先に済ませる
// (排他の中で fsync すると全書き込みが直列になる)。コミットしなかったら呼び出し側が asiaLocalImgDrop する。
// 応答 (201) は run の中でコミット後に返るので「fsync の後に 201」は保たれる。
func storePreKeepImage(c *echo.Context, owner string, data []byte) (string, bool) {
	ctx := c.Request().Context()
	id := newImageID(ctx)
	return id, asiaLocalImgKeep(ctx, id, owner, data) // 転送 (external) なら false
}

func storeDropUncommitted(imageID string, kept, committed bool) {
	if kept && !committed {
		asiaLocalImgDrop(imageID)
	}
}

func (e *storeEngine) createPost(c *echo.Context, userID string, input createPostRequest, location PostLocation, imageData []byte) (bool, error) {
	keepStart := time.Now()
	imageID, kept := storePreKeepImage(c, userID, imageData)
	postTimingAdd("localimg", time.Since(keepStart))
	committed := false
	defer func() { storeDropUncommitted(imageID, kept, committed) }()
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		u := d.users[userID]
		if !u.active() {
			return nil, errStoreFallback
		}
		w := d.open[u.Region]
		if w == nil {
			return nil, NewAPIError(http.StatusConflict, "post_slot_unavailable")
		}
		active, deleted := 0, 0
		for _, p := range u.posts {
			if p.window.LocalDate.Equal(w.LocalDate) {
				if p.deleted {
					deleted++
				} else {
					active++
				}
			}
		}
		postedAt := t.now
		if active != 0 || deleted >= 2 || postedAt.Before(w.StartsAt) {
			return nil, NewAPIError(http.StatusConflict, "post_slot_unavailable")
		}
		external := externalImage(c, userID)
		postID := uuid.NewString()
		if !external && !kept {
			t.execImage(imageID, `INSERT INTO image_objects (id, owner_user_id, data, media_type, created_at) VALUES ($1, $2, $3, 'image/jpeg', $4)`, imageID, userID, imageData, postedAt)
		}
		t.exec(`INSERT INTO posts (id, author_id, posting_window_id, image_id, caption, visibility,
                      location_kind, latitude, longitude, locality, posted_at, created_at)
   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`, postID, userID, w.ID, imageID, input.Caption, input.Visibility,
			location.Kind, location.Latitude, location.Longitude, location.Locality, postedAt, postedAt)
		t.emit("posts", "I", memPostRow{
			ID: postID, AuthorID: userID, PostingWindowID: w.ID, ImageID: imageID, Caption: input.Caption,
			Visibility: input.Visibility, LocationKind: location.Kind, Latitude: location.Latitude,
			Longitude: location.Longitude, Locality: location.Locality, PostedAt: postedAt,
		})
		resp := newPostResponse(d.posts[postID].full(), u.User)
		if !external {
			regions := d.pushRegionsForPost(userID, input.Visibility, "")
			t.after = append(t.after, func() { replicateImageVis(imageID, userID, "image/jpeg", imageData, regions, input.Visibility) })
		}
		committed = true
		return func() error {
			return c.JSON(http.StatusCreated, resp)
		}, nil
	})
}

// ---- comments ----

func (e *storeEngine) createComment(c *echo.Context, actor string, input createCommentRequest) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		p := d.posts[input.PostID]
		if p == nil {
			return nil, errStoreFallback
		}
		if p.deleted {
			return nil, NewAPIError(http.StatusForbidden, "interaction_forbidden")
		}
		au := d.users[actor]
		if !au.active() {
			return nil, errStoreFallback
		}
		if !d.canComment(actor, p) {
			return nil, NewAPIError(http.StatusForbidden, "interaction_forbidden")
		}
		id := uuid.NewString()
		t.exec(`INSERT INTO comments(id,post_id,author_id,body,created_at) VALUES($1,$2,$3,$4,$5)`, id, p.ID, actor, input.Body, t.now)
		t.emit("comments", "I", memCommentRow{ID: id, PostID: p.ID, AuthorID: actor, Body: input.Body, CreatedAt: t.now})
		resp := newCommentResponse(Comment{ID: id, PostID: p.ID, AuthorID: actor, Body: input.Body, CreatedAt: t.now}, au.User)
		return func() error { return c.JSON(http.StatusCreated, resp) }, nil
	})
}

func (e *storeEngine) deleteComment(c *echo.Context, actor, commentID string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		p := d.posts[e.comments[commentID]]
		if p == nil {
			return nil, errStoreFallback
		}
		var cm *memComment
		for _, x := range p.comments {
			if x.ID == commentID {
				cm = x
				break
			}
		}
		if cm == nil {
			return nil, errStoreFallback
		}
		if actor != cm.author.ID && actor != p.AuthorID {
			return nil, NewAPIError(http.StatusForbidden, "interaction_forbidden")
		}
		t.exec(`UPDATE comments SET deleted_at=$2 WHERE id=$1`, commentID, t.now)
		t.emit("comments", "U", memCommentRow{ID: cm.ID, PostID: p.ID, AuthorID: cm.author.ID, Body: cm.Body, CreatedAt: cm.CreatedAt, DeletedAt: timePtr(t.now)})
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

// ---- realmojis ----

func realmojiRow(x *memRealMoji) memRealMojiRow {
	return memRealMojiRow{ID: x.ID, PostID: x.PostID, ReactorID: x.reactor.ID, ImageID: x.ImageID, ReactionType: x.ReactionType, CreatedAt: x.CreatedAt}
}

func (e *storeEngine) replaceRealMoji(c *echo.Context, actor string, input replaceRealMojiRequest, data []byte) (bool, error) {
	imageID, kept := storePreKeepImage(c, actor, data)
	committed := false
	defer func() { storeDropUncommitted(imageID, kept, committed) }()
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		p := d.posts[input.PostID]
		if p == nil {
			return nil, errStoreFallback
		}
		if p.deleted || actor == p.AuthorID {
			return nil, NewAPIError(http.StatusForbidden, "interaction_forbidden")
		}
		au := d.users[actor]
		if !au.active() || !p.author.active() {
			return nil, NewAPIError(http.StatusNotFound, "relationship_not_found")
		}
		viewer, ok := d.viewer(actor)
		if !ok || !viewer.canView(p.full()) {
			return nil, NewAPIError(http.StatusForbidden, "interaction_forbidden")
		}
		var old []*memRealMoji
		for _, x := range p.realmojis {
			if x.reactor.ID == actor {
				old = append(old, x)
			}
		}
		var oldIDs []string
		for _, x := range old {
			r := realmojiRow(x)
			r.ReplacedAt = timePtr(t.now)
			t.emit("realmojis", "U", r)
			oldIDs = append(oldIDs, x.ID)
		}
		if len(oldIDs) > 0 {
			t.exec(`UPDATE realmojis SET replaced_at=$3 WHERE post_id=$1 AND reactor_id=$2 AND replaced_at IS NULL AND deleted_at IS NULL AND id = ANY($4)`, p.ID, actor, t.now, oldIDs)
		}
		external := externalImage(c, actor)
		if !external && !kept {
			t.execImage(imageID, `INSERT INTO image_objects(id,owner_user_id,data,media_type,created_at) VALUES($1,$2,$3,'image/jpeg',$4)`, imageID, actor, data, t.now)
		}
		id := uuid.NewString()
		t.exec(`INSERT INTO realmojis(id,post_id,reactor_id,image_id,reaction_type,created_at) VALUES($1,$2,$3,$4,$5,$6)`, id, p.ID, actor, imageID, input.ReactionType, t.now)
		t.emit("realmojis", "I", memRealMojiRow{ID: id, PostID: p.ID, ReactorID: actor, ImageID: imageID, ReactionType: input.ReactionType, CreatedAt: t.now})
		resp := newRealMojiResponse(RealMoji{ID: id, PostID: p.ID, ReactorID: actor, ImageID: imageID, ReactionType: input.ReactionType, CreatedAt: t.now}, au.User)
		if !external {
			regions := t.d.pushRegionsForPost(p.AuthorID, p.Visibility, actor)
			t.after = append(t.after, func() { replicateImageTo(imageID, actor, "image/jpeg", data, regions) })
		}
		committed = true
		return func() error {
			return c.JSON(http.StatusCreated, resp)
		}, nil
	})
}

func (e *storeEngine) deleteRealMoji(c *echo.Context, actor, id string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		p := d.posts[e.realmojis[id]]
		if p == nil {
			return nil, errStoreFallback
		}
		var x *memRealMoji
		for _, r := range p.realmojis {
			if r.ID == id {
				x = r
				break
			}
		}
		if x == nil {
			return nil, errStoreFallback
		}
		if actor != x.reactor.ID && actor != p.AuthorID {
			return nil, NewAPIError(http.StatusForbidden, "interaction_forbidden")
		}
		t.exec(`UPDATE realmojis SET deleted_at=$2 WHERE id=$1`, id, t.now)
		r := realmojiRow(x)
		r.DeletedAt = timePtr(t.now)
		t.emit("realmojis", "U", r)
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

// ---- friend requests ----

func (t *storeTx) addFriendship(requestID, a, b string) error {
	low, high := min(a, b), max(a, b)
	if _, ok := t.d.users[low].friends[high]; ok {
		// 元実装は UNIQUE 違反で 500
		return fmt.Errorf("store: friendship already exists")
	}
	id := uuid.NewString()
	t.exec(`INSERT INTO friendships (id, user_low_id, user_high_id, source_request_id, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$5)`, id, low, high, requestID, t.now)
	t.emit("friendships", "I", memFriendshipRow{ID: id, UserLowID: low, UserHighID: high, UpdatedAt: t.now})
	return nil
}

func (t *storeTx) setRequestStatus(r *memReq, status string) {
	t.exec(`UPDATE friend_requests SET status=$2, updated_at=$3 WHERE id=$1`, r.ID, status, t.now)
	t.emit("friend_requests", "U", memFriendRequestRow{ID: r.ID, RequesterID: r.RequesterID, RecipientID: r.RecipientID, Status: status, UpdatedAt: t.now})
}

func (e *storeEngine) createFriendRequest(c *echo.Context, actor, recipient string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		if actor == recipient || recipient == "" {
			return nil, NewAPIError(http.StatusBadRequest, "invalid_relationship")
		}
		au, ru := d.users[actor], d.users[recipient]
		if !au.active() || !ru.active() {
			return nil, NewAPIError(http.StatusNotFound, "relationship_not_found")
		}
		if _, ok := au.blocking[recipient]; ok {
			return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		if _, ok := au.blockedBy[recipient]; ok {
			return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		if _, ok := au.friends[recipient]; ok {
			return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		for _, r := range au.outgoing {
			if r.RecipientID == recipient {
				return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
			}
		}
		var resp createFriendRequestResponse
		var incoming *memReq
		for _, r := range au.incoming {
			if r.RequesterID == recipient {
				incoming = r
				break
			}
		}
		if incoming != nil {
			req := *incoming
			if err := t.addFriendship(req.ID, req.RequesterID, req.RecipientID); err != nil {
				return nil, err
			}
			t.setRequestStatus(&req, "accepted")
			resp = createFriendRequestResponse{RelationID: req.ID, Status: "accepted", UpdatedAt: t.now}
		} else {
			id := uuid.NewString()
			t.exec(`INSERT INTO friend_requests (id, requester_id, recipient_id, status, created_at, updated_at) VALUES ($1,$2,$3,'pending',$4,$4)`, id, actor, recipient, t.now)
			t.emit("friend_requests", "I", memFriendRequestRow{ID: id, RequesterID: actor, RecipientID: recipient, Status: "pending", UpdatedAt: t.now})
			resp = createFriendRequestResponse{RelationID: id, Status: "pending", UpdatedAt: t.now}
		}
		return func() error { return c.JSON(http.StatusCreated, resp) }, nil
	})
}

func (e *storeEngine) transitionFriendRequest(c *echo.Context, actor, requestID, status string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		r := e.requests[requestID]
		if r == nil || d.users[r.RequesterID].outgoing[requestID] != r {
			f, ok := e.finalized[requestID]
			if !ok {
				return nil, errStoreFallback // 不明 (ロード前に確定済み等) → DB 経路
			}
			// 元実装: 権限 → 403、当事者が退会済み → 404、pending でない → 403
			if (status == "cancelled" && actor != f.RequesterID) || (status != "cancelled" && actor != f.RecipientID) {
				return nil, NewAPIError(http.StatusForbidden, "relationship_action_forbidden")
			}
			if !d.users[f.RequesterID].active() || !d.users[f.RecipientID].active() {
				return nil, NewAPIError(http.StatusNotFound, "relationship_not_found")
			}
			return nil, NewAPIError(http.StatusForbidden, "relationship_action_forbidden")
		}
		if (status == "cancelled" && actor != r.RequesterID) || (status != "cancelled" && actor != r.RecipientID) {
			return nil, NewAPIError(http.StatusForbidden, "relationship_action_forbidden")
		}
		requester, recipient := d.users[r.RequesterID], d.users[r.RecipientID]
		if !requester.active() || !recipient.active() {
			return nil, NewAPIError(http.StatusNotFound, "relationship_not_found")
		}
		req := *r
		if status == "accepted" {
			_, b1 := requester.blocking[r.RecipientID]
			_, b2 := requester.blockedBy[r.RecipientID]
			if b1 || b2 {
				return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
			}
			if err := t.addFriendship(req.ID, req.RequesterID, req.RecipientID); err != nil {
				return nil, err
			}
		}
		t.setRequestStatus(&req, status)
		resp := transitionFriendRequestResponse{RelationID: req.ID, Status: status, UpdatedAt: t.now}
		return func() error { return c.JSON(http.StatusOK, resp) }, nil
	})
}

// ---- posting window (admin) ----

func (e *storeEngine) openPostingWindow(c *echo.Context, region string, localDate time.Time, seconds int64) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		for _, w := range d.windows {
			if w.Region == region && !w.LocalDate.Before(localDate) {
				return nil, NewAPIError(http.StatusConflict, "posting_window_conflict")
			}
		}
		startsAt := t.now
		if w := d.open[region]; w != nil {
			t.emit("posting_windows", "U", memWindowRow{
				ID: w.ID, Region: w.Region, LocalDate: w.LocalDate.Format(time.DateOnly), StartsAt: w.StartsAt,
				OnTimeDeadline: w.OnTimeDeadline, EndedAt: timePtr(startsAt),
			})
			t.exec(`UPDATE posting_windows SET ended_at = $2 WHERE id = $1 AND ended_at IS NULL`, w.ID, startsAt)
		}
		id := uuid.NewString()
		deadline := startsAt.Add(time.Duration(seconds) * time.Second)
		ld := localDate.Format(time.DateOnly)
		t.exec(`INSERT INTO posting_windows (id, region, local_date, starts_at, on_time_deadline, created_at) VALUES ($1, $2, $3::date, $4, $5, $4)`, id, region, ld, startsAt, deadline)
		t.emit("posting_windows", "I", memWindowRow{ID: id, Region: region, LocalDate: ld, StartsAt: startsAt, OnTimeDeadline: deadline})
		resp := openPostingWindowResponse{ID: id, Region: region, LocalDate: ld, StartsAt: startsAt, OnTimeDeadline: deadline}
		return func() error { return c.JSON(http.StatusCreated, resp) }, nil
	})
}

// 統計 (handled / fallback)
func (e *storeEngine) count(name string, i int) {
	v, _ := e.stats.LoadOrStore(name, new([2]atomic.Int64))
	v.(*[2]atomic.Int64)[i].Add(1)
}

func (e *storeEngine) statsString() string {
	var b strings.Builder
	e.stats.Range(func(k, v any) bool {
		x := v.(*[2]atomic.Int64)
		fmt.Fprintf(&b, "%s=%d/%d ", k, x[0].Load(), x[1].Load())
		return true
	})
	return b.String()
}
