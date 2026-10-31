package isureal

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// 残りの書き込み (ユーザー / プロフィール画像 / 退会 / 認定 / フォロー / ブロック / 友達解除 / 投稿の編集・削除)。

func userRow(u User) memUserRow {
	return memUserRow{
		ID: u.ID, Username: u.Username, SecretKeyHash: u.SecretKeyHash, DisplayName: u.DisplayName, Bio: u.Bio,
		ProfileImageID: u.ProfileImageID, Region: u.Region, AccountType: u.AccountType, Status: u.Status,
	}
}

func (e *storeEngine) register(c *echo.Context, input registerRequest) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		if t.d.userByUsername(input.Username) != nil {
			return nil, NewAPIError(http.StatusConflict, "username_taken")
		}
		secretKey := uuid.NewString()
		hash := sha256.Sum256([]byte(secretKey))
		userID := uuid.NewString()
		hashHex := hex.EncodeToString(hash[:])
		t.exec(`INSERT INTO users (id, username, secret_key_hash, display_name, region, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $6)`,
			userID, input.Username, hashHex, input.DisplayName, input.Region, t.now)
		t.emit("users", "I", memUserRow{ID: userID, Username: input.Username, SecretKeyHash: hashHex, DisplayName: input.DisplayName,
			Region: input.Region, AccountType: "regular", Status: "active"})
		resp := registerResponse{SecretKey: secretKey, User: newSessionUserResponse(t.d.users[userID].User)}
		return func() error { return c.JSON(http.StatusCreated, resp) }, nil
	})
}

func (e *storeEngine) updateMe(c *echo.Context, userID string, input updateMeRequest) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		u := t.d.users[userID]
		if !u.active() {
			return nil, errStoreFallback
		}
		next := u.User
		if input.DisplayName != nil {
			next.DisplayName = *input.DisplayName
		}
		if input.Bio != nil {
			next.Bio = *input.Bio
		}
		if input.Region != nil {
			next.Region = *input.Region
		}
		t.exec(`UPDATE users SET display_name = $2, bio = $3, region = $4, updated_at = $5 WHERE id = $1`, userID, next.DisplayName, next.Bio, next.Region, t.now)
		t.emit("users", "U", userRow(next))
		resp := updateMeResponse(newGetMeResponse(t.d.users[userID].User))
		return func() error { return c.JSON(http.StatusOK, resp) }, nil
	})
}

func (e *storeEngine) putProfileImage(c *echo.Context, userID string, data []byte) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		u := t.d.users[userID]
		if !u.active() {
			return nil, errStoreFallback
		}
		old := u.ProfileImageID
		imageID := uuid.NewString()
		t.exec(`INSERT INTO image_objects(id,owner_user_id,data,media_type,created_at) VALUES ($1,$2,$3,'image/jpeg',$4)`, imageID, userID, data, t.now)
		t.exec(`UPDATE users SET profile_image_id=$2,updated_at=$3 WHERE id=$1`, userID, imageID, t.now)
		if old != nil {
			t.exec(`DELETE FROM image_objects WHERE id=$1`, *old)
		}
		next := u.User
		next.ProfileImageID = strPtr(imageID)
		t.emit("users", "U", userRow(next))
		return func() error {
			replicateImage(imageID, userID, "image/jpeg", data)
			return c.NoContent(http.StatusNoContent)
		}, nil
	})
}

func (e *storeEngine) deleteProfileImage(c *echo.Context, userID string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		u := t.d.users[userID]
		if !u.active() {
			return nil, errStoreFallback
		}
		if u.ProfileImageID == nil {
			return nil, NewAPIError(http.StatusNotFound, "account_not_found")
		}
		t.exec(`UPDATE users SET profile_image_id=NULL,updated_at=$2 WHERE id=$1`, userID, t.now)
		t.exec(`DELETE FROM image_objects WHERE id=$1`, *u.ProfileImageID)
		next := u.User
		next.ProfileImageID = nil
		t.emit("users", "U", userRow(next))
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

func (e *storeEngine) deleteAccount(c *echo.Context, userID string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		u := t.d.users[userID]
		if !u.active() {
			return nil, NewAPIError(http.StatusNotFound, "account_not_found")
		}
		t.exec(`UPDATE users SET status='deleted', deleted_at=$2, updated_at=$2 WHERE id=$1`, userID, t.now)
		t.exec(`UPDATE login_sessions SET active=false, invalidated_at=$2, reason='account_deletion' WHERE user_id=$1 AND active=true`, userID, t.now)
		next := u.User
		next.Status = "deleted"
		t.emit("users", "U", userRow(next))
		for th, s := range t.d.sessions {
			if s.UserID == userID {
				t.emit("login_sessions", "U", sessionRow(th, s, false))
			}
		}
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

func (e *storeEngine) certifyUser(c *echo.Context, userID string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		u := t.d.users[userID]
		if !u.active() || u.AccountType != "regular" {
			return nil, NewAPIError(http.StatusConflict, "certification_conflict")
		}
		t.exec(`UPDATE users SET account_type='certified_public', updated_at=$2 WHERE id=$1`, userID, t.now)
		next := u.User
		next.AccountType = "certified_public"
		t.emit("users", "U", userRow(next))
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

// relPair は beginRelationshipTx と同じ前提チェック。
func relPair(d *memData, actor, target string) (*memUser, *memUser, error) {
	if actor == target || target == "" {
		return nil, nil, NewAPIError(http.StatusBadRequest, "invalid_relationship")
	}
	a, b := d.users[actor], d.users[target]
	if !a.active() || !b.active() {
		return nil, nil, NewAPIError(http.StatusNotFound, "relationship_not_found")
	}
	return a, b, nil
}

func (t *storeTx) dropFriendship(a, b string) bool {
	low, high := min(a, b), max(a, b)
	f, ok := t.d.users[low].friends[high]
	if !ok {
		return false
	}
	t.emit("friendships", "D", memFriendshipRow{ID: f.ID, UserLowID: low, UserHighID: high, UpdatedAt: f.UpdatedAt})
	return true
}

func (t *storeTx) dropFollow(follower, followee string) bool {
	f, ok := t.d.users[follower].following[followee]
	if !ok {
		return false
	}
	t.emit("follows", "D", memFollowRow{ID: f.ID, FollowerID: follower, FolloweeID: followee, UpdatedAt: f.UpdatedAt})
	return true
}

func (e *storeEngine) deleteFriendship(c *echo.Context, actor, target string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		if _, _, err := relPair(t.d, actor, target); err != nil {
			return nil, err
		}
		if _, ok := t.d.users[actor].friends[target]; !ok {
			return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		t.exec(`DELETE FROM friendships WHERE user_low_id=$1 AND user_high_id=$2`, min(actor, target), max(actor, target))
		t.dropFriendship(actor, target)
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

func (e *storeEngine) follow(c *echo.Context, actor, target string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		a, b, err := relPair(t.d, actor, target)
		if err != nil {
			return nil, err
		}
		if b.AccountType != "certified_public" {
			return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		_, b1 := a.blocking[target]
		_, b2 := a.blockedBy[target]
		_, exists := a.following[target]
		if b1 || b2 || exists {
			return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		id := uuid.NewString()
		t.exec(`INSERT INTO follows (id, follower_id, followee_id, created_at, updated_at) VALUES ($1,$2,$3,$4,$4)`, id, actor, target, t.now)
		t.emit("follows", "I", memFollowRow{ID: id, FollowerID: actor, FolloweeID: target, UpdatedAt: t.now})
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

func (e *storeEngine) unfollow(c *echo.Context, actor, target string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		if _, _, err := relPair(t.d, actor, target); err != nil {
			return nil, err
		}
		if _, ok := t.d.users[actor].following[target]; !ok {
			return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		t.exec(`DELETE FROM follows WHERE follower_id=$1 AND followee_id=$2`, actor, target)
		t.dropFollow(actor, target)
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

func (e *storeEngine) block(c *echo.Context, actor, target string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		a, _, err := relPair(t.d, actor, target)
		if err != nil {
			return nil, err
		}
		if _, ok := a.blocking[target]; ok {
			return nil, NewAPIError(http.StatusConflict, "relationship_conflict")
		}
		var pending []*memReq
		for _, r := range a.outgoing {
			if r.RecipientID == target {
				pending = append(pending, r)
			}
		}
		for _, r := range a.incoming {
			if r.RequesterID == target {
				pending = append(pending, r)
			}
		}
		t.exec(`INSERT INTO block_relations (id, blocker_id, blocked_id, created_at) VALUES ($1,$2,$3,$4)`, uuid.NewString(), actor, target, t.now)
		t.exec(`DELETE FROM friendships WHERE user_low_id=$1 AND user_high_id=$2`, min(actor, target), max(actor, target))
		t.exec(`DELETE FROM follows WHERE (follower_id=$1 AND followee_id=$2) OR (follower_id=$2 AND followee_id=$1)`, actor, target)
		t.exec(`UPDATE friend_requests SET status='cancelled', updated_at=$3 WHERE status='pending' AND ((requester_id=$1 AND recipient_id=$2) OR (requester_id=$2 AND recipient_id=$1))`, actor, target, t.now)
		t.emit("block_relations", "I", memBlockRow{BlockerID: actor, BlockedID: target})
		t.dropFriendship(actor, target)
		t.dropFollow(actor, target)
		t.dropFollow(target, actor)
		for _, r := range pending {
			req := *r
			t.emit("friend_requests", "U", memFriendRequestRow{ID: req.ID, RequesterID: req.RequesterID, RecipientID: req.RecipientID, Status: "cancelled", UpdatedAt: t.now})
		}
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

func (e *storeEngine) unblock(c *echo.Context, actor, target string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		a, _, err := relPair(t.d, actor, target)
		if err != nil {
			return nil, err
		}
		if _, ok := a.blocking[target]; !ok {
			return nil, NewAPIError(http.StatusForbidden, "relationship_action_forbidden")
		}
		t.exec(`DELETE FROM block_relations WHERE blocker_id=$1 AND blocked_id=$2`, actor, target)
		t.emit("block_relations", "D", memBlockRow{BlockerID: actor, BlockedID: target})
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}

func (e *storeEngine) updatePost(c *echo.Context, userID string, input updatePostRequest) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		p := d.posts[input.PostID]
		u := d.users[userID]
		if p == nil || !u.active() {
			return nil, errStoreFallback
		}
		if p.deleted {
			return nil, NewAPIError(http.StatusNotFound, "post_not_found")
		}
		if p.AuthorID != userID || p.window.EndedAt != nil {
			return nil, NewAPIError(http.StatusForbidden, "post_not_editable")
		}
		r := postRow(p)
		if input.Caption != nil {
			r.Caption = *input.Caption
		}
		if input.Visibility != nil {
			r.Visibility = *input.Visibility
		}
		t.exec(`UPDATE posts SET caption = $2, visibility = $3 WHERE id = $1`, p.ID, r.Caption, r.Visibility)
		t.emit("posts", "U", r)
		resp := updatePostResponse(newPostResponse(p.full(), u.User))
		return func() error { return c.JSON(http.StatusOK, resp) }, nil
	})
}

func (e *storeEngine) deletePost(c *echo.Context, userID, postID string) (bool, error) {
	return e.run(c, func(t *storeTx) (func() error, error) {
		d := t.d
		p := d.posts[postID]
		u := d.users[userID]
		if p == nil || !u.active() {
			return nil, errStoreFallback
		}
		if p.deleted {
			return nil, NewAPIError(http.StatusNotFound, "post_not_found")
		}
		if p.AuthorID != userID {
			return nil, NewAPIError(http.StatusForbidden, "post_not_editable")
		}
		deletions := 0
		for _, x := range u.posts {
			if x.deleted && x.window.LocalDate.Equal(p.window.LocalDate) {
				deletions++
			}
		}
		if deletions >= 2 {
			return nil, NewAPIError(http.StatusConflict, "post_slot_unavailable")
		}
		t.exec(`UPDATE posts SET deleted_at = $2 WHERE id = $1`, p.ID, t.now)
		r := postRow(p)
		r.DeletedAt = timePtr(t.now)
		t.emit("posts", "U", r)
		return func() error { return c.NoContent(http.StatusNoContent) }, nil
	})
}
