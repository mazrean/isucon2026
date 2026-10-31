package isureal

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// インメモリ読みモデル (ISUREAL_MEM=1)。
// 保持するもの: 全ユーザー, 有効セッション, 友達/フォロー/ブロック/保留中友達申請, 全投稿期間,
// 「ホット」な投稿 (読み込み時点で開いている投稿期間の最小 local_date 以降の期間の投稿 + 以後の新規投稿)
// とその有効なコメント/RealMoji。ホット外の投稿は終了済み期間のものだけなので、作者本人の閲覧などは DB 経路へフォールバックする。
// Memories / export / 通知は DB 経路のまま。

var memEnabled = os.Getenv("ISUREAL_MEM") == "1"

type memEdge struct {
	ID        string
	UpdatedAt time.Time
}

type memReq struct {
	ID          string
	RequesterID string
	RecipientID string
	UpdatedAt   time.Time
}

type memUser struct {
	User
	normUsername string
	normDisplay  string
	friends      map[string]memEdge // other user id -> friendship
	following    map[string]memEdge // followee id -> follow
	followers    map[string]memEdge // follower id -> follow
	blocking     map[string]struct{}
	blockedBy    map[string]struct{}
	incoming     map[string]*memReq // request id
	outgoing     map[string]*memReq
	posts        []*memPost // hot posts by this author (deleted included)
}

func (u *memUser) active() bool { return u != nil && u.Status == "active" }

type memWindow struct {
	PostingWindow
	posts []*memPost // sorted asc by (PostedAt, ID)
}

type memPost struct {
	Post
	deleted   bool
	author    *memUser
	window    *memWindow
	comments  []*memComment  // active, asc by (CreatedAt, ID)
	realmojis []*memRealMoji // active, asc by (CreatedAt, ID)
}

// full は投稿期間の情報を埋めた Post を返す。
func (p *memPost) full() Post {
	post := p.Post
	post.OnTimeDeadline = p.window.OnTimeDeadline
	post.WindowEndedAt = p.window.EndedAt
	return post
}

type memComment struct {
	ID        string
	PostID    string
	author    *memUser
	Body      string
	CreatedAt time.Time
}

type memRealMoji struct {
	ID           string
	PostID       string
	reactor      *memUser
	ImageID      string
	ReactionType string
	CreatedAt    time.Time
}

type memSession struct {
	UserID    string
	CreatedAt time.Time
}

type memData struct {
	users       map[string]*memUser
	sessions    map[string]memSession // token hash
	windows     map[string]*memWindow
	open        map[string]*memWindow // region -> open window
	posts       map[string]*memPost
	sorted      []*memUser // by (username, id)
	sortedDirty bool
}

func newMemData() *memData {
	return &memData{
		users:    map[string]*memUser{},
		sessions: map[string]memSession{},
		windows:  map[string]*memWindow{},
		open:     map[string]*memWindow{},
		posts:    map[string]*memPost{},
	}
}

type memStore struct {
	mu    sync.RWMutex
	d     *memData
	ready atomic.Bool

	useLease   bool         // peer のみ
	leaseUntil atomic.Int64 // unix nano
}

// on は、このリクエストをメモリから返してよいか (フラグ ON かつロード完了、peer は lease 内)。
func (m *memStore) on() bool {
	if m == nil || !m.ready.Load() {
		return false
	}
	return !m.useLease || time.Now().UnixNano() < m.leaseUntil.Load()
}

// extendLeaseUntil は lease を until (pump の送信時刻 + memLease) まで延ばす。今 + memLease を超えず、縮めない。
func (m *memStore) extendLeaseUntil(until int64) {
	if until <= 0 {
		return
	}
	if lim := time.Now().Add(memLease).UnixNano(); until > lim {
		until = lim
	}
	if until > m.leaseUntil.Load() {
		m.leaseUntil.Store(until)
	}
}

func (m *memStore) extendLease() {
	m.leaseUntil.Store(time.Now().Add(memLease).UnixNano())
}

// ---- row types (shared by primary load, changelog jsonb and peer dump) ----

type memUserRow struct {
	ID             string  `db:"id" json:"id"`
	Username       string  `db:"username" json:"username"`
	SecretKeyHash  string  `db:"secret_key_hash" json:"secret_key_hash"`
	DisplayName    string  `db:"display_name" json:"display_name"`
	Bio            string  `db:"bio" json:"bio"`
	ProfileImageID *string `db:"profile_image_id" json:"profile_image_id"`
	Region         string  `db:"region" json:"region"`
	AccountType    string  `db:"account_type" json:"account_type"`
	Status         string  `db:"status" json:"status"`
}

type memSessionRow struct {
	TokenHash string    `db:"token_hash" json:"token_hash"`
	UserID    string    `db:"user_id" json:"user_id"`
	Active    bool      `db:"active" json:"active"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

type memFriendshipRow struct {
	ID         string    `db:"id" json:"id"`
	UserLowID  string    `db:"user_low_id" json:"user_low_id"`
	UserHighID string    `db:"user_high_id" json:"user_high_id"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

type memFollowRow struct {
	ID         string    `db:"id" json:"id"`
	FollowerID string    `db:"follower_id" json:"follower_id"`
	FolloweeID string    `db:"followee_id" json:"followee_id"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

type memBlockRow struct {
	BlockerID string `db:"blocker_id" json:"blocker_id"`
	BlockedID string `db:"blocked_id" json:"blocked_id"`
}

type memFriendRequestRow struct {
	ID          string    `db:"id" json:"id"`
	RequesterID string    `db:"requester_id" json:"requester_id"`
	RecipientID string    `db:"recipient_id" json:"recipient_id"`
	Status      string    `db:"status" json:"status"`
	UpdatedAt   time.Time `db:"updated_at" json:"updated_at"`
}

type memWindowRow struct {
	ID             string     `db:"id" json:"id"`
	Region         string     `db:"region" json:"region"`
	LocalDate      string     `db:"local_date" json:"local_date"` // YYYY-MM-DD
	StartsAt       time.Time  `db:"starts_at" json:"starts_at"`
	OnTimeDeadline time.Time  `db:"on_time_deadline" json:"on_time_deadline"`
	EndedAt        *time.Time `db:"ended_at" json:"ended_at"`
}

type memPostRow struct {
	ID              string     `db:"id" json:"id"`
	AuthorID        string     `db:"author_id" json:"author_id"`
	PostingWindowID string     `db:"posting_window_id" json:"posting_window_id"`
	ImageID         string     `db:"image_id" json:"image_id"`
	Caption         string     `db:"caption" json:"caption"`
	Visibility      string     `db:"visibility" json:"visibility"`
	LocationKind    string     `db:"location_kind" json:"location_kind"`
	Latitude        *float64   `db:"latitude" json:"latitude"`
	Longitude       *float64   `db:"longitude" json:"longitude"`
	Locality        *string    `db:"locality" json:"locality"`
	PostedAt        time.Time  `db:"posted_at" json:"posted_at"`
	DeletedAt       *time.Time `db:"deleted_at" json:"deleted_at"`
}

type memCommentRow struct {
	ID        string     `db:"id" json:"id"`
	PostID    string     `db:"post_id" json:"post_id"`
	AuthorID  string     `db:"author_id" json:"author_id"`
	Body      string     `db:"body" json:"body"`
	CreatedAt time.Time  `db:"created_at" json:"created_at"`
	DeletedAt *time.Time `db:"deleted_at" json:"deleted_at"`
}

type memRealMojiRow struct {
	ID           string     `db:"id" json:"id"`
	PostID       string     `db:"post_id" json:"post_id"`
	ReactorID    string     `db:"reactor_id" json:"reactor_id"`
	ImageID      string     `db:"image_id" json:"image_id"`
	ReactionType string     `db:"reaction_type" json:"reaction_type"`
	CreatedAt    time.Time  `db:"created_at" json:"created_at"`
	ReplacedAt   *time.Time `db:"replaced_at" json:"replaced_at"`
	DeletedAt    *time.Time `db:"deleted_at" json:"deleted_at"`
}

// memChange は changelog の 1 行 (トリガーが記録した行変更)。
type memChange struct {
	Seq int64           `db:"seq" json:"s"`
	Tbl string          `db:"tbl" json:"t"`
	Op  string          `db:"op" json:"o"`
	Row json.RawMessage `db:"row" json:"r"`
}

func memLocal(t time.Time) time.Time { return t.In(time.Local) }

func memLocalPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := memLocal(*t)
	return &v
}

// ---- apply (caller holds write lock) ----

func (d *memData) user(id string) *memUser {
	u := d.users[id]
	if u == nil {
		u = &memUser{User: User{ID: id, Status: "deleted"}}
		d.users[id] = u
		d.sortedDirty = true
	}
	return u
}

func (d *memData) applyUser(r memUserRow) {
	u := d.user(r.ID)
	if u.Username != r.Username {
		d.sortedDirty = true
	}
	u.User = User{
		ID: r.ID, Username: r.Username, SecretKeyHash: r.SecretKeyHash, DisplayName: r.DisplayName,
		Bio: r.Bio, ProfileImageID: r.ProfileImageID, Region: r.Region, AccountType: r.AccountType, Status: r.Status,
	}
	_ = u.User.setImageKey()
	u.normUsername = normalizeSearchText(r.Username)
	u.normDisplay = normalizeSearchText(r.DisplayName)
}

func (d *memData) applySession(del bool, r memSessionRow) {
	if del || !r.Active {
		delete(d.sessions, r.TokenHash)
		return
	}
	d.sessions[r.TokenHash] = memSession{UserID: r.UserID, CreatedAt: memLocal(r.CreatedAt)}
}

func setEdge(m *map[string]memEdge, key string, e memEdge) {
	if *m == nil {
		*m = map[string]memEdge{}
	}
	(*m)[key] = e
}

func setFlag(m *map[string]struct{}, key string) {
	if *m == nil {
		*m = map[string]struct{}{}
	}
	(*m)[key] = struct{}{}
}

func (d *memData) applyFriendship(del bool, r memFriendshipRow) {
	low, high := d.user(r.UserLowID), d.user(r.UserHighID)
	if del {
		delete(low.friends, r.UserHighID)
		delete(high.friends, r.UserLowID)
		return
	}
	e := memEdge{ID: r.ID, UpdatedAt: memLocal(r.UpdatedAt)}
	setEdge(&low.friends, r.UserHighID, e)
	setEdge(&high.friends, r.UserLowID, e)
}

func (d *memData) applyFollow(del bool, r memFollowRow) {
	follower, followee := d.user(r.FollowerID), d.user(r.FolloweeID)
	if del {
		delete(follower.following, r.FolloweeID)
		delete(followee.followers, r.FollowerID)
		return
	}
	e := memEdge{ID: r.ID, UpdatedAt: memLocal(r.UpdatedAt)}
	setEdge(&follower.following, r.FolloweeID, e)
	setEdge(&followee.followers, r.FollowerID, e)
}

func (d *memData) applyBlock(del bool, r memBlockRow) {
	blocker, blocked := d.user(r.BlockerID), d.user(r.BlockedID)
	if del {
		delete(blocker.blocking, r.BlockedID)
		delete(blocked.blockedBy, r.BlockerID)
		return
	}
	setFlag(&blocker.blocking, r.BlockedID)
	setFlag(&blocked.blockedBy, r.BlockerID)
}

func (d *memData) applyFriendRequest(del bool, r memFriendRequestRow) {
	requester, recipient := d.user(r.RequesterID), d.user(r.RecipientID)
	if del || r.Status != "pending" {
		delete(requester.outgoing, r.ID)
		delete(recipient.incoming, r.ID)
		return
	}
	req := &memReq{ID: r.ID, RequesterID: r.RequesterID, RecipientID: r.RecipientID, UpdatedAt: memLocal(r.UpdatedAt)}
	if requester.outgoing == nil {
		requester.outgoing = map[string]*memReq{}
	}
	if recipient.incoming == nil {
		recipient.incoming = map[string]*memReq{}
	}
	requester.outgoing[r.ID] = req
	recipient.incoming[r.ID] = req
}

func (d *memData) applyWindow(r memWindowRow) error {
	localDate, err := time.Parse(time.DateOnly, r.LocalDate)
	if err != nil {
		// sqlx 経由で date を string に読むと RFC3339 になる
		t, err2 := time.Parse(time.RFC3339Nano, r.LocalDate)
		if err2 != nil {
			return fmt.Errorf("mem: bad local_date %q: %w", r.LocalDate, err)
		}
		localDate = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	}
	w := d.windows[r.ID]
	if w == nil {
		w = &memWindow{}
		d.windows[r.ID] = w
	}
	w.PostingWindow = PostingWindow{
		ID: r.ID, Region: r.Region, LocalDate: localDate, StartsAt: memLocal(r.StartsAt),
		OnTimeDeadline: memLocal(r.OnTimeDeadline), EndedAt: memLocalPtr(r.EndedAt),
	}
	if w.EndedAt == nil {
		d.open[r.Region] = w
	} else if d.open[r.Region] == w {
		delete(d.open, r.Region)
	}
	return nil
}

func memPostLess(a, b *memPost) bool {
	if !a.PostedAt.Equal(b.PostedAt) {
		return a.PostedAt.Before(b.PostedAt)
	}
	return a.ID < b.ID
}

func insertPostSorted(list []*memPost, p *memPost) []*memPost {
	i := sort.Search(len(list), func(i int) bool { return memPostLess(p, list[i]) })
	list = append(list, nil)
	copy(list[i+1:], list[i:])
	list[i] = p
	return list
}

func (d *memData) applyPost(r memPostRow) {
	post := Post{
		ID: r.ID, AuthorID: r.AuthorID, PostingWindowID: r.PostingWindowID, ImageID: r.ImageID,
		Caption: r.Caption, Visibility: r.Visibility,
		PostLocation: PostLocation{Kind: r.LocationKind, Latitude: r.Latitude, Longitude: r.Longitude, Locality: r.Locality},
		PostedAt:     memLocal(r.PostedAt),
	}
	if p := d.posts[r.ID]; p != nil {
		p.Post = post
		p.deleted = r.DeletedAt != nil
		return
	}
	w := d.windows[r.PostingWindowID]
	if w == nil {
		// window row not applied yet: keep the post on a placeholder that
		// applyWindow fills in later, instead of dropping it for good
		w = &memWindow{}
		w.ID = r.PostingWindowID
		d.windows[r.PostingWindowID] = w
		slog.Warn("mem: post before its window", "post", r.ID, "window", r.PostingWindowID)
	}
	author := d.user(r.AuthorID)
	p := &memPost{Post: post, deleted: r.DeletedAt != nil, author: author, window: w}
	d.posts[r.ID] = p
	w.posts = insertPostSorted(w.posts, p)
	author.posts = append(author.posts, p)
}

func memTimeIDLess(at time.Time, aid string, bt time.Time, bid string) bool {
	if !at.Equal(bt) {
		return at.Before(bt)
	}
	return aid < bid
}

func (d *memData) applyComment(r memCommentRow) {
	p := d.posts[r.PostID]
	if p == nil {
		return
	}
	for i, c := range p.comments {
		if c.ID == r.ID {
			p.comments = append(p.comments[:i:i], p.comments[i+1:]...)
			break
		}
	}
	if r.DeletedAt != nil {
		return
	}
	c := &memComment{ID: r.ID, PostID: r.PostID, author: d.user(r.AuthorID), Body: r.Body, CreatedAt: memLocal(r.CreatedAt)}
	list := p.comments
	i := sort.Search(len(list), func(i int) bool { return memTimeIDLess(c.CreatedAt, c.ID, list[i].CreatedAt, list[i].ID) })
	list = append(list, nil)
	copy(list[i+1:], list[i:])
	list[i] = c
	p.comments = list
}

func (d *memData) applyRealMoji(r memRealMojiRow) {
	p := d.posts[r.PostID]
	if p == nil {
		return
	}
	for i, x := range p.realmojis {
		if x.ID == r.ID {
			p.realmojis = append(p.realmojis[:i:i], p.realmojis[i+1:]...)
			break
		}
	}
	if r.DeletedAt != nil || r.ReplacedAt != nil {
		return
	}
	x := &memRealMoji{ID: r.ID, PostID: r.PostID, reactor: d.user(r.ReactorID), ImageID: r.ImageID, ReactionType: r.ReactionType, CreatedAt: memLocal(r.CreatedAt)}
	list := p.realmojis
	i := sort.Search(len(list), func(i int) bool { return memTimeIDLess(x.CreatedAt, x.ID, list[i].CreatedAt, list[i].ID) })
	list = append(list, nil)
	copy(list[i+1:], list[i:])
	list[i] = x
	p.realmojis = list
}

// applyChange は changelog 1 行を反映する。'_' テーブルは制御用 (呼び出し側で処理)。
func (d *memData) applyChange(ch memChange) error {
	del := ch.Op == "D"
	var err error
	switch ch.Tbl {
	case "users":
		var r memUserRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			d.applyUser(r)
		}
	case "login_sessions":
		var r memSessionRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			d.applySession(del, r)
		}
	case "friendships":
		var r memFriendshipRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			d.applyFriendship(del, r)
		}
	case "follows":
		var r memFollowRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			d.applyFollow(del, r)
		}
	case "block_relations":
		var r memBlockRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			d.applyBlock(del, r)
		}
	case "friend_requests":
		var r memFriendRequestRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			d.applyFriendRequest(del, r)
		}
	case "posting_windows":
		var r memWindowRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			err = d.applyWindow(r)
		}
	case "posts":
		var r memPostRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			if del {
				now := time.Now()
				r.DeletedAt = &now
			}
			d.applyPost(r)
		}
	case "comments":
		var r memCommentRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			if del {
				now := time.Now()
				r.DeletedAt = &now
			}
			d.applyComment(r)
		}
	case "realmojis":
		var r memRealMojiRow
		if err = json.Unmarshal(ch.Row, &r); err == nil {
			if del {
				now := time.Now()
				r.DeletedAt = &now
			}
			d.applyRealMoji(r)
		}
	}
	if err != nil {
		return fmt.Errorf("mem: apply %s/%s: %w", ch.Tbl, ch.Op, err)
	}
	return nil
}

func (d *memData) sortedUsers() []*memUser {
	if d.sortedDirty || d.sorted == nil {
		list := make([]*memUser, 0, len(d.users))
		for _, u := range d.users {
			list = append(list, u)
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].Username != list[j].Username {
				return list[i].Username < list[j].Username
			}
			return list[i].ID < list[j].ID
		})
		d.sorted = list
		d.sortedDirty = false
	}
	return d.sorted
}
