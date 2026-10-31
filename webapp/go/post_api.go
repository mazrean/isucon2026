package isureal

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

type createPostRequest struct {
	Image        *multipart.FileHeader `form:"image"`
	Caption      string                `form:"caption"`
	Visibility   string                `form:"visibility"`
	LocationKind string                `form:"location_kind"`
	Latitude     *float64              `form:"latitude"`
	Longitude    *float64              `form:"longitude"`
	Locality     *string               `form:"locality"`
}

type postAuthorResponse struct {
	ID              string  `json:"id"`
	Username        string  `json:"username"`
	DisplayName     string  `json:"displayName"`
	ProfileImageURL *string `json:"profileImageUrl"`
	Bio             string  `json:"bio"`
	AccountType     string  `json:"accountType"`
}

type createPostResponse struct {
	ID              string             `json:"id"`
	Author          postAuthorResponse `json:"author"`
	PostingWindowID string             `json:"postingWindowId"`
	ImageURL        string             `json:"imageUrl"`
	Caption         string             `json:"caption"`
	Visibility      string             `json:"visibility"`
	Location        PostLocation       `json:"location"`
	PostedAt        time.Time          `json:"postedAt"`
	OnTime          bool               `json:"onTime"`
	LateSeconds     int64              `json:"lateSeconds"`
}

var postWindowFast = os.Getenv("ISUREAL_PW_FAST") != "0"

func createPostHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input createPostRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		if input.Image == nil || !validUserText(input.Caption, 0, 1000) {
			return NewAPIError(http.StatusBadRequest, "invalid_post")
		}
		switch input.Visibility {
		case "friends", "friends_of_friends", "public":
		default:
			return NewAPIError(http.StatusBadRequest, "invalid_post")
		}
		location := PostLocation{
			Kind: input.LocationKind, Latitude: input.Latitude,
			Longitude: input.Longitude, Locality: input.Locality,
		}
		if !location.valid() {
			return NewAPIError(http.StatusBadRequest, "invalid_post")
		}
		imageFile, err := input.Image.Open()
		if err != nil {
			return err
		}
		defer imageFile.Close()
		imageData, err := readAllSized(imageFile, input.Image.Size)
		if err != nil {
			return err
		}
		imgW, imgH, imgOK := checkJPEG(c.Request().Context(), imageData)
		if !imgOK {
			return NewAPIError(http.StatusBadRequest, "invalid_post")
		}
		if imgW > 4096 || imgH > 4096 {
			return NewAPIError(http.StatusBadRequest, "invalid_post")
		}

		ctx := c.Request().Context()
		userID := GetSession(c).UserID
		if h.Store != nil {
			if ok, err := h.Store.createPost(c, userID, input, location, imageData); ok {
				return err
			}
		}
		// ISUREAL_PW_FAST (既定 on): 361KB の image_objects INSERT を窓の FOR SHARE より前に出す。
		// 窓の行ロックを持つ時間が短くなり、admin の open (UPDATE ended_at) が投稿の tx を待つ時間が減る。
		// h.Img があれば tx の外 (画像専用プールの autocommit) で先に INSERT する: TOAST の extension lock 待ちの間
		// メインのプールの接続も users の行ロックも握らない。コミットしなかったら後で行を消す (参照の無い画像は
		// queryImageValidity が見つけないので、消すまでの間も外からは見えない)。
		imageID := newImageID(ctx)
		imageInserted := false
		asiaKept, committed := false, false
		imgOutside, commitTried := false, false
		defer func() {
			if asiaKept && !committed {
				asiaLocalImgDrop(imageID)
			}
			if imgOutside && !commitTried { // Commit がエラーでも確定している可能性があるので消さない
				go dropOrphanImage(h.Img, imageID)
			}
		}()
		preInsert := postWindowFast && localImgFromCtx(ctx) == nil && !asiaLocalImgOn
		if preInsert && h.Img != nil {
			if _, err := h.Img.ExecContext(ctx, `INSERT INTO image_objects (id, owner_user_id, data, media_type, created_at) VALUES ($1, $2, $3, 'image/jpeg', $4)`, imageID, userID, imageData, time.Now().UTC()); err != nil {
				return err
			}
			imageInserted, imgOutside = true, true
		}
		// ISUREAL_ASIA_LOCALIMG: 画像の fsync も tx の外で先に済ませる (接続・users の行ロック・窓のロックを握ったまま
		// ディスクを待たない)。コミットしなければ上の defer が asiaLocalImgDrop する。失敗したら従来どおり tx 内で INSERT。
		if asiaLocalImgOn && localImgFromCtx(ctx) == nil {
			asiaKept = asiaLocalImgKeep(ctx, imageID, userID, imageData)
		}
		// ISUREAL_POST_BATCH=1: 画像が tx の外で片付いている (DB に先に入れた / asia1 のディスクに置いた / 転送) ときは
		// tx を 1 往復の batch で流す (post_batch.go に正しさの論証)。それ以外は従来どおり。
		// ISUREAL_GP_FAIR: GP 中は DB 区間に入る順を締め切り順にする (gp_fair.go)。
		gpRelease := gpFairEnter(h, c)
		defer gpRelease()
		if postBatchOn && (imageInserted || asiaKept || localImgFromCtx(ctx) != nil) {
			user, post, ok, err := createPostBatch(ctx, h.DB, userID, uuid.NewString(), imageID, input, location)
			if err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					commitTried = true // COMMIT まで届いたかわからないので画像は消さない
				}
				return err
			}
			if !ok {
				return NewAPIError(http.StatusConflict, "post_slot_unavailable")
			}
			commitTried, committed = true, true
			if !externalImage(c, userID) {
				replicateImageVis(imageID, userID, "image/jpeg", imageData, memPushRegionsForPost(h.Mem, userID, input.Visibility, ""), input.Visibility)
			}
			return c.JSON(http.StatusCreated, newPostResponse(post, user))
		}
		tx, err := h.DB.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		user, err := getUserForUpdate(ctx, tx, userID)
		if err != nil {
			return err
		}
		if preInsert && !imageInserted {
			if _, err := tx.ExecContext(ctx, `INSERT INTO image_objects (id, owner_user_id, data, media_type, created_at) VALUES ($1, $2, $3, 'image/jpeg', $4)`, imageID, userID, imageData, time.Now().UTC()); err != nil {
				return err
			}
			imageInserted = true
		}
		var window struct {
			ID        string    `db:"id"`
			LocalDate time.Time `db:"local_date"`
			StartsAt  time.Time `db:"starts_at"`
		}
		// FOR UPDATE だと同一リージョンの投稿がすべて直列化されるため FOR SHARE にする。
		// ISUREAL_PW_KEYSHARE=1 (既定) は FOR KEY SHARE: ended_at はキー列ではない (部分一意索引の述語のみ) ので
		// 窓を閉じる UPDATE (FOR NO KEY UPDATE) と衝突せず、GP の窓の開始が走行中の投稿を待たない。
		// 投稿期間の終了 (UPDATE ended_at) は進行中の投稿のコミットを待つので整合性は変わらない。
		err = tx.GetContext(ctx, &window, windowLockSQL, user.Region)
		if errors.Is(err, sql.ErrNoRows) {
			// 投稿期間の切り替えと競合した場合、新しいスナップショットで取り直す (元実装どおり)。
			err = tx.GetContext(ctx, &window, windowLockSQL, user.Region)
		}
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return NewAPIError(http.StatusConflict, "post_slot_unavailable")
			}
			return err
		}
		var counts struct {
			Active  int `db:"active"`
			Deleted int `db:"deleted"`
		}
		if err := tx.GetContext(ctx, &counts, `
   SELECT count(*) FILTER (WHERE deleted_at IS NULL) AS active,
          count(*) FILTER (WHERE deleted_at IS NOT NULL) AS deleted
   FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE p.author_id = $1 AND w.local_date = $2
  `, userID, window.LocalDate); err != nil {
			return err
		}
		postedAt := time.Now().UTC()
		if counts.Active != 0 || counts.Deleted >= 2 || postedAt.Before(window.StartsAt) {
			return NewAPIError(http.StatusConflict, "post_slot_unavailable")
		}
		external := externalImage(c, userID)
		if !external && !imageInserted && !asiaKept {
			asiaKept = asiaLocalImgKeep(ctx, imageID, userID, imageData)
		}
		if !external && !imageInserted && !asiaKept {
			if _, err := tx.ExecContext(ctx, `INSERT INTO image_objects (id, owner_user_id, data, media_type, created_at) VALUES ($1, $2, $3, 'image/jpeg', $4)`, imageID, userID, imageData, postedAt); err != nil {
				return err
			}
		}
		postID := uuid.NewString()
		if _, err := tx.ExecContext(ctx, `
   INSERT INTO posts (id, author_id, posting_window_id, image_id, caption, visibility,
                      location_kind, latitude, longitude, locality, posted_at)
   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
  `, postID, userID, window.ID, imageID, input.Caption, input.Visibility,
			location.Kind, location.Latitude, location.Longitude, location.Locality, postedAt); err != nil {
			return err
		}
		post, err := getPost(ctx, tx, postID)
		if err != nil {
			return err
		}
		commitTried = true
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		if !external {
			replicateImageVis(imageID, userID, "image/jpeg", imageData, memPushRegionsForPost(h.Mem, userID, input.Visibility, ""), input.Visibility)
		}
		return c.JSON(http.StatusCreated, newPostResponse(post, user))
	}
}

func newPostResponse(post Post, user User) createPostResponse {
	response := createPostResponse{
		ID: post.ID,
		Author: postAuthorResponse{
			ID: user.ID, Username: user.Username, DisplayName: user.DisplayName,
			ProfileImageURL: newGetMeResponse(user).ProfileImageURL,
			Bio:             user.Bio, AccountType: user.AccountType,
		},
		PostingWindowID: post.PostingWindowID, ImageURL: signedImageURL(user, post.ImageID),
		Caption: post.Caption, Visibility: post.Visibility, Location: post.PostLocation,
		PostedAt: post.PostedAt, OnTime: !post.PostedAt.After(post.OnTimeDeadline),
	}
	if !response.OnTime {
		response.LateSeconds = int64(post.PostedAt.Sub(post.OnTimeDeadline) / time.Second)
	}
	return response
}

type getMyCurrentPostResponse createPostResponse

func getMyCurrentPostHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if h.Mem.on() {
			if ok, err := h.Mem.myCurrentPost(c); ok {
				return err
			}
		}
		ctx := c.Request().Context()
		userID := GetSession(c).UserID
		var postID string
		err := h.Reader().GetContext(ctx, &postID, `
   SELECT p.id FROM posts p
   JOIN posting_windows w ON w.id = p.posting_window_id
   JOIN users u ON u.id = p.author_id
   JOIN posting_windows current ON current.region = u.region AND current.ended_at IS NULL
                              AND current.local_date = w.local_date
   WHERE p.author_id = $1 AND p.deleted_at IS NULL
  `, userID)
		if errors.Is(err, sql.ErrNoRows) {
			return NewAPIError(http.StatusNotFound, "post_not_found")
		}
		if err != nil {
			return err
		}
		post, err := getPost(ctx, h.Reader(), postID)
		if errors.Is(err, sql.ErrNoRows) {
			return NewAPIError(http.StatusNotFound, "post_not_found")
		}
		if err != nil {
			return err
		}
		user, err := getUser(ctx, h.Reader(), userID)
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, getMyCurrentPostResponse(newPostResponse(post, user)))
	}
}

type getPostRequest struct {
	PostID string `param:"postId"`
}

type getPostResponse createPostResponse

func getPostHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input getPostRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		if h.Mem.on() {
			if ok, err := h.Mem.getPost(c, input.PostID); ok {
				return err
			}
		}
		ctx := c.Request().Context()
		post, err := getPostWithAuthor(ctx, h.Reader(), input.PostID)
		if errors.Is(err, sql.ErrNoRows) {
			return NewAPIError(http.StatusNotFound, "post_not_found")
		}
		if err != nil {
			return err
		}
		viewer, err := loadViewerContext(ctx, h.Reader(), GetSession(c).UserID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err != nil || !viewer.canView(post.Post) {
			return NewAPIError(http.StatusNotFound, "post_not_found")
		}
		return c.JSON(http.StatusOK, getPostResponse(newPostResponse(post.Post, post.Author)))
	}
}

type updatePostRequest struct {
	PostID     string  `param:"postId" json:"-"`
	Caption    *string `json:"caption"`
	Visibility *string `json:"visibility"`
}

type updatePostResponse createPostResponse

func updatePostHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input updatePostRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		if input.Caption == nil && input.Visibility == nil {
			return NewAPIError(http.StatusBadRequest, "invalid_post")
		}
		if input.Caption != nil && !validUserText(*input.Caption, 0, 1000) {
			return NewAPIError(http.StatusBadRequest, "invalid_post")
		}
		if input.Visibility != nil {
			switch *input.Visibility {
			case "friends", "friends_of_friends", "public":
			default:
				return NewAPIError(http.StatusBadRequest, "invalid_post")
			}
		}
		ctx := c.Request().Context()
		userID := GetSession(c).UserID
		if h.Store != nil {
			if ok, err := h.Store.updatePost(c, userID, input); ok {
				return err
			}
		}
		tx, err := h.DB.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var lockedUserID string
		if err := tx.GetContext(ctx, &lockedUserID, `SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE`, userID); err != nil {
			return err
		}
		post, err := getPost(ctx, tx, input.PostID)
		if errors.Is(err, sql.ErrNoRows) {
			return NewAPIError(http.StatusNotFound, "post_not_found")
		}
		if err != nil {
			return err
		}
		if post.AuthorID != userID {
			return NewAPIError(http.StatusForbidden, "post_not_editable")
		}
		window, err := getPostingWindow(ctx, tx, post.PostingWindowID)
		if err != nil {
			return err
		}
		if window.EndedAt != nil {
			return NewAPIError(http.StatusForbidden, "post_not_editable")
		}
		if input.Caption != nil {
			if _, err := tx.ExecContext(ctx, `UPDATE posts SET caption = $2 WHERE id = $1`, post.ID, *input.Caption); err != nil {
				return err
			}
		}
		if input.Visibility != nil {
			if _, err := tx.ExecContext(ctx, `UPDATE posts SET visibility = $2 WHERE id = $1`, post.ID, *input.Visibility); err != nil {
				return err
			}
		}
		post, err = getPost(ctx, tx, post.ID)
		if err != nil {
			return err
		}
		user, err := getUser(ctx, tx, userID)
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return c.JSON(http.StatusOK, updatePostResponse(newPostResponse(post, user)))
	}
}

type deletePostRequest struct {
	PostID string `param:"postId" json:"-"`
}

func deletePostHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input deletePostRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		ctx := c.Request().Context()
		userID := GetSession(c).UserID
		if h.Store != nil {
			if ok, err := h.Store.deletePost(c, userID, input.PostID); ok {
				return err
			}
		}
		tx, err := h.DB.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var lockedUserID string
		if err := tx.GetContext(ctx, &lockedUserID, `SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE`, userID); err != nil {
			return err
		}
		post, err := getPost(ctx, tx, input.PostID)
		if errors.Is(err, sql.ErrNoRows) {
			return NewAPIError(http.StatusNotFound, "post_not_found")
		}
		if err != nil {
			return err
		}
		if post.AuthorID != userID {
			return NewAPIError(http.StatusForbidden, "post_not_editable")
		}
		window, err := getPostingWindow(ctx, tx, post.PostingWindowID)
		if err != nil {
			return err
		}
		var deletions int
		if err := tx.GetContext(ctx, &deletions, `
   SELECT count(*) FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE p.author_id = $1 AND w.local_date = $2 AND p.deleted_at IS NOT NULL
  `, userID, window.LocalDate); err != nil {
			return err
		}
		if deletions >= 2 {
			return NewAPIError(http.StatusConflict, "post_slot_unavailable")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE posts SET deleted_at = clock_timestamp() WHERE id = $1`, post.ID); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return c.NoContent(http.StatusNoContent)
	}
}

var windowLockSQL = func() string {
	if os.Getenv("ISUREAL_PW_KEYSHARE") == "0" {
		return `SELECT id, local_date, starts_at FROM posting_windows WHERE region = $1 AND ended_at IS NULL FOR SHARE`
	}
	return `SELECT id, local_date, starts_at FROM posting_windows WHERE region = $1 AND ended_at IS NULL FOR KEY SHARE`
}()

// dropOrphanImage は、tx の外で先に入れた画像の行を、投稿がコミットされなかったときに消す。
func dropOrphanImage(db *sqlx.DB, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `DELETE FROM image_objects WHERE id = $1`, id); err != nil {
		slog.Warn("post: drop orphan image failed", "image_id", id, "error", err)
	}
}
