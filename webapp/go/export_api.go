package isureal

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
	"golang.org/x/sync/errgroup"
)

type exportPostResponse struct {
	createPostResponse
	ImagePath string `json:"imagePath"`
}

func exportPostsHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		defer heavyAcquire(ctx)()
		userID := GetSession(c).UserID
		author, err := getUser(ctx, h.Reader(), userID)
		if err != nil {
			return err
		}
		var posts []Post
		if err := h.Reader().SelectContext(ctx, &posts, `
   SELECT `+postColumns+`
   FROM posts p
   JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE p.author_id = $1 AND p.deleted_at IS NULL
   ORDER BY p.posted_at, p.id
  `, userID); err != nil {
			return err
		}
		// 画像の中身は不変なので、image_cache.go のキャッシュ (メモリ → ディスク → DB) から並列に取る。
		objs := make([]*imageObject, len(posts))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(8)
		for i := range posts {
			g.Go(func() error {
				obj, err := images.get(gctx, h.Reader(), h.DB, posts[i].ImageID)
				objs[i] = obj
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		total := 0
		for _, obj := range objs {
			if obj != nil {
				total += len(obj.Data) + 128
			}
		}

		var body bytes.Buffer
		body.Grow(total + 4096 + len(posts)*1024)
		writer := zip.NewWriter(&body)
		defer writer.Close()
		reqs := []imageURLReq{}
		reqs = appendUserProfileImageReq(reqs, author)
		for i, post := range posts {
			if objs[i] != nil {
				reqs = appendImageURLReq(reqs, author, post.ImageID)
			}
		}
		prewarmImageURLs(reqs)
		items := make([]exportPostResponse, 0, len(posts))
		for i, post := range posts {
			// 元実装は image_objects との INNER JOIN だったので、画像が無い投稿は含めない。
			if objs[i] == nil {
				continue
			}
			imagePath := fmt.Sprintf("images/%06d.jpg", len(items)+1)
			image, err := writer.CreateHeader(&zip.FileHeader{Name: imagePath, Method: zip.Store})
			if err != nil {
				return err
			}
			if _, err := image.Write(objs[i].Data); err != nil {
				return err
			}
			items = append(items, exportPostResponse{createPostResponse: newPostResponse(post, author), ImagePath: imagePath})
		}
		manifest, err := writer.Create("posts.json")
		if err != nil {
			return err
		}
		if err := json.NewEncoder(manifest).Encode(items); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
		c.Response().Header().Set(echo.HeaderContentDisposition, `attachment; filename="isureal-posts.zip"`)
		c.Response().Header().Set(echo.HeaderCacheControl, "no-store")
		return c.Blob(http.StatusOK, "application/zip", body.Bytes())
	}
}
