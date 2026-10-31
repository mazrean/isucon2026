package isureal

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"

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
		userID := GetSession(c).UserID
		// mem_respcache.go: the zip of an unchanged post list is the same bytes
		rc := h.Mem.respCache(ctx)
		key := memRespKey{kind: memRespExport, user: userID}
		var fill memRespFill
		if rc != nil {
			if b := rc.get(key); b != nil {
				return writeExport(c, b)
			}
			fill = rc.begin()
			if exportRebuildOn {
				// signed-URL bucket rolled over: re-assemble with new signatures, no DB.
				if sk, _ := rc.staleAux(key).(*exportSkel); sk != nil {
					body, tags, err := assembleExport(sk)
					if err == nil && tags != nil {
						rc.putAux(key, fill, body, tags, sk)
						exportRebuilds.Add(1)
						return writeExport(c, body)
					}
				}
			}
			if r, ok := fetchOrigin(c); ok {
				if r.tags != nil {
					rc.put(key, fill, r.body, r.tags)
				}
				return r.write(c)
			}
		}
		defer heavyAcquire(ctx)()
		body, tags, sk, err := buildExportSkel(h, c, userID)
		if err != nil {
			return err
		}
		if rc != nil && tags != nil {
			if !exportRebuildOn {
				sk = nil
			}
			rc.putAux(key, fill, body, tags, sk)
		}
		if rc == nil {
			memRespSetTags(c, tags)
		}
		return writeExport(c, body)
	}
}

func writeExport(c *echo.Context, body []byte) error {
	c.Response().Header().Set(echo.HeaderContentDisposition, `attachment; filename="isureal-posts.zip"`)
	c.Response().Header().Set(echo.HeaderCacheControl, "no-store")
	return c.Blob(http.StatusOK, "application/zip", body)
}

// buildExport returns the zip and, when every post's image was found, the
// cache tags it depends on (nil = do not cache).
func buildExport(h *HandlerContext, c *echo.Context, userID string) ([]byte, []string, error) {
	body, tags, _, err := buildExportSkel(h, c, userID)
	return body, tags, err
}

// exportSkel is what an export zip is assembled from (DB rows + image bytes).
// ISUREAL_EXPORT_REBUILD=1 keeps it next to the cached zip so that, when the
// signed-URL bucket rolls over, the zip is re-assembled with the new
// signatures without touching the DB (same assembleExport, same bytes).
type exportSkel struct {
	userID string
	author User
	posts  []Post
	objs   []*imageObject
}

var (
	exportRebuildOn = os.Getenv("ISUREAL_EXPORT_REBUILD") == "1"
	exportRebuilds  atomic.Int64 // テスト / 観測用
)

func buildExportSkel(h *HandlerContext, c *echo.Context, userID string) ([]byte, []string, *exportSkel, error) {
	ctx := c.Request().Context()
	author, err := getUser(ctx, h.Reader(), userID)
	if err != nil {
		return nil, nil, nil, err
	}
	var posts []Post
	if err := h.Reader().SelectContext(ctx, &posts, `
   SELECT `+postColumns+`
   FROM posts p
   JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE p.author_id = $1 AND p.deleted_at IS NULL
   ORDER BY p.posted_at, p.id
  `, userID); err != nil {
		return nil, nil, nil, err
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
		return nil, nil, nil, err
	}
	skel := &exportSkel{userID: userID, author: author, posts: posts, objs: objs}
	b, tags, err := assembleExport(skel)
	if err != nil {
		return nil, nil, nil, err
	}
	return b, tags, skel, nil
}

// assembleExport builds the zip and its cache tags (nil = do not cache) from a skeleton.
func assembleExport(sk *exportSkel) ([]byte, []string, error) {
	author, posts, objs := sk.author, sk.posts, sk.objs
	userID := sk.userID
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
	tags := []string{"o" + userID, "u" + userID}
	seenWindow := map[string]bool{}
	cacheable := true
	items := make([]exportPostResponse, 0, len(posts))
	for i, post := range posts {
		// 元実装は image_objects との INNER JOIN だったので、画像が無い投稿は含めない。
		if objs[i] == nil {
			cacheable = false
			continue
		}
		if !seenWindow[post.PostingWindowID] {
			seenWindow[post.PostingWindowID] = true
			tags = append(tags, "w"+post.PostingWindowID)
		}
		imagePath := fmt.Sprintf("images/%06d.jpg", len(items)+1)
		image, err := writer.CreateHeader(&zip.FileHeader{Name: imagePath, Method: zip.Store})
		if err != nil {
			return nil, nil, err
		}
		if _, err := image.Write(objs[i].Data); err != nil {
			return nil, nil, err
		}
		items = append(items, exportPostResponse{createPostResponse: newPostResponse(post, author), ImagePath: imagePath})
	}
	manifest, err := writer.Create("posts.json")
	if err != nil {
		return nil, nil, err
	}
	if err := json.NewEncoder(manifest).Encode(items); err != nil {
		return nil, nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, nil, err
	}
	if !cacheable {
		tags = nil
	}
	return body.Bytes(), tags, nil
}
