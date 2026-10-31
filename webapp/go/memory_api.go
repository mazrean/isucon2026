package isureal

import (
	"context"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v5"
)

type getMemoriesRequest struct {
	Month          string     `query:"month"`
	BeforePostedAt *time.Time `query:"beforePostedAt"`
	BeforeID       string     `query:"beforeId"`
}

type memoryPostResponse struct {
	createPostResponse
	RealMojis []replaceRealMojiResponse `json:"realmojis"`
	Comments  []createCommentResponse   `json:"comments"`
}

type getMemoriesResponse struct {
	Items   []memoryPostResponse `json:"items"`
	HasMore bool                 `json:"hasMore"`
}

type getMemoriesByDateRequest struct {
	Date           string     `param:"date"`
	BeforePostedAt *time.Time `query:"beforePostedAt"`
	BeforeID       string     `query:"beforeId"`
}

type getMemoriesByDateResponse getMemoriesResponse

func getMemoriesHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input getMemoriesRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		start, err := time.Parse("2006-01", input.Month)
		if err != nil {
			return NewAPIError(http.StatusBadRequest, "invalid_memory_period")
		}
		end := start.AddDate(0, 1, 0)
		if h.Mem.on() {
			if ok, response, err := h.Mem.memories(c, GetSession(c).UserID, start, end, input.BeforePostedAt, input.BeforeID); ok {
				if err != nil {
					return err
				}
				return c.JSON(http.StatusOK, response)
			}
		}
		return serveMemories(h, c, GetSession(c).UserID, start, end, input.BeforePostedAt, input.BeforeID, func(r getMemoriesResponse) any { return r })
	}
}

func getMemoriesByDateHandler(h *HandlerContext) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var input getMemoriesByDateRequest
		if err := c.Bind(&input); err != nil {
			return err
		}
		start, err := time.Parse(time.DateOnly, input.Date)
		if err != nil {
			return NewAPIError(http.StatusBadRequest, "invalid_memory_period")
		}
		end := start.AddDate(0, 0, 1)
		if h.Mem.on() {
			if ok, response, err := h.Mem.memories(c, GetSession(c).UserID, start, end, input.BeforePostedAt, input.BeforeID); ok {
				if err != nil {
					return err
				}
				return c.JSON(http.StatusOK, getMemoriesByDateResponse(response))
			}
		}
		return serveMemories(h, c, GetSession(c).UserID, start, end, input.BeforePostedAt, input.BeforeID, func(r getMemoriesResponse) any { return getMemoriesByDateResponse(r) })
	}
}

func listMemories(ctx context.Context, db sqlx.QueryerContext, userID string, start, end time.Time, beforeAt *time.Time, beforeID string) (getMemoriesResponse, error) {
	if pdb, ok := db.(*sqlx.DB); ok && memoriesParallel {
		return listMemoriesParallel(ctx, pdb, userID, start, end, beforeAt, beforeID)
	}
	var response getMemoriesResponse
	if (beforeAt == nil) != (beforeID == "") {
		return response, NewAPIError(http.StatusBadRequest, "invalid_pagination")
	}
	posts, err := selectPostsWithAuthor(ctx, db, `
  AND p.author_id = $1 AND w.local_date >= $2 AND w.local_date < $3
  AND ($4::timestamptz IS NULL OR (p.posted_at, p.id) < ($4, $5))
  ORDER BY p.posted_at DESC, p.id DESC LIMIT 51
 `, userID, start, end, beforeAt, beforeID)
	if err != nil {
		return response, err
	}
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
	if err := loadMemoryInteractions(ctx, db, items); err != nil {
		return response, err
	}
	response.Items = items
	return response, nil
}

func loadMemoryInteractions(ctx context.Context, db sqlx.QueryerContext, items []memoryPostResponse) error {
	if len(items) == 0 {
		return nil
	}
	postIDs := make([]string, len(items))
	itemByPostID := make(map[string]*memoryPostResponse, len(items))
	for i := range items {
		postIDs[i] = items[i].ID
		itemByPostID[items[i].ID] = &items[i]
	}

	query, args, err := sqlx.In(`AND r.post_id IN (?) ORDER BY r.created_at DESC, r.id DESC`, postIDs)
	if err != nil {
		return err
	}
	reactions, err := selectCurrentRealMojis(ctx, db, sqlx.Rebind(sqlx.DOLLAR, query), args...)
	if err != nil {
		return err
	}
	query, args, err = sqlx.In(`AND c.post_id IN (?) ORDER BY c.created_at, c.id`, postIDs)
	if err != nil {
		return err
	}
	comments, err := selectCommentsWithAuthor(ctx, db, sqlx.Rebind(sqlx.DOLLAR, query), args...)
	if err != nil {
		return err
	}
	// sign every URL the loops below will emit (same 50-per-post caps) as one batch.
	reqs := make([]imageURLReq, 0, 2*len(reactions)+len(comments))
	counts := make(map[string]int, len(items))
	for _, reaction := range reactions {
		if counts[reaction.PostID] < 50 {
			counts[reaction.PostID]++
			reqs = appendImageURLReq(reqs, reaction.Reactor, reaction.ImageID)
			reqs = appendUserProfileImageReq(reqs, reaction.Reactor)
		}
	}
	clear(counts)
	for _, comment := range comments {
		if counts[comment.PostID] < 50 {
			counts[comment.PostID]++
			reqs = appendUserProfileImageReq(reqs, comment.Author)
		}
	}
	prewarmImageURLs(reqs)

	for _, reaction := range reactions {
		item := itemByPostID[reaction.PostID]
		if len(item.RealMojis) < 50 {
			item.RealMojis = append(item.RealMojis, newRealMojiResponse(reaction.RealMoji, reaction.Reactor))
		}
	}
	for _, comment := range comments {
		item := itemByPostID[comment.PostID]
		if len(item.Comments) < 50 {
			item.Comments = append(item.Comments, newCommentResponse(comment.Comment, comment.Author))
		}
	}
	return nil
}
