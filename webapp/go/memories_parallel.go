package isureal

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

// ISUREAL_MEMORIES_PAR=1: GET /api/memories and /api/memories/:date issue
// their three queries (posts, RealMojis, comments) at once instead of one
// after another, so europe1/america1 pay one round trip to the DB instead of
// three. The interaction queries select the page's posts with the same
// conditions as a subquery (LIMIT 50 = the items kept), so the SQL each row
// passes is the same as in listMemories; only the post-ID list is computed by
// the DB instead of by the app. As in listMemories, the three statements run
// outside a transaction (each sees its own snapshot).

var memoriesParallel = os.Getenv("ISUREAL_MEMORIES_PAR") == "1"

const memoriesPostFilter = `
  AND p.author_id = $1 AND w.local_date >= $2 AND w.local_date < $3
  AND ($4::timestamptz IS NULL OR (p.posted_at, p.id) < ($4, $5))`

// the page's post IDs, with selectPostsWithAuthor's base conditions
const memoriesPageIDs = `
  SELECT p.id FROM posts p
  JOIN posting_windows w ON w.id = p.posting_window_id
  JOIN users u ON u.id = p.author_id AND u.status = 'active'
  WHERE p.deleted_at IS NULL ` + memoriesPostFilter + `
  ORDER BY p.posted_at DESC, p.id DESC LIMIT 50`

func listMemoriesParallel(ctx context.Context, db *sqlx.DB, userID string, start, end time.Time, beforeAt *time.Time, beforeID string) (getMemoriesResponse, error) {
	var response getMemoriesResponse
	if (beforeAt == nil) != (beforeID == "") {
		return response, NewAPIError(http.StatusBadRequest, "invalid_pagination")
	}
	args := []any{userID, start, end, beforeAt, beforeID}
	var (
		posts     []PostWithAuthor
		reactions []RealMojiWithReactor
		comments  []CommentWithAuthor
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		posts, err = selectPostsWithAuthor(gctx, db, memoriesPostFilter+`
  ORDER BY p.posted_at DESC, p.id DESC LIMIT 51`, args...)
		return err
	})
	g.Go(func() (err error) {
		reactions, err = selectCurrentRealMojis(gctx, db, `AND r.post_id IN (`+memoriesPageIDs+`) ORDER BY r.created_at DESC, r.id DESC`, args...)
		return err
	})
	g.Go(func() (err error) {
		comments, err = selectCommentsWithAuthor(gctx, db, `AND c.post_id IN (`+memoriesPageIDs+`) ORDER BY c.created_at, c.id`, args...)
		return err
	})
	if err := g.Wait(); err != nil {
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
	attachMemoryInteractions(items, reactions, comments)
	response.Items = items
	return response, nil
}

// attachMemoryInteractions is loadMemoryInteractions after its two queries.
// Rows for posts not on the page (a post created/deleted between the
// statements) are dropped.
func attachMemoryInteractions(items []memoryPostResponse, reactions []RealMojiWithReactor, comments []CommentWithAuthor) {
	if len(items) == 0 {
		return
	}
	itemByPostID := make(map[string]*memoryPostResponse, len(items))
	for i := range items {
		itemByPostID[items[i].ID] = &items[i]
	}
	reqs := make([]imageURLReq, 0, 2*len(reactions)+len(comments))
	counts := make(map[string]int, len(items))
	for _, reaction := range reactions {
		if itemByPostID[reaction.PostID] != nil && counts[reaction.PostID] < 50 {
			counts[reaction.PostID]++
			reqs = appendImageURLReq(reqs, reaction.Reactor, reaction.ImageID)
			reqs = appendUserProfileImageReq(reqs, reaction.Reactor)
		}
	}
	clear(counts)
	for _, comment := range comments {
		if itemByPostID[comment.PostID] != nil && counts[comment.PostID] < 50 {
			counts[comment.PostID]++
			reqs = appendUserProfileImageReq(reqs, comment.Author)
		}
	}
	prewarmImageURLs(reqs)
	for _, reaction := range reactions {
		if item := itemByPostID[reaction.PostID]; item != nil && len(item.RealMojis) < 50 {
			item.RealMojis = append(item.RealMojis, newRealMojiResponse(reaction.RealMoji, reaction.Reactor))
		}
	}
	for _, comment := range comments {
		if item := itemByPostID[comment.PostID]; item != nil && len(item.Comments) < 50 {
			item.Comments = append(item.Comments, newCommentResponse(comment.Comment, comment.Author))
		}
	}
}
