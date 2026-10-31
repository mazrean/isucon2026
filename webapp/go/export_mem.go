package isureal

import (
	"context"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// ISUREAL_EXPORT_MEM=1 (needs ISUREAL_MEM=1; 既定 0 = 挙動は変えない): GET /api/me/export の
// 作者と投稿一覧をメモリから組み立てる (画像の中身は DB 経路と同じ images.get)。
//   - MEM_FULL (hotFrom がゼロ時刻) なら全期間がメモリにあるので、そのまま答える。
//   - それ以外は、ホット外 (local_date < hotFrom) に未削除の投稿が無い利用者だけメモリで答える。
//     ホット外の投稿は新しく作られない (投稿は開いている期間にだけ作られる) ので、「無い」は
//     この memData の間ずっと正しい → (memData, hotFrom) ごとに覚える。PG が古くて削除済みの
//     ホット外投稿が見えても、DB 経路に落ちるだけ (安全側)。だから確認は barrier を通さず h.DB で読む。
//   - 応答は assembleExport (DB 経路と同じ関数) で作るので、並び (posted_at, id)・画像 URL は同じ。

var exportMem = os.Getenv("ISUREAL_EXPORT_MEM") == "1"

var exportMemServed, exportMemFell atomic.Int64 // テスト / 観測用

var exportColdCache struct {
	mu      sync.Mutex
	d       *memData
	hotFrom time.Time
	noCold  map[string]bool // userID -> ホット外に未削除の投稿が無い
}

func exportNoCold(d *memData, hotFrom time.Time, userID string) (bool, bool) {
	exportColdCache.mu.Lock()
	defer exportColdCache.mu.Unlock()
	if exportColdCache.d != d || !exportColdCache.hotFrom.Equal(hotFrom) {
		return false, false
	}
	v, ok := exportColdCache.noCold[userID]
	return v, ok
}

func exportSetNoCold(d *memData, hotFrom time.Time, userID string, v bool) {
	exportColdCache.mu.Lock()
	defer exportColdCache.mu.Unlock()
	if exportColdCache.d != d || !exportColdCache.hotFrom.Equal(hotFrom) {
		exportColdCache.d, exportColdCache.hotFrom, exportColdCache.noCold = d, hotFrom, map[string]bool{}
	}
	exportColdCache.noCold[userID] = v
}

// exportMemSkel: (skel, ok)。ok=false なら DB 経路へ。
func (h *HandlerContext) exportMemSkel(ctx context.Context, userID string) (*exportSkel, bool, error) {
	m := h.Mem
	if !exportMem || m == nil {
		return nil, false, nil
	}
	m.mu.RLock()
	if !m.onCtx(ctx) {
		m.mu.RUnlock()
		return nil, false, nil
	}
	d := m.d
	hotKnown, hotFrom := d.hotKnown, d.hotFrom
	u := d.users[userID]
	m.mu.RUnlock()
	if !hotKnown || !u.active() {
		return nil, false, nil
	}
	if !hotFrom.IsZero() {
		noCold, ok := exportNoCold(d, hotFrom, userID)
		if !ok {
			var cold bool
			if err := h.DB.GetContext(ctx, &cold, `
   SELECT EXISTS (SELECT 1 FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE p.author_id = $1 AND p.deleted_at IS NULL AND w.local_date < $2)`, userID, hotFrom); err != nil {
				return nil, false, nil
			}
			noCold = !cold
			exportSetNoCold(d, hotFrom, userID, noCold)
		}
		if !noCold {
			exportMemFell.Add(1)
			return nil, false, nil
		}
	}
	m.mu.RLock()
	if !m.onCtx(ctx) || m.d != d {
		m.mu.RUnlock()
		return nil, false, nil
	}
	u = d.users[userID]
	if !u.active() {
		m.mu.RUnlock()
		return nil, false, nil
	}
	author := u.User
	posts := make([]Post, 0, len(u.posts))
	for _, p := range u.posts {
		if !p.deleted {
			posts = append(posts, p.full())
		}
	}
	m.mu.RUnlock()
	sortPostsByPostedAt(posts)
	return &exportSkel{userID: userID, author: author, posts: posts}, true, nil
}

func sortPostsByPostedAt(posts []Post) {
	// u.posts は挿入順とは限らないので (posted_at, id) で並べ直す (DB 経路の ORDER BY と同じ)。
	sort.Slice(posts, func(i, j int) bool { return postLess(posts[i], posts[j]) })
}

func postLess(a, b Post) bool {
	if !a.PostedAt.Equal(b.PostedAt) {
		return a.PostedAt.Before(b.PostedAt)
	}
	return a.ID < b.ID
}

// fillExportImages: DB 経路と同じく image_cache.go から並列に取る。
func (h *HandlerContext) fillExportImages(ctx context.Context, sk *exportSkel) error {
	sk.objs = make([]*imageObject, len(sk.posts))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i := range sk.posts {
		g.Go(func() error {
			obj, err := images.get(gctx, h.Reader(), h.DB, sk.posts[i].ImageID)
			sk.objs[i] = obj
			return err
		})
	}
	return g.Wait()
}
