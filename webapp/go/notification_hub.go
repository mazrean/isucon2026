package isureal

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const notificationPollInterval = 100 * time.Millisecond

// notificationHub は、SSE 接続ごとの DB ポーリングをプロセス内 1 本にまとめる。
// アクティブな投稿期間の一覧を読み取り接続でポーリングし、新しい期間が見えたらその通知をまとめて読み込んで配る。
// 期間と通知は同一トランザクションで作られるため、期間が見えた時点で通知も見えている。
// 別リージョンのホストで作られた期間も DB 経由で検出するので届く。
type notificationHub struct {
	reader func() *sqlx.DB

	mu      sync.Mutex
	windows map[string]map[string]getNotificationsResponse // window id -> user id -> notification
	subs    map[string]map[*notificationSubscriber]struct{}
	userCur time.Time
}

type notificationSubscriber struct {
	userID      string
	region      string
	connectedAt time.Time
	// 1 件だけ送る。nil は接続終了 (退会・リージョン変更) を表す。
	ch   chan *getNotificationsResponse
	done bool
}

func newNotificationHub(reader func() *sqlx.DB) *notificationHub {
	hub := &notificationHub{
		reader:  reader,
		windows: map[string]map[string]getNotificationsResponse{},
		subs:    map[string]map[*notificationSubscriber]struct{}{},
		userCur: time.Now().Add(-time.Minute),
	}
	go hub.run()
	return hub
}

func (h *notificationHub) subscribe(userID, region string, connectedAt time.Time) *notificationSubscriber {
	sub := &notificationSubscriber{userID: userID, region: region, connectedAt: connectedAt, ch: make(chan *getNotificationsResponse, 1)}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[userID] == nil {
		h.subs[userID] = map[*notificationSubscriber]struct{}{}
	}
	h.subs[userID][sub] = struct{}{}
	// 接続前に検出済みの期間でも、接続時刻より後に作られた通知なら届ける。
	if n, ok := h.earliestLocked(userID, connectedAt); ok {
		h.deliverLocked(sub, &n)
	}
	return sub
}

func (h *notificationHub) unsubscribe(sub *notificationSubscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs := h.subs[sub.userID]; subs != nil {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(h.subs, sub.userID)
		}
	}
}

func (h *notificationHub) deliverLocked(sub *notificationSubscriber, n *getNotificationsResponse) {
	if sub.done {
		return
	}
	sub.done = true
	sub.ch <- n
	if subs := h.subs[sub.userID]; subs != nil {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(h.subs, sub.userID)
		}
	}
}

// earliestLocked は、元実装の `ORDER BY n.created_at, n.id LIMIT 1` と同じ順で最初の通知を返す。
func (h *notificationHub) earliestLocked(userID string, after time.Time) (getNotificationsResponse, bool) {
	var best getNotificationsResponse
	found := false
	for _, byUser := range h.windows {
		n, ok := byUser[userID]
		if !ok || !n.CreatedAt.After(after) {
			continue
		}
		if !found || n.CreatedAt.Before(best.CreatedAt) || (n.CreatedAt.Equal(best.CreatedAt) && n.ID < best.ID) {
			best, found = n, true
		}
	}
	return best, found
}

func (h *notificationHub) run() {
	ticker := time.NewTicker(notificationPollInterval)
	defer ticker.Stop()
	for range ticker.C {
		if err := h.poll(); err != nil {
			slog.Error("notification poll failed", "error", err)
		}
	}
}

func (h *notificationHub) poll() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db := h.reader()

	var active []string
	if err := db.SelectContext(ctx, &active, `SELECT id FROM posting_windows WHERE ended_at IS NULL`); err != nil {
		return err
	}
	activeSet := make(map[string]bool, len(active))
	var fresh []string
	h.mu.Lock()
	for _, id := range active {
		activeSet[id] = true
		if _, ok := h.windows[id]; !ok {
			fresh = append(fresh, id)
		}
	}
	for id := range h.windows {
		if !activeSet[id] {
			delete(h.windows, id)
		}
	}
	h.mu.Unlock()

	for _, windowID := range fresh {
		var rows []struct {
			getNotificationsResponse
			UserID string `db:"user_id"`
		}
		if err := db.SelectContext(ctx, &rows, `
   SELECT n.id, n.user_id, n.posting_window_id, w.region, n.created_at
   FROM notifications n JOIN posting_windows w ON w.id = n.posting_window_id
   WHERE n.posting_window_id = $1
  `, windowID); err != nil {
			return err
		}
		byUser := make(map[string]getNotificationsResponse, len(rows))
		for _, row := range rows {
			byUser[row.UserID] = row.getNotificationsResponse
		}
		var targets []*notificationSubscriber
		h.mu.Lock()
		h.windows[windowID] = byUser
		for userID, subs := range h.subs {
			if _, ok := byUser[userID]; !ok {
				continue
			}
			for sub := range subs {
				targets = append(targets, sub)
			}
		}
		h.mu.Unlock()
		h.deliver(ctx, db, targets)
	}
	return h.checkUsers(ctx, db)
}

// deliver は、元実装と同じく送信直前に受信者が有効かつ同じリージョンであることを確認してから送る。
func (h *notificationHub) deliver(ctx context.Context, db *sqlx.DB, targets []*notificationSubscriber) {
	if len(targets) == 0 {
		return
	}
	userIDs := make([]string, 0, len(targets))
	for _, sub := range targets {
		userIDs = append(userIDs, sub.userID)
	}
	regions, err := activeUserRegions(ctx, db, userIDs)
	if err != nil {
		slog.Error("failed to load notification recipients", "error", err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range targets {
		region, ok := regions[sub.userID]
		if !ok || region != sub.region {
			h.deliverLocked(sub, nil)
			continue
		}
		if n, ok := h.earliestLocked(sub.userID, sub.connectedAt); ok {
			h.deliverLocked(sub, &n)
		}
	}
}

// checkUsers は、退会やリージョン変更をした利用者の接続を閉じる (元実装の毎回の getUser 相当)。
func (h *notificationHub) checkUsers(ctx context.Context, db *sqlx.DB) error {
	h.mu.Lock()
	if len(h.subs) == 0 {
		h.mu.Unlock()
		return nil
	}
	since := h.userCur
	h.mu.Unlock()
	var rows []struct {
		ID        string    `db:"id"`
		Region    string    `db:"region"`
		Status    string    `db:"status"`
		UpdatedAt time.Time `db:"updated_at"`
	}
	// コミット遅れを考慮して少し重ねて読む。処理は冪等。
	if err := db.SelectContext(ctx, &rows, `SELECT id, region, status, updated_at FROM users WHERE updated_at > $1`, since.Add(-5*time.Second)); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, row := range rows {
		if row.UpdatedAt.After(h.userCur) {
			h.userCur = row.UpdatedAt
		}
		for sub := range h.subs[row.ID] {
			if row.Status != "active" || row.Region != sub.region {
				h.deliverLocked(sub, nil)
			}
		}
	}
	return nil
}

func activeUserRegions(ctx context.Context, db *sqlx.DB, userIDs []string) (map[string]string, error) {
	var rows []struct {
		ID     string `db:"id"`
		Region string `db:"region"`
	}
	query, args, err := sqlx.In(`SELECT id, region FROM users WHERE status = 'active' AND id IN (?)`, userIDs)
	if err != nil {
		return nil, err
	}
	if err := db.SelectContext(ctx, &rows, db.Rebind(query), args...); err != nil {
		return nil, err
	}
	regions := make(map[string]string, len(rows))
	for _, row := range rows {
		regions[row.ID] = row.Region
	}
	return regions, nil
}
