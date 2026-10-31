package isureal

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

const notificationPollInterval = 100 * time.Millisecond

// ISUREAL_NOTIFY_MEM=1: 期間と利用者をメモリ (memStore) から読む。PG を読まないので STORE_WAL の barrier が掛からない。
// メモリが使えない (未ロード・lease 切れ) 間と、メモリに居ない利用者は従来どおり PG を読む。
var notifyMemOn = os.Getenv("ISUREAL_NOTIFY_MEM") == "1"

// notificationHub は、SSE 接続ごとの DB ポーリングをプロセス内 1 本にまとめる。
// アクティブな投稿期間の一覧を読み取り接続でポーリングし、新しい期間が見えたら該当リージョンの接続へ配る。
//
// 通知は notifications 行を作らずに (期間, 利用者) から導出する。元実装は開始時点で
// region=期間のリージョン かつ status='active' の利用者全員に行を作っていたが、届けるのは
// 「接続時刻より後に作られた通知」だけで、送信直前に現在のリージョン一致・有効を確認し、
// リージョン変更・退会で接続を閉じる。よって届く相手は「期間開始前から同じリージョンで接続していて、
// 送信時点でも有効かつ同じリージョン」の利用者に限られ、これは行の有無に依らず決まる。
// ID は (user_id, posting_window_id) から UUID v5 で決定的に作り、createdAt は期間の starts_at。
type notificationHub struct {
	reader func() *sqlx.DB
	mem    atomic.Pointer[memStore] // ISUREAL_NOTIFY_MEM

	mu      sync.Mutex
	windows map[string]notificationWindow // window id -> window
	subs    map[string]map[*notificationSubscriber]struct{}
	userCur time.Time
}

type notificationWindow struct {
	Region   string    `db:"region"`
	StartsAt time.Time `db:"starts_at"`
}

var notificationIDNamespace = uuid.MustParse("6f1d3c2a-8b4e-5a7f-9c0d-1e2f3a4b5c6d")

func notificationID(userID, windowID string) string {
	return uuid.NewSHA1(notificationIDNamespace, []byte(userID+"/"+windowID)).String()
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
		windows: map[string]notificationWindow{},
		subs:    map[string]map[*notificationSubscriber]struct{}{},
		userCur: time.Now().Add(-time.Minute),
	}
	go hub.run()
	return hub
}

// setMem: setupMem の後に呼ぶ。nil なら何もしない。
func (h *notificationHub) setMem(m *memStore) {
	if h != nil && m != nil {
		h.mem.Store(m)
	}
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
	if n, ok := h.earliestLocked(sub); ok {
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
// 対象は購読者のリージョンの期間のうち、接続時刻より後に開始したもの。
func (h *notificationHub) earliestLocked(sub *notificationSubscriber) (getNotificationsResponse, bool) {
	var best getNotificationsResponse
	found := false
	for windowID, w := range h.windows {
		if w.Region != sub.region || !w.StartsAt.After(sub.connectedAt) {
			continue
		}
		id := notificationID(sub.userID, windowID)
		if !found || w.StartsAt.Before(best.CreatedAt) || (w.StartsAt.Equal(best.CreatedAt) && id < best.ID) {
			best = getNotificationsResponse{ID: id, PostingWindowID: windowID, Region: w.Region, CreatedAt: w.StartsAt}
			found = true
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

type notificationActiveWindow struct {
	ID string `db:"id"`
	notificationWindow
}

func (h *notificationHub) poll() error {
	if notifyMemOn {
		if m := h.mem.Load(); m.on() {
			return h.pollMem(m)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = storePGReadCtx(ctx) // ISUREAL_STORE_WAL: poll 開始までの書き込みが PG に入ってから読む
	db := h.reader()

	var active []notificationActiveWindow
	if err := db.SelectContext(ctx, &active, `SELECT id, region, starts_at FROM posting_windows WHERE ended_at IS NULL`); err != nil {
		return err
	}
	targets := h.applyActive(active)
	h.deliver(ctx, db, targets)
	return h.checkUsers(ctx, db)
}

// pollMem: poll と同じ処理を memStore から行う (ISUREAL_NOTIFY_MEM)。
func (h *notificationHub) pollMem(m *memStore) error {
	m.mu.RLock()
	d := m.d
	active := make([]notificationActiveWindow, 0, len(d.open))
	for _, w := range d.windows {
		if w.EndedAt == nil {
			active = append(active, notificationActiveWindow{ID: w.ID, notificationWindow: notificationWindow{Region: w.Region, StartsAt: w.StartsAt}})
		}
	}
	m.mu.RUnlock()
	targets := h.applyActive(active)
	if len(targets) > 0 {
		regions := map[string]string{}
		var missing []string
		m.mu.RLock()
		for _, sub := range targets {
			u := m.d.users[sub.userID]
			if u == nil {
				missing = append(missing, sub.userID)
			} else if u.active() {
				regions[sub.userID] = u.Region
			}
		}
		m.mu.RUnlock()
		if len(missing) > 0 {
			// メモリに居ない利用者だけ PG で確かめる (まれ)。
			ctx, cancel := context.WithTimeout(storePGReadCtx(context.Background()), 3*time.Second)
			pg, err := activeUserRegions(ctx, h.reader(), missing)
			cancel()
			if err != nil {
				slog.Error("failed to load notification recipients", "error", err)
			} else {
				for id, r := range pg {
					regions[id] = r
				}
				h.deliverWith(targets, regions)
			}
		} else {
			h.deliverWith(targets, regions)
		}
	}
	h.checkUsersMem(m)
	return nil
}

// checkUsersMem: checkUsers と同じく、退会・リージョン変更をした利用者の接続を閉じる。
func (h *notificationHub) checkUsersMem(m *memStore) {
	h.mu.Lock()
	if len(h.subs) == 0 {
		h.mu.Unlock()
		return
	}
	type subState struct {
		sub    *notificationSubscriber
		region string
	}
	subs := make([]subState, 0, len(h.subs))
	for _, ss := range h.subs {
		for sub := range ss {
			subs = append(subs, subState{sub: sub, region: sub.region})
		}
	}
	h.mu.Unlock()
	var closing, missing []*notificationSubscriber
	m.mu.RLock()
	for _, s := range subs {
		u := m.d.users[s.sub.userID]
		if u == nil {
			missing = append(missing, s.sub)
		} else if u.Status != "active" || u.Region != s.region {
			closing = append(closing, s.sub)
		}
	}
	m.mu.RUnlock()
	if len(missing) > 0 {
		// メモリに居ない利用者だけ PG で確かめる (まれ)。
		ids := make([]string, 0, len(missing))
		for _, sub := range missing {
			ids = append(ids, sub.userID)
		}
		ctx, cancel := context.WithTimeout(storePGReadCtx(context.Background()), 3*time.Second)
		pg, err := activeUserRegions(ctx, h.reader(), ids)
		cancel()
		if err != nil {
			slog.Error("notification user check failed", "error", err)
		} else {
			for _, sub := range missing {
				if r, ok := pg[sub.userID]; !ok || r != sub.region {
					closing = append(closing, sub)
				}
			}
		}
	}
	if len(closing) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range closing {
		h.deliverLocked(sub, nil)
	}
}

// applyActive: 有効な期間の一覧を取り込み、新しい期間で届ける通知がある接続を返す。
func (h *notificationHub) applyActive(active []notificationActiveWindow) []*notificationSubscriber {
	activeSet := make(map[string]bool, len(active))
	freshRegions := map[string]bool{}
	h.mu.Lock()
	for _, w := range active {
		activeSet[w.ID] = true
		if _, ok := h.windows[w.ID]; !ok {
			h.windows[w.ID] = w.notificationWindow
			freshRegions[w.Region] = true
		}
	}
	for id := range h.windows {
		if !activeSet[id] {
			delete(h.windows, id)
		}
	}
	var targets []*notificationSubscriber
	if len(freshRegions) > 0 {
		for _, subs := range h.subs {
			for sub := range subs {
				if !freshRegions[sub.region] {
					continue
				}
				// 元実装と同じく、届ける通知がある接続だけを対象にする。
				if _, ok := h.earliestLocked(sub); ok {
					targets = append(targets, sub)
				}
			}
		}
	}
	h.mu.Unlock()
	return targets
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
	h.deliverWith(targets, regions)
}

// deliverWith: regions (有効な受信者の id -> リージョン) で確認して送る。
func (h *notificationHub) deliverWith(targets []*notificationSubscriber, regions map[string]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range targets {
		region, ok := regions[sub.userID]
		if !ok || region != sub.region {
			h.deliverLocked(sub, nil)
			continue
		}
		if n, ok := h.earliestLocked(sub); ok {
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
