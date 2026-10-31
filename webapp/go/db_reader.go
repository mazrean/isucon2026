package isureal

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
)

const replicaCheckInterval = 150 * time.Millisecond

// readRouter は、ローカルのホットスタンバイが同期レプリカとして健全なときだけ読み取りを replica に流す。
// 健全でない・判定できないときは primary を使う (read-your-writes を守る安全側)。
type readRouter struct {
	primary  *sqlx.DB
	replica  *sqlx.DB
	nodeName string
	healthy  atomic.Bool
}

func (r *readRouter) reader() *sqlx.DB {
	if r.replica != nil && r.healthy.Load() {
		return r.replica
	}
	return r.primary
}

func (r *readRouter) run() {
	ticker := time.NewTicker(replicaCheckInterval)
	defer ticker.Stop()
	last := false
	for range ticker.C {
		ok := r.check()
		r.healthy.Store(ok)
		if ok != last {
			slog.Info("read replica state changed", "healthy", ok, "node", r.nodeName)
			last = ok
		}
	}
}

func (r *readRouter) check() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// replica が primary から streaming 中か。
	var status string
	if err := r.replica.GetContext(ctx, &status, `SELECT status FROM pg_stat_wal_receiver`); err != nil || status != "streaming" {
		return false
	}
	// watchdog が primary に書く同期メンバー表。遅延中の replica 上の値は古い可能性があるので primary から読む。
	var inSync bool
	if err := r.primary.GetContext(ctx, &inSync, `SELECT in_sync FROM isucon_sync_members WHERE name = $1`, r.nodeName); err != nil || !inSync {
		return false
	}
	return true
}

func newReadRouter(config *Config, primary *sqlx.DB) *readRouter {
	router := &readRouter{primary: primary}
	if !config.ReadReplicaEnabled() {
		return router
	}
	replica, err := sqlx.Open("pgx", config.databaseDSNForHost(config.ReadHost))
	if err != nil {
		slog.Error("failed to open read replica; reading from primary", "error", err)
		return router
	}
	configurePool(replica, 50)
	router.replica = replica
	router.nodeName = config.NodeName
	if router.nodeName == "" {
		router.nodeName, _ = os.Hostname()
	}
	router.healthy.Store(router.check())
	go router.run()
	return router
}

func configurePool(db *sqlx.DB, size int) {
	db.SetMaxOpenConns(size)
	db.SetMaxIdleConns(size)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
}
