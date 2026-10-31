package isureal

import (
	"context"
	"database/sql"
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
	// 監視専用の接続 (各 1 本)。リクエスト用プールが詰まっても監視が失敗しないように分ける。
	monitorPrimary *sqlx.DB
	monitorReplica *sqlx.DB
	healthy        atomic.Bool
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
		ok := r.check(last)
		r.healthy.Store(ok)
		if ok != last {
			slog.Info("read replica state changed", "healthy", ok, "node", r.nodeName)
			last = ok
		}
	}
}

func (r *readRouter) check(wasHealthy bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// replica が primary から streaming 中か。
	// pg_read_all_stats 権限が無いと status は NULL になる。その場合は行の存在 (= WAL receiver 稼働) だけを見る。
	// 正しさは primary 上の in_sync (watchdog が同期集合から外す前に false にする) で担保される。
	var status sql.NullString
	if err := r.monitorReplica.GetContext(ctx, &status, `SELECT status FROM pg_stat_wal_receiver`); err != nil || (status.Valid && status.String != "streaming") {
		return false
	}
	// watchdog が primary に書く同期メンバー表。遅延中の replica 上の値は古い可能性があるので primary から読む。
	var member struct {
		InSync bool   `db:"in_sync"`
		LSN    string `db:"lsn"`
	}
	if err := r.monitorPrimary.GetContext(ctx, &member, `SELECT in_sync, pg_current_wal_lsn()::text AS lsn FROM isucon_sync_members WHERE name = $1`, r.nodeName); err != nil || !member.InSync {
		return false
	}
	if wasHealthy {
		return true
	}
	// 非健全→健全の切り替え時だけ、replica が primary の現在位置まで適用済みであることを確認する
	// (同期集合へ戻された直後はまだ遅れを取り戻している途中の可能性がある)。
	var caughtUp bool
	if err := r.monitorReplica.GetContext(ctx, &caughtUp, `SELECT coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn, false)`, member.LSN); err != nil {
		return false
	}
	return caughtUp
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
	router.monitorReplica, _ = sqlx.Open("pgx", config.databaseDSNForHost(config.ReadHost))
	router.monitorPrimary, _ = sqlx.Open("pgx", config.DatabaseDSN())
	configurePool(router.monitorReplica, 1)
	configurePool(router.monitorPrimary, 1)
	router.nodeName = config.NodeName
	if router.nodeName == "" {
		router.nodeName, _ = os.Hostname()
	}
	router.healthy.Store(router.check(false))
	go router.run()
	return router
}

func configurePool(db *sqlx.DB, size int) {
	db.SetMaxOpenConns(size)
	db.SetMaxIdleConns(size)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
}
