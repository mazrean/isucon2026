package isureal

import (
	"context"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// ISUREAL_DBTRACE=1 (既定 0): このプロセスが投げた SQL を (プール名, SQL の先頭 80 文字) ごとに数え、
// 10 秒ごとに件数・合計時間の上位を slog に出す。eu/am から primary への往復の内訳を取るための計測。
// 応答・挙動は変えない (pgx の QueryTracer で時間を測るだけ)。
var dbTraceOn = os.Getenv("ISUREAL_DBTRACE") == "1"

type dbTraceStat struct {
	n   int
	sum time.Duration
	max time.Duration
}

type dbTracer struct {
	pool string
}

type dbTraceKey struct{}

// dbTraceObserve: テスト用 (nil 以外なら tracer 付きのプールの全文で呼ぶ)。
var dbTraceObserve func(ctx context.Context, sql string)

type dbTraceStart struct {
	at  time.Time
	sql string
}

var (
	dbTraceMu    sync.Mutex
	dbTraceStats = map[string]*dbTraceStat{}
	dbTraceOnce  sync.Once
)

// traceDSN: フラグ on のとき、tracer 付きの接続設定を stdlib に登録して、その名前を返す (sql.Open("pgx", name) で使う)。
func traceDSN(dsn, pool string) string {
	if !dbTraceOn && !storeWALOn && coldOrigin == "" {
		return dsn
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return dsn
	}
	cfg.Tracer = &dbTracer{pool: pool}
	if dbTraceOn {
		dbTraceOnce.Do(func() { go dbTraceLoop() })
	}
	return stdlib.RegisterConnConfig(cfg)
}

func dbTraceSQLKey(sql string) string {
	s := strings.Join(strings.Fields(sql), " ")
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}

func (t *dbTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if coldOrigin != "" && t.pool != "store" && coldOnPG(ctx) {
		return ctx // ISUREAL_COLD_ORIGIN: この要求は asia2 へ転送する (ctx は取り消し済み。cold_origin.go)
	}
	if storeWALOn && t.pool != "store" {
		ctx = pgBarrierWait(ctx) // ISUREAL_STORE_WAL: 到着時刻までの書き込みが PG に入るまで待つ (store_pgbarrier.go)
	}
	if dbTraceObserve != nil {
		dbTraceObserve(ctx, data.SQL)
	}
	if !dbTraceOn {
		return ctx
	}
	return context.WithValue(ctx, dbTraceKey{}, dbTraceStart{at: time.Now(), sql: data.SQL})
}

func (t *dbTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	st, ok := ctx.Value(dbTraceKey{}).(dbTraceStart)
	if !ok {
		return
	}
	d := time.Since(st.at)
	key := t.pool + " | " + dbTraceSQLKey(st.sql)
	dbTraceMu.Lock()
	s := dbTraceStats[key]
	if s == nil {
		s = &dbTraceStat{}
		dbTraceStats[key] = s
	}
	s.n++
	s.sum += d
	s.max = max(s.max, d)
	dbTraceMu.Unlock()
}

func dbTraceLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		dbTraceMu.Lock()
		stats := dbTraceStats
		dbTraceStats = map[string]*dbTraceStat{}
		dbTraceMu.Unlock()
		if len(stats) == 0 {
			continue
		}
		keys := make([]string, 0, len(stats))
		total, totalN := time.Duration(0), 0
		for k, s := range stats {
			keys = append(keys, k)
			total += s.sum
			totalN += s.n
		}
		sort.Slice(keys, func(i, j int) bool { return stats[keys[i]].sum > stats[keys[j]].sum })
		slog.Info("dbtrace: total", "n", totalN, "sum", total, "kinds", len(keys))
		for i, k := range keys {
			if i >= 20 {
				break
			}
			s := stats[k]
			slog.Info("dbtrace", "q", k, "n", s.n, "sum", s.sum, "avg", s.sum/time.Duration(s.n), "max", s.max)
		}
	}
}
