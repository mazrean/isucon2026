package isureal

import (
	"context"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
)

// ISUREAL_MEM_HOT_EXTRA_DAYS=N (> 0, needs ISUREAL_MEM=1): the loaders also
// load the posts (and their comments / RealMojis) of the N days before the
// hot range they would load otherwise (the earliest open window's date, or
// the day after the latest window when none is open), and hotFrom moves back
// by the same N days. The invariant stays "every post whose window local_date
// >= hotFrom is in memory"; the change stream already applies every posts /
// comments / realmojis row whose post is in memory, so those days are kept
// up to date like the rest. Memories (MEMORIES_MEM) for periods inside the
// extended range are then answered from memory instead of the DB / origin.
//
// ISUREAL_MEM_HOT_EXTRA_MAX_POSTS (default 80000): when the extra days hold
// more posts than this (or the count fails), nothing is extended and the
// current hot range is used (memory bound for asia1).
var (
	memHotExtraDays     = envIntDefault("ISUREAL_MEM_HOT_EXTRA_DAYS", 0)
	memHotExtraMaxPosts = envIntDefault("ISUREAL_MEM_HOT_EXTRA_MAX_POSTS", 80000)
)

// hotBase is the date setHotFrom would set for minDate (ok false: unknown / no windows).
func (d *memData) hotBase(minDate *time.Time) (time.Time, bool) {
	if minDate != nil {
		return *minDate, true
	}
	var last time.Time
	for _, w := range d.windows {
		if w.LocalDate.After(last) {
			last = w.LocalDate
		}
	}
	if last.IsZero() {
		return time.Time{}, false
	}
	return last.AddDate(0, 0, 1), true
}

// hotLoadFrom returns the date the loaders load posts from (and pass to
// setHotFrom). With the flag off it returns minDate unchanged.
func (d *memData) hotLoadFrom(ctx context.Context, q sqlx.QueryerContext, minDate *time.Time) *time.Time {
	if memHotExtraDays <= 0 {
		return minDate
	}
	base, ok := d.hotBase(minDate)
	if !ok {
		return minDate
	}
	from := base.AddDate(0, 0, -memHotExtraDays)
	var n int
	if err := sqlx.GetContext(ctx, q, &n, `
   SELECT count(*) FROM posts p JOIN posting_windows w ON w.id = p.posting_window_id
   WHERE w.local_date >= $1::date AND w.local_date < $2::date`, from.Format(time.DateOnly), base.Format(time.DateOnly)); err != nil {
		slog.Warn("mem: hot extend count failed; not extended", "err", err)
		return minDate
	}
	if n > memHotExtraMaxPosts {
		slog.Warn("mem: hot extend over the post cap; not extended", "posts", n, "max", memHotExtraMaxPosts)
		return minDate
	}
	slog.Info("mem: hot range extended", "from", from.Format(time.DateOnly), "base", base.Format(time.DateOnly), "extra_posts", n)
	return &from
}
