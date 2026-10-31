package isureal

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// ISUREAL_IMG_PREEXTEND_MB (0 = off): after initialize, grow the TOAST of the
// image_objects partition that receives new rows, then free it again.
//
// reset.sql TRUNCATEs that partition, so on every run its TOAST starts at 0
// bytes and every 360KB image INSERT must extend the file. In the grand prix
// the image INSERTs queue on that one relation extension lock (asia2
// pg_stat_activity: Lock/extend on INSERT INTO image_objects, asia1: 100+
// posts waiting for the h.Img pool). Pages freed by VACUUM (with truncation
// off) are in the free space map, and inserts take them from the FSM without
// the extension lock.
//
// The filler rows are never visible to the API: images are only read by id,
// the filler ids are random and referenced by nothing, and the rows are
// deleted again before the VACUUM. image_objects has no trigger and is not in
// the mem publication.
//
// Size: one run writes ~1.5GB of image TOAST into the partition (days 1-5 +
// grand prix), and the days use the free pages first, so the grand prix only
// benefits when MB covers about that much. Runs in the background after the
// initialize response; one connection, 16MB per INSERT.
var imgPreextendMB = envIntOrDefault("ISUREAL_IMG_PREEXTEND_MB", 0)

const imgPreextendMediaType = "application/x-isureal-preextend"

func startImgPreextend(db *sqlx.DB) {
	if imgPreextendMB <= 0 || db == nil {
		return
	}
	go func() {
		start := time.Now()
		// Inserting stops at its own deadline; the cleanup (DELETE + VACUUM)
		// always runs afterwards with a fresh context and a fresh connection,
		// so filler rows never outlive a slow or failed run.
		ictx, icancel := context.WithTimeout(context.Background(), 60*time.Second)
		part, inserted, ierr := imgPreextendFill(ictx, db, imgPreextendMB)
		icancel()
		var deleted int64
		var cerr error
		if part != "" {
			cctx, ccancel := context.WithTimeout(context.Background(), 90*time.Second)
			deleted, cerr = imgPreextendCleanup(cctx, db, part)
			ccancel()
		}
		slog.Info("img-preextend", "mb", imgPreextendMB, "partition", part, "inserted_mb", inserted,
			"deleted_rows", deleted, "took", time.Since(start), "fill_err", ierr, "cleanup_err", cerr)
	}()
}

// imgPreextendFill returns the partition name ("" if nothing may need cleanup)
// and the number of MB inserted.
func imgPreextendFill(ctx context.Context, db *sqlx.DB, mb int) (string, int, error) {
	conn, err := db.Connx(ctx)
	if err != nil {
		return "", 0, err
	}
	defer conn.Close()
	var owner string
	if err := conn.GetContext(ctx, &owner, `SELECT id FROM users ORDER BY created_at LIMIT 1`); err != nil {
		return "", 0, err
	}
	now := time.Now().UTC()
	// The partition that a row created now lands in.
	var part string
	if err := conn.GetContext(ctx, &part, `
   SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
   WHERE i.inhparent = 'image_objects'::regclass
     AND $1::timestamptz >= (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'FROM \(''([^'']+)''\)'))[1]::timestamptz
     AND $1::timestamptz <  (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \(''([^'']+)''\)'))[1]::timestamptz
   LIMIT 1`, now); err != nil {
		return "", 0, err
	}
	if !strings.HasPrefix(part, "image_objects_") || strings.ContainsAny(part, `"'; `) {
		return "", 0, nil
	}
	if _, err := conn.ExecContext(ctx, `ALTER TABLE `+part+` SET (vacuum_truncate = false, toast.vacuum_truncate = false)`); err != nil {
		return "", 0, err
	}
	inserted := 0
	for inserted < mb {
		n := min(16, mb-inserted)
		if _, err := conn.ExecContext(ctx, `
   INSERT INTO image_objects (id, owner_user_id, data, media_type, created_at)
   SELECT gen_random_uuid()::text, $1, convert_to(repeat('x', 1048576), 'UTF8'), $2, $3
   FROM generate_series(1, $4)`, owner, imgPreextendMediaType, now, n); err != nil {
			return part, inserted, err
		}
		inserted += n
	}
	return part, inserted, nil
}

func imgPreextendCleanup(ctx context.Context, db *sqlx.DB, part string) (int64, error) {
	res, err := db.ExecContext(ctx, `DELETE FROM `+part+` WHERE media_type = $1`, imgPreextendMediaType)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	_, err = db.ExecContext(ctx, `VACUUM (TRUNCATE false) `+part)
	return n, err
}
