package isureal

import (
	"os"
	"strings"
)

// image_objects is RANGE-partitioned on created_at (36 partitions) with PK
// (id, created_at). "WHERE id = $1" alone makes every lookup plan, lock and
// probe all 36 partitions + their pkeys (72 relation locks > 16 fast-path
// slots -> LWLock:LockManager contention on asia2; measured as the top wait of
// that query during the bench).
//
// Every ID minted during the contest carries a 6-hex tag (localimg eu/am, asia
// local, or imgHotTag below) and its row (if any) is created at clock time
// >= imgHotFrom, i.e. in the newest partition. For those IDs the lookup names
// the partition bound as a literal, so the planner prunes to one partition.
// A miss falls back to the full query (an initial-data UUID that happens to
// end in a tag, an asia-local ID with no row): never fewer answers than before.
//
// ISUREAL_IMG_HOTPART=0 disables it.
var imgHotPartOn = os.Getenv("ISUREAL_IMG_HOTPART") != "0"

// imgHotFrom/imgHotTo are the bounds of image_objects_20261028 (webapp/sql/001-schema.sql);
// later partitions (202611.., default) exist, so both bounds are needed to prune to one.
// A row created after imgHotTo is still found by the fallback.
const (
	imgHotFrom = "2026-10-28 06:00:00+00"
	imgHotTo   = "2026-11-01 00:00:00+00"
)

// imgHotTag tags IDs of new images that are neither eu/am-local nor asia-local.
const imgHotTag = "d0d0d0"

const (
	imageSelectAll = `SELECT owner_user_id, media_type, data FROM image_objects WHERE id = $1 LIMIT 1`
	imageSelectHot = `SELECT owner_user_id, media_type, data FROM image_objects WHERE id = $1 AND created_at >= '` + imgHotFrom + `' AND created_at < '` + imgHotTo + `' LIMIT 1`
)

func newHotImageID(id string) string {
	if !imgHotPartOn || len(id) != 36 {
		return id
	}
	return id[:30] + imgHotTag
}

// imgHotID reports whether id was minted during the contest (tagged).
func imgHotID(id string) bool {
	if !imgHotPartOn || len(id) != 36 {
		return false
	}
	return strings.HasSuffix(id, imgHotTag) || isAsiaLocalID(id) || localImgOriginOf(id) != ""
}
