package isureal

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ISUREAL_MEMCACHE_FINE=1 (with ISUREAL_MEMORIES_CACHE=1; default 0 = unchanged):
// Memories entries no longer hang on the owner tag "o"+viewer (dropped by every
// posts row of the viewer, so a new post today dropped every past month).
// Instead, when stored (putAux, on this node, whatever tags an origin sent),
// "o"+viewer is replaced by
//   - "om"+viewer+"|"+YYYY-MM  the calendar month of the period (a month or a
//     day period lies inside one month)
//   - "oa"+viewer              any posts row of the viewer whose month is unknown
//
// and a posts row invalidates "o"+author (exports keep the owner tag),
// "p"+post, and "om"+author+"|"+month of the post's window local_date, or
// "oa"+author when the window (or its date) is not known yet.
// The other tags are kept as they are: "u"+viewer (display name, status /
// deletion, image key), "u"+reactor/commenter, "p"+post (comments,
// RealMojis, the post itself), "w"+window. A users row equal in every field
// to the one in memory invalidates nothing (no response field can change).
//
// Every 10s a log line gives hit / miss / put / veto and, per cause, the
// invalidations and the entries they actually removed.
var memCacheFine = os.Getenv("ISUREAL_MEMCACHE_FINE") == "1"

const (
	rcWhyPostMonth = iota // posts row, month known
	rcWhyPostBroad        // posts row, month unknown
	rcWhyUser             // users row changed
	rcWhyUserNoop         // users row unchanged (skipped)
	rcWhyWindow
	rcWhyComment
	rcWhyRealMoji
	rcWhyClear
	rcWhyN
)

var rcWhyName = [rcWhyN]string{"post_month", "post_broad", "user", "user_noop", "window", "comment", "realmoji", "clear"}

var memRespInvN, memRespInvRemoved [rcWhyN]atomic.Int64

func memFineMonthTag(user string, month time.Time) string {
	return "om" + user + "|" + month.UTC().Format("2006-01")
}

// fineTags rewrites the tags of a Memories entry (see above).
func fineTags(k memRespKey, tags []string) []string {
	if !memCacheFine || k.kind != memRespMemories {
		return tags
	}
	owner := "o" + k.user
	out := make([]string, 0, len(tags)+1)
	for _, t := range tags {
		if t == owner {
			out = append(out, memFineMonthTag(k.user, time.Unix(0, k.start)), "oa"+k.user)
			continue
		}
		out = append(out, t)
	}
	return out
}

// respInvalidatePostFine: posts row r, before it is applied (MEMCACHE_FINE).
func (d *memData) respInvalidatePostFine(r memPostRow) {
	tags := []string{"o" + r.AuthorID, "p" + r.ID}
	why := rcWhyPostMonth
	var w *memWindow
	if p := d.posts[r.ID]; p != nil {
		w = p.window
		if p.author != nil && p.author.ID != r.AuthorID {
			tags = append(tags, "o"+p.author.ID, "oa"+p.author.ID)
			why = rcWhyPostBroad
		}
		if w != nil && w.ID != r.PostingWindowID {
			w = nil
		}
	} else {
		w = d.windows[r.PostingWindowID]
	}
	if w == nil || w.LocalDate.IsZero() {
		tags = append(tags, "oa"+r.AuthorID)
		why = rcWhyPostBroad
	} else {
		tags = append(tags, memFineMonthTag(r.AuthorID, w.LocalDate))
	}
	d.rc.invalidateWhy(why, tags...)
}

// memUserSame: the users row carries nothing new for the user in memory.
func memUserSame(u *memUser, r memUserRow) bool {
	if u == nil {
		return false
	}
	o := u.User
	samePtr := (o.ProfileImageID == nil) == (r.ProfileImageID == nil) &&
		(o.ProfileImageID == nil || *o.ProfileImageID == *r.ProfileImageID)
	return o.ID == r.ID && o.Username == r.Username && o.SecretKeyHash == r.SecretKeyHash &&
		o.DisplayName == r.DisplayName && o.Bio == r.Bio && samePtr && o.Region == r.Region &&
		o.AccountType == r.AccountType && o.Status == r.Status
}

func init() {
	if memCacheFine && memRespCacheOn {
		go memRespFineLog()
	}
}

func memRespFineLog() {
	var last string
	for range time.Tick(10 * time.Second) {
		var b strings.Builder
		for i := range rcWhyN {
			if n := memRespInvN[i].Load(); n > 0 {
				b.WriteString(rcWhyName[i])
				b.WriteString("=")
				b.WriteString(strconv.FormatInt(n, 10))
				b.WriteString("/")
				b.WriteString(strconv.FormatInt(memRespInvRemoved[i].Load(), 10))
				b.WriteString(" ")
			}
		}
		s := b.String()
		line := strconv.FormatInt(memRespHits.Load(), 10) + " " + strconv.FormatInt(memRespMiss.Load(), 10) + " " + s
		if line == last {
			continue
		}
		last = line
		slog.Info("mem: respcache", "hit", memRespHits.Load(), "miss", memRespMiss.Load(), "put", memRespPuts.Load(), "veto", memRespVeto.Load(), "invalidations(n/removed)", s)
	}
}
