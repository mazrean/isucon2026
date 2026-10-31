#!/usr/bin/env bash
# usage: scripts/fail-dump.sh <bench log> <outfile>
# On a consistency FAIL, snapshot the rows behind each "見えてはいけない" violation
# (reader, post author, their relations and mem_changelog history) before the next
# initialize wipes them.
set -uo pipefail
log=$1 out=$2
pairs=$(grep -oE '[A-Za-z0-9_.-]+ (に見えてはいけない投稿|に見えるべき投稿) [0-9a-f-]{36}' "$log" | sort -u | awk '{print $1" "$NF}')
[ -n "$pairs" ] || exit 0
{
  while read -r reader post; do
    echo "=== reader=$reader post=$post"
    ssh -o BatchMode=yes asia2 "sudo -u postgres psql -d isureal -P pager=off" <<SQL
\set r '$reader'
\set p '$post'
SELECT id AS reader_id, region FROM users WHERE username = :'r' \gset
SELECT p.author_id, p.visibility, p.created_at, p.deleted_at, u.username AS author, u.region AS author_region FROM posts p JOIN users u ON u.id = p.author_id WHERE p.id = :'p' \gset
\echo reader :reader_id :region author :author :author_id :author_region post :visibility :created_at :deleted_at
SELECT posting_window_id FROM posts WHERE id = :'p' \gset
\echo post window :posting_window_id
SELECT w.*, w.id = :'posting_window_id' AS is_post_window FROM posting_windows w WHERE w.starts_at BETWEEN :'created_at'::timestamptz - interval '60 seconds' AND :'created_at'::timestamptz + interval '20 seconds' OR w.ended_at BETWEEN :'created_at'::timestamptz - interval '20 seconds' AND :'created_at'::timestamptz + interval '20 seconds' OR w.id = :'posting_window_id' ORDER BY w.starts_at;
SELECT * FROM friendships WHERE :'reader_id' IN (user_low_id, user_high_id) OR :'author_id' IN (user_low_id, user_high_id) ORDER BY created_at;
SELECT * FROM friend_requests WHERE requester_id IN (:'reader_id', :'author_id') OR recipient_id IN (:'reader_id', :'author_id') ORDER BY created_at;
SELECT * FROM block_relations WHERE blocker_id IN (:'reader_id', :'author_id') OR blocked_id IN (:'reader_id', :'author_id') ORDER BY created_at;
SELECT seq, tbl, op, row FROM mem_changelog WHERE tbl IN ('friendships', 'friend_requests', 'block_relations', 'posts', 'posting_windows') AND (row::text LIKE '%' || :'reader_id' || '%' OR row::text LIKE '%' || :'author_id' || '%') ORDER BY seq;
SQL
  done <<<"$pairs"
} >"$out" 2>&1
echo "fail dump: $out"

# 401 invalid_session: the user's region, sessions and the region-change history
users401=$(grep -oE '"[A-Za-z0-9_.-]+: [^"]*401[^"]*invalid_session' "$log" | sed -E 's/^"([^:]+):.*/\1/' | sort -u)
[ -n "$users401" ] || exit 0
{
  for u in $users401; do
    echo "=== 401 user=$u"
    ssh -o BatchMode=yes asia2 "sudo -u postgres psql -d isureal -P pager=off" <<SQL
\set u '$u'
SELECT id AS uid, region, status, updated_at FROM users WHERE username = :'u' \gset
\echo user :uid :region :status :updated_at
SELECT id, active, reason, created_at, invalidated_at FROM login_sessions WHERE user_id = :'uid' ORDER BY created_at;
SELECT seq, op, row->>'region' AS region, row->>'updated_at' AS updated_at FROM mem_changelog WHERE tbl = 'users' AND row->>'id' = :'uid' ORDER BY seq;
SELECT seq, op, row->>'active' AS active, row->>'reason' AS reason, row->>'created_at' AS created_at, row->>'invalidated_at' AS inv FROM mem_changelog WHERE tbl = 'login_sessions' AND row->>'user_id' = :'uid' ORDER BY seq;
SQL
  done
} >>"$out" 2>&1
