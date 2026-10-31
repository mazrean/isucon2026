#!/usr/bin/env bash
# 初期データ (initial_data_boundary 以前) から yuma_kobayashi_186 の Memories/export 期待値を再抽出する。
# usage: tools/pseudobench/golden/extract.sh > tools/pseudobench/golden/yuma_memories.json
set -euo pipefail
ssh -o BatchMode=yes -o ConnectTimeout=10 asia2 "sudo -u postgres psql -d isureal -At" <<'SQL'
SELECT json_agg(x ORDER BY x.posted_at, x.id) FROM (
SELECT p.id, to_char(w.local_date,'YYYY-MM-DD') AS local_date, p.caption, p.visibility, p.location_kind, p.posted_at,
 encode(sha256(i.data),'hex') AS image_sha256,
 (SELECT coalesce(json_agg(r.id ORDER BY r.created_at DESC, r.id DESC),'[]') FROM realmojis r JOIN initial_users u ON u.id=r.reactor_id AND u.status='active'
   WHERE r.post_id=p.id AND r.replaced_at IS NULL AND r.deleted_at IS NULL AND r.created_at <= (SELECT created_at FROM initial_data_boundary)) AS realmoji_ids,
 (SELECT coalesce(json_agg(c.id ORDER BY c.created_at, c.id),'[]') FROM comments c JOIN initial_users u ON u.id=c.author_id AND u.status='active'
   WHERE c.post_id=p.id AND c.deleted_at IS NULL AND c.created_at <= (SELECT created_at FROM initial_data_boundary)) AS comment_ids
FROM posts p JOIN posting_windows w ON w.id=p.posting_window_id JOIN image_objects i ON i.id=p.image_id
WHERE p.author_id=(SELECT id FROM users WHERE username='yuma_kobayashi_186') AND p.deleted_at IS NULL
  AND p.created_at <= (SELECT created_at FROM initial_data_boundary)) x;
SQL
