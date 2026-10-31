-- 010-indexes.sql (ドライバー管理) に無い追加インデックス。既存 DB にもそのまま流せる (冪等)。
-- 本番適用: psql -h 192.168.4.10 -U isureal isureal -f 011-indexes.sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS users_username ON users(username);
CREATE INDEX CONCURRENTLY IF NOT EXISTS administrator_sessions_token_hash ON administrator_sessions(token_hash) WHERE active;
CREATE INDEX CONCURRENTLY IF NOT EXISTS users_updated_at ON users(updated_at);
-- 画像 (JPEG, 最大 ~700KB) は圧縮できないのに、bytea 既定の STORAGE EXTENDED だと TOAST 時に毎回 pglz を全量試す。
-- ローカル計測 (PG16, 715KB JPEG 300 行): EXTENDED 2159ms / EXTERNAL 299ms (7.2x)。21KB: 510ms→142ms / 3000 行。
-- 本番 asia2 のベンチ中 (GP) は INSERT INTO image_objects が PG の CPU 状態サンプルの約 8 割で、
-- 同じ tx がユーザー行ロックを握るので待ち (Lock:transactionid/tuple) も長くなっていた。
-- カタログのみの変更 (書き換え無し・既存行はそのまま)。親に掛ければ全パーティションと今後のパーティションに効く。
-- 実画像 80 枚 (計 20MB) 混在 x10: EXTENDED 2.0s / EXTERNAL 0.3s。
-- AccessExclusiveLock を一瞬取るので、ベンチ外で流すこと (lock_timeout で詰まりを防ぐ。タイムアウトしたら再実行)。
-- init/reset では消えない (テーブルを作り直さない)。確認: SELECT attstorage FROM pg_attribute WHERE attrelid='image_objects_20261028'::regclass AND attname='data'; → e
SET lock_timeout = '5s';
ALTER TABLE image_objects ALTER COLUMN data SET STORAGE EXTERNAL;
