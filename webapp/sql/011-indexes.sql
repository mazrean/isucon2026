-- 010-indexes.sql (ドライバー管理) に無い追加インデックス。既存 DB にもそのまま流せる (冪等)。
-- 本番適用: psql -h 192.168.4.10 -U isureal isureal -f 011-indexes.sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS users_username ON users(username);
CREATE INDEX CONCURRENTLY IF NOT EXISTS administrator_sessions_token_hash ON administrator_sessions(token_hash) WHERE active;
CREATE INDEX CONCURRENTLY IF NOT EXISTS users_updated_at ON users(updated_at);
