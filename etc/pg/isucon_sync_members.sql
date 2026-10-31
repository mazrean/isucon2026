-- asia2 (primary) の isureal DB に作成。initialize で DROP/TRUNCATE されない独立テーブル。
-- pg-sync-watchdog が更新し、app はローカル replica でこの行を読む (false ならプライマリ読み)。
CREATE TABLE IF NOT EXISTS isucon_sync_members (
  name       text PRIMARY KEY,
  in_sync    boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO isucon_sync_members(name, in_sync) VALUES
  ('asia1', false), ('europe1', false), ('america1', false)
ON CONFLICT (name) DO NOTHING;
GRANT SELECT ON isucon_sync_members TO isureal;
