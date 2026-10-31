package isureal

// ISUREAL_MEM=1 のときだけ起動時に primary へ適用する (冪等)。
// 各テーブルの行変更をトリガーで mem_changelog に記録し、各ホストは
// ローカル reader から xid スナップショット差分で tail してメモリに反映する。
// notifications / image_objects は対象外 (行数が多い/メモリに持たない)。
// 撤去: DROP TRIGGER mem_changelog ON <tbl> (下の memTriggerTables 全て); DROP TABLE mem_changelog;
const memSchemaSQL = `
CREATE TABLE IF NOT EXISTS mem_changelog (
  seq bigserial PRIMARY KEY,
  xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
  tbl text NOT NULL,
  op text NOT NULL,
  row jsonb
);
CREATE INDEX IF NOT EXISTS mem_changelog_xid ON mem_changelog(xid);
CREATE OR REPLACE FUNCTION mem_changelog_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    INSERT INTO mem_changelog(tbl, op, row) VALUES (TG_TABLE_NAME, 'D', to_jsonb(OLD));
    RETURN OLD;
  END IF;
  INSERT INTO mem_changelog(tbl, op, row) VALUES (TG_TABLE_NAME, left(TG_OP, 1), to_jsonb(NEW));
  RETURN NEW;
END $$;
`

var memTriggerTables = []string{
	"users", "login_sessions", "friendships", "follows", "block_relations",
	"friend_requests", "posting_windows", "posts", "comments", "realmojis",
}
