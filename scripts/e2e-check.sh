#!/usr/bin/env bash
# ISUReal ブラウザ E2E 構造チェック (初期実装ベースラインとの差分)
#   scripts/e2e-check.sh [URL]          # 差分チェック。exit 0 = FAIL なし
#   scripts/e2e-check.sh --record [URL] # ベースライン再作成 (初期実装のときだけ!)
# 環境変数: E2E_STRICT=1 (DOM 追加要素も FAIL), E2E_PAUSE_MS (操作間待ち, 既定 250), E2E_BUDGET_S (全体時間上限, 既定 110)
# 出力: tests/e2e/out/<timestamp>/{summary.json,diff.txt,raw/,screens/}
set -euo pipefail
cd "$(dirname "$0")/../tests/e2e"
mode=check
if [[ "${1:-}" == "--record" ]]; then mode=record; shift; fi
url="${1:-https://57.183.58.128}"
if [[ ! -d node_modules/playwright ]]; then npm ci --no-audit --no-fund >/dev/null; fi
if ! node -e "require('fs').accessSync(require('playwright').chromium.executablePath())" 2>/dev/null; then
  npx playwright install chromium
fi
# 公式ベンチ・デプロイと排他 (初期ユーザーのログインは既存セッションを無効化し、投稿など書き込みもあるため)
if [[ -z ${LOCK_HELD:-} ]]; then
  mkdir -p /tmp/isucon
  exec 9>/tmp/isucon/env.lock
  flock -w 900 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner 2>/dev/null)" >&2; exit 75; }
  echo "$(date +%T) e2e-check $mode" >| /tmp/isucon/env.lock.owner
fi
node run.mjs "$mode" "$url"
