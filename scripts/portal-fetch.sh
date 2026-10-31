#!/usr/bin/env bash
# usage: scripts/portal-fetch.sh METHOD PATH [base64-body] -> raw response bytes on stdout (via logged-in playwright browser)
# Requires: playwright-cli browser opened with --profile=$HOME/.cache/playwright-isucon-portal
set -euo pipefail
M=$1; P=$2; B=${3:-}
cd "${PW_DIR:-/tmp/isucon/pw}" 2>/dev/null || { mkdir -p /tmp/isucon/pw; cd /tmp/isucon/pw; }
js="async page => { const r = await page.evaluate(async ([m,p,b]) => {
  const o = {method:m, headers:{'Accept':'application/vnd.google.protobuf, application/protobuf'}};
  const meta = document.querySelector('meta[name=csrf-token]'); if (meta) o.headers['X-CSRF-Token'] = meta.content;
  if (b) { o.headers['Content-Type'] = 'application/vnd.google.protobuf'; o.body = Uint8Array.from(atob(b), c => c.charCodeAt(0)); }
  const x = await fetch(p, o); const a = new Uint8Array(await x.arrayBuffer());
  let s = ''; for (const v of a) s += String.fromCharCode(v);
  return x.status + ' ' + btoa(s); }, ['$M','$P','$B']); return r; }"
out=$(timeout 60 playwright-cli run-code "$js" 2>&1)
line=$(grep -oE '^"?[0-9]{3} [A-Za-z0-9+/=]*"?$' <<<"$out" | head -1 | tr -d '"')
[[ -n $line ]] || { echo "portal-fetch: unexpected output: $(head -c 400 <<<"$out")" >&2; exit 11; }
code=${line%% *}; body=${line#* }
echo "HTTP $code" >&2
[[ $code == 401 || $code == 403 ]] && { echo PORTAL_AUTH_EXPIRED >&2; exit 10; }
base64 -d <<<"$body"
