#!/usr/bin/env bash
# isureal-rs (Rust: GET /api/memories, /api/memories/:date, /api/me/export) on asia1.
# Go stays untouched on :8080; nginx sends only those 3 routes to 127.0.0.1:8081.
#
#   scripts/deploy-rs.sh build      # static musl binary
#   scripts/deploy-rs.sh on  [host] # install binary+unit, start, route nginx → rs   (default host asia1)
#   scripts/deploy-rs.sh off [host] # rollback: nginx routes back to Go, stop rs
#   scripts/deploy-rs.sh status [host]
#
# Run under the env lock like scripts/deploy.sh. Re-running scripts/deploy.sh rewrites
# /etc/nginx/sites-available/isureal.conf → routing falls back to Go (safe); run `on` again after it.
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
bin=$root/webapp/rust/isureal-rs/target/x86_64-unknown-linux-musl/release/isureal-rs
cmd=${1:-}
h=${2:-asia1}

case $cmd in
build)
	(cd "$root/webapp/rust/isureal-rs" && cargo +nightly build --release --target x86_64-unknown-linux-musl)
	ls -l "$bin"
	;;
on)
	test -x "$bin" || { echo "build first" >&2; exit 1; }
	scp -q "$bin" "$h:/tmp/isureal-rs"
	scp -q "$root/etc/systemd/isureal-rs.service" "$h:/tmp/isureal-rs.service"
	ssh "$h" bash -s <<'EOF'
set -euo pipefail
sudo install -d -o isucon -g isucon /home/isucon/webapp/rust
sudo install -m 755 -o isucon -g isucon /tmp/isureal-rs /home/isucon/webapp/rust/isureal-rs
sudo install -m 644 /tmp/isureal-rs.service /etc/systemd/system/isureal-rs.service
sudo systemctl daemon-reload
sudo systemctl enable isureal-rs >/dev/null 2>&1
sudo systemctl restart isureal-rs
for i in $(seq 1 50); do curl -s -o /dev/null -m 1 http://127.0.0.1:8081/api/me && break; sleep 0.1; done
code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/api/memories?month=2025-11)
[ "$code" = 401 ] || { echo "isureal-rs not healthy (got $code); leaving nginx on Go" >&2; exit 1; }
# host-environment smoke diff (TZ / env.sh / DB / imgcache perms): Go :8080 vs Rust :8081 on real data.
# NOTE: logs yuma in once (1 session row) -> run only outside an official bench, under the env lock.
systemctl show isureal-go -p Environment | sed 's/ISUREAL_FWD_SECRET=[^ ]*/ISUREAL_FWD_SECRET=***/'
python3 - <<'PY' || { echo "smoke diff FAILED; leaving nginx on Go" >&2; exit 1; }
import json, sys, urllib.request
def get(port, path, tok):
    r = urllib.request.Request(f"http://127.0.0.1:{port}{path}", headers={"Authorization": "Bearer " + tok})
    try:
        with urllib.request.urlopen(r, timeout=30) as x: return x.status, x.headers.get("Content-Type"), x.read()
    except urllib.error.HTTPError as e: return e.code, e.headers.get("Content-Type"), e.read()
r = urllib.request.Request("http://127.0.0.1:8080/api/auth/login", data=json.dumps({"username": "yuma_kobayashi_186", "secretKey": "0f6-BAnyBELD2MA-t-fekeDtQN4ynqD0mfgOF8JWwWs"}).encode(), headers={"Content-Type": "application/json"})
tok = json.loads(urllib.request.urlopen(r, timeout=30).read())["token"]
bad = 0
for p in ["/api/memories?month=2025-11", "/api/memories?month=2025-12", "/api/memories/2025-11-09", "/api/me/export"]:
    a, b = get(8080, p, tok), get(8081, p, tok)
    ok = a == b
    print(("ok  " if ok else "DIFF"), p, a[0], len(a[2]), b[0], len(b[2]))
    bad += not ok
sys.exit(1 if bad else 0)
PY
conf=/etc/nginx/sites-available/isureal.conf
if ! grep -q 'app_rs' "$conf"; then
  sudo cp "$conf" /tmp/isureal.conf.pre-rs
  sudo python3 - "$conf" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
up = "upstream app_rs {\n  server 127.0.0.1:8081;\n  keepalive 64;\n  keepalive_requests 100000;\n}\n"
loc = ""
for m in ["= /api/memories", "^~ /api/memories/", "= /api/me/export"]:
    loc += ("  location %s {\n    proxy_http_version 1.1;\n    proxy_set_header Connection \"\";\n"
            "    proxy_set_header Host $host;\n    proxy_set_header X-Isureal-Fwd-Region \"\";\n"
            "    proxy_set_header X-Isureal-Fwd-Secret \"\";\n    proxy_pass http://app_rs;\n  }\n\n") % m
s = up + s
i = s.index("  location /api/ {")
s = s[:i] + loc + s[i:]
open(p, "w").write(s)
PY
fi
sudo nginx -t && sudo systemctl reload nginx
echo "isureal-rs on"
EOF
	;;
off)
	ssh "$h" bash -s <<'EOF'
set -euo pipefail
conf=/etc/nginx/sites-available/isureal.conf
if grep -q 'app_rs' "$conf"; then
  sudo python3 - "$conf" <<'PY'
import re, sys
p = sys.argv[1]
s = open(p).read()
s = re.sub(r"upstream app_rs \{.*?\n\}\n", "", s, flags=re.S)
s = re.sub(r"  location (= /api/memories|\^~ /api/memories/|= /api/me/export) \{.*?\n  \}\n\n", "", s, flags=re.S)
open(p, "w").write(s)
PY
fi
sudo nginx -t && sudo systemctl reload nginx
sudo systemctl disable --now isureal-rs >/dev/null 2>&1 || true
echo "isureal-rs off (all traffic on Go)"
EOF
	;;
status)
	ssh "$h" 'systemctl is-active isureal-rs; grep -c app_rs /etc/nginx/sites-available/isureal.conf || true; sudo journalctl -u isureal-rs -n 5 --no-pager'
	;;
*)
	echo "usage: $0 build|on|off|status [host]" >&2
	exit 2
	;;
esac
