#!/usr/bin/env bash
# usage: scripts/deploy.sh [hosts...]   (default: asia1 europe1 america1)
# Builds webapp/go locally and deploys binary + nginx conf to app hosts, restarts services.
# Host state owned here: /home/isucon/webapp/go/isureal, /etc/nginx/nginx.conf, /etc/nginx/sites-available/isureal.conf
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/isucon
if [[ -z ${LOCK_HELD:-} ]]; then exec 9>/tmp/isucon/env.lock; flock -w 600 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner 2>/dev/null)" >&2; exit 75; }; echo "$(date +%T) deploy $(git rev-parse --short HEAD)" >| /tmp/isucon/env.lock.owner; fi
HOSTS=("$@"); [[ ${#HOSTS[@]} -gt 0 ]] || HOSTS=(asia1 europe1 america1)
OUT=$(mktemp -d)
( cd webapp/go && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$OUT/isureal" ./cmd/isureal )
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10)
SECRET_FILE=${FWD_SECRET_FILE:-$HOME/.config/isucon2026/fwd_secret}
if [[ ! -s $SECRET_FILE ]]; then mkdir -p "${SECRET_FILE%/*}"; head -c 24 /dev/urandom | base64 | tr -d '/+=' >| "$SECRET_FILE"; chmod 600 "$SECRET_FILE"; fi
SECRET=$(cat "$SECRET_FILE")
FWD=${FWD:-1}   # 1 = europe1/america1 forward non-GET /api/ to asia1
region_of() { case $1 in asia1) echo asia;; europe1) echo europe;; america1) echo america;; esac; }
for h in "${HOSTS[@]}"; do
  (
    scp -q "$OUT/isureal" "$h:/tmp/isureal.new"
    scp -q etc/app/nginx.conf "$h:/tmp/nginx.conf"
    r=$(region_of "$h"); f=$FWD; [[ $h == asia1 ]] && f=0
    sed -e "s/__REGION__/$r/" -e "s/__FWD__/$f/" -e "s/__FWD_SECRET__/$SECRET/" etc/app/isureal.conf >"$OUT/isureal.$h.conf"
    scp -q "$OUT/isureal.$h.conf" "$h:/tmp/isureal.conf"
    "${SSH[@]}" "$h" 'set -e
      sudo install -m 644 /tmp/nginx.conf /etc/nginx/nginx.conf
      sudo install -m 644 /tmp/isureal.conf /etc/nginx/sites-available/isureal.conf
      grep -q "^ISUREAL_FWD_SECRET=" /home/isucon/env.sh && sed -i "s|^ISUREAL_FWD_SECRET=.*|ISUREAL_FWD_SECRET='"$SECRET"'|" /home/isucon/env.sh || echo "ISUREAL_FWD_SECRET='"$SECRET"'" >> /home/isucon/env.sh
      setenv() { grep -q "^$1=" /home/isucon/env.sh && sed -i "s|^$1=.*|$1=$2|" /home/isucon/env.sh || echo "$1=$2" >> /home/isucon/env.sh; }
      setenv ISUREAL_DB_READ_HOST '"${READ_HOST:-127.0.0.1}"'
      setenv ISUREAL_NODE_NAME '"$h"'
      sudo nginx -t -q
      install -m 755 /tmp/isureal.new /home/isucon/webapp/go/isureal
      sudo systemctl restart isureal-go
      sudo systemctl reload nginx
      sha1sum /home/isucon/webapp/go/isureal | cut -c1-12' </dev/null | sed "s/^/$h: /"
  ) &
done
wait
rm -rf "$OUT"
