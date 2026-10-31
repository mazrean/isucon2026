#!/usr/bin/env bash
# usage: scripts/deploy.sh [hosts...]   (default: asia1 europe1 america1)
# Builds webapp/go locally and deploys binary + nginx conf to app hosts, restarts services.
# Host state owned here: /home/isucon/webapp/go/isureal, /etc/nginx/nginx.conf, /etc/nginx/sites-available/isureal.conf
set -euo pipefail
cd "$(dirname "$0")/.."
HOSTS=("$@"); [[ ${#HOSTS[@]} -gt 0 ]] || HOSTS=(asia1 europe1 america1)
OUT=$(mktemp -d)
( cd webapp/go && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$OUT/isureal" ./cmd/isureal )
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10)
for h in "${HOSTS[@]}"; do
  (
    scp -q "$OUT/isureal" "$h:/tmp/isureal.new"
    scp -q etc/app/nginx.conf "$h:/tmp/nginx.conf"
    scp -q etc/app/isureal.conf "$h:/tmp/isureal.conf"
    "${SSH[@]}" "$h" 'set -e
      sudo install -m 644 /tmp/nginx.conf /etc/nginx/nginx.conf
      sudo install -m 644 /tmp/isureal.conf /etc/nginx/sites-available/isureal.conf
      sudo nginx -t -q
      install -m 755 /tmp/isureal.new /home/isucon/webapp/go/isureal
      sudo systemctl restart isureal-go
      sudo systemctl reload nginx
      sha1sum /home/isucon/webapp/go/isureal | cut -c1-12' </dev/null | sed "s/^/$h: /"
  ) &
done
wait
rm -rf "$OUT"
