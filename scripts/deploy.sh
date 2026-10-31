#!/usr/bin/env bash
# usage: scripts/deploy.sh [hosts...]   (default: asia1 europe1 america1)
# Builds webapp/go locally and deploys binary + nginx conf to app hosts, restarts services.
# Host state owned here: /home/isucon/webapp/go/isureal, /etc/nginx/nginx.conf, /etc/nginx/sites-available/isureal.conf
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/isucon
if [[ -z ${LOCK_HELD:-} ]]; then exec 9>>/tmp/isucon/env.lock; flock -w 600 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner 2>/dev/null)" >&2; exit 75; }; echo "$(date +%T) deploy $(git rev-parse --short HEAD)" >| /tmp/isucon/env.lock.owner; fi
HOSTS=("$@"); [[ ${#HOSTS[@]} -gt 0 ]] || HOSTS=(asia1 europe1 america1)
OUT=$(mktemp -d)
( cd webapp/go && GOOS=linux GOARCH=amd64 GOAMD64=v3 CGO_ENABLED=0 go build -o "$OUT/isureal" ./cmd/isureal )
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10)
SECRET_FILE=${FWD_SECRET_FILE:-$HOME/.config/isucon2026/fwd_secret}
if [[ ! -s $SECRET_FILE ]]; then mkdir -p "${SECRET_FILE%/*}"; head -c 24 /dev/urandom | base64 | tr -d '/+=' >| "$SECRET_FILE"; chmod 600 "$SECRET_FILE"; fi
SECRET=$(cat "$SECRET_FILE")
FWD=${FWD:-1}   # 1 = europe1/america1 forward non-GET /api/ to asia1
FWD_TARGET=${FWD_TARGET:-192.168.1.10}   # forward target app (asia1=192.168.1.10, asia2=192.168.4.10)
MEM=${MEM:-1}   # 1 = in-memory read model (asia1 pump -> europe1/america1 peers)
PREVALIDATE=${PREVALIDATE:-1}   # 1 = europe1/america1 check JPEGs locally and forward a signed result to FWD_TARGET
LOCALIMG=${LOCALIMG:-0}   # 1 (needs PREVALIDATE=1) = europe1/america1 keep their users' image bytes (fsync), asia1 gets metadata only; PG gets bytes async
FWD_GET=${FWD_GET:-0}   # 1 = also forward GET /api/ (except images, notifications) to asia1
FRONT=${FRONT:-0}   # 1 = Go listens on :80 itself (webapp/go/frontproxy.go) and nginx is stopped+disabled
FRONT_LOG=${FRONT_LOG-}   # e.g. /home/isucon/front-access.log: nginx "timed" access log from the Go front (default off)
region_of() { case $1 in asia1) echo asia;; europe1) echo europe;; america1) echo america;; esac; }
for h in "${HOSTS[@]}"; do
  (
    scp -q "$OUT/isureal" "$h:/tmp/isureal.new"
    scp -q etc/app/nginx.conf "$h:/tmp/nginx.conf"
    scp -q etc/systemd/isureal-go.service.d/20-stop.conf "$h:/tmp/20-stop.conf"
    scp -q etc/systemd/isureal-go.service.d/10-tune.conf "$h:/tmp/10-tune.conf"
    scp -q etc/systemd/isureal-go.service.d/30-front.conf "$h:/tmp/30-front.conf"
    r=$(region_of "$h"); f=$FWD; g=$FWD_GET; pv=$PREVALIDATE; pvt="$FWD_TARGET:8080"; ht=127.0.0.1; [[ $h == asia1 ]] && f=0 && g=0 && pvt="" && ht=${HEAVY_TARGET-192.168.4.10}; [[ $f == 1 ]] || pvt=""
    sed -e "s/__REGION__/$r/" -e "s/__FWD__/$f/" -e "s/__FWD_GET__/$g/" -e "s/__FWD_SECRET__/$SECRET/" -e "s/__FWD_TARGET__/$FWD_TARGET/" -e "s/__PREVALIDATE__/$pv/" -e "s/__HEAVY_TARGET__/$ht/" etc/app/isureal.conf >"$OUT/isureal.$h.conf"
    scp -q "$OUT/isureal.$h.conf" "$h:/tmp/isureal.conf"
    "${SSH[@]}" "$h" 'set -e
      sudo install -m 644 /tmp/nginx.conf /etc/nginx/nginx.conf
      sudo install -m 644 /tmp/isureal.conf /etc/nginx/sites-available/isureal.conf
    grep -q "^ISUREAL_FWD_SECRET=" /home/isucon/env.sh && sed -i "s|^ISUREAL_FWD_SECRET=.*|ISUREAL_FWD_SECRET='"$SECRET"'|" /home/isucon/env.sh || echo "ISUREAL_FWD_SECRET='"$SECRET"'" >> /home/isucon/env.sh
      setenv() { grep -q "^$1=" /home/isucon/env.sh && sed -i "s|^$1=.*|$1=$2|" /home/isucon/env.sh || echo "$1=$2" >> /home/isucon/env.sh; }
      setenv ISUREAL_DB_READ_HOST '"${READ_HOST:-192.168.4.10}"'
      setenv ISUREAL_NODE_NAME '"$h"'
      setenv ISUREAL_PPROF_ADDR 127.0.0.1:6060
      setenv ISUREAL_MEM '"$MEM"'
      setenv ISUREAL_PREVALIDATE '"$pv"'
      setenv ISUREAL_PREVALIDATE_TARGET '"$pvt"'
      setenv ISUREAL_LOCALIMG '"$LOCALIMG"'
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_DB_MAX_OPEN 60; else setenv ISUREAL_DB_MAX_OPEN 25; fi
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_JPEG_REMOTES '"${JPEG_REMOTES-192.168.4.10:8080/2/400,192.168.2.10:8080/8/1200,192.168.3.10:8080/8/1200}"'; else setenv ISUREAL_JPEG_REMOTES ""; fi
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_MEM_PEERS 192.168.2.10:8080,192.168.3.10:8080; setenv ISUREAL_MEM_PUMP ""; else setenv ISUREAL_MEM_PEERS ""; setenv ISUREAL_MEM_PUMP 192.168.1.10:8080; fi
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_IMG_PEERS '"${IMG_PEERS-192.168.2.10:8080,192.168.3.10:8080}"'; else setenv ISUREAL_IMG_PEERS ""; fi
      setenv ISUREAL_FRONT '"$FRONT"'
      setenv ISUREAL_FRONT_FWD '"$f"'
      setenv ISUREAL_FRONT_FWD_GET '"$g"'
      setenv ISUREAL_FRONT_FWD_TARGET '"$FWD_TARGET"':8080
      if [ "'"$ht"'" = 127.0.0.1 ] || [ -z "'"$ht"'" ]; then setenv ISUREAL_FRONT_HEAVY_TARGET ""; else setenv ISUREAL_FRONT_HEAVY_TARGET '"$ht"':8080; fi
      setenv ISUREAL_FRONT_LOG "'"$FRONT_LOG"'"
      sudo nginx -t -q
      sudo install -D -m 644 /tmp/20-stop.conf /etc/systemd/system/isureal-go.service.d/20-stop.conf
      sudo install -D -m 644 /tmp/10-tune.conf /etc/systemd/system/isureal-go.service.d/10-tune.conf
      sudo install -D -m 644 /tmp/30-front.conf /etc/systemd/system/isureal-go.service.d/30-front.conf && sudo systemctl daemon-reload
      install -m 755 /tmp/isureal.new /home/isucon/webapp/go/isureal
      sudo systemctl restart isureal-go
      if [ "'"$FRONT"'" = 1 ]; then
        # the Go front retries bind(:80) every 200ms, so it takes over as soon as nginx is gone
        sudo systemctl disable -q --now nginx
        for i in $(seq 50); do c=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1/); [ "$c" = 200 ] && break; sleep 0.2; done; echo "front :80 -> $c"
      else
        sudo systemctl enable -q nginx
        if systemctl is-active -q nginx; then sudo systemctl reload nginx; else sudo systemctl start nginx; fi
      fi
      for i in $(seq 100); do c=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/api/posting-window/current); [ "$c" != 000 ] && break; sleep 0.2; done
      sha1sum /home/isucon/webapp/go/isureal | cut -c1-12' </dev/null | sed "s/^/$h: /"
  ) &
done
if true; then
  # app on asia2 (same host as PostgreSQL): JPEG validation offload for asia1, and the
  # forward target when FWD_TARGET=192.168.4.10; no nginx there
  scp -q "$OUT/isureal" asia2:/tmp/isureal.new
  scp -q etc/systemd/isureal-go.service.d/20-stop.conf asia2:/tmp/20-stop.conf
  "${SSH[@]}" asia2 'set -e
    grep -q "^ISUREAL_DB_MAX_OPEN=" /home/isucon/env.sh && sed -i "s|^ISUREAL_DB_MAX_OPEN=.*|ISUREAL_DB_MAX_OPEN=30|" /home/isucon/env.sh || echo "ISUREAL_DB_MAX_OPEN=30" >> /home/isucon/env.sh
    grep -q "^ISUREAL_FWD_SECRET=" /home/isucon/env.sh && sed -i "s|^ISUREAL_FWD_SECRET=.*|ISUREAL_FWD_SECRET='"$SECRET"'|" /home/isucon/env.sh || echo "ISUREAL_FWD_SECRET='"$SECRET"'" >> /home/isucon/env.sh
    grep -q "^ISUREAL_LOCALIMG=" /home/isucon/env.sh && sed -i "s|^ISUREAL_LOCALIMG=.*|ISUREAL_LOCALIMG='"$LOCALIMG"'|" /home/isucon/env.sh || echo "ISUREAL_LOCALIMG='"$LOCALIMG"'" >> /home/isucon/env.sh
    grep -q "^ISUREAL_PREVALIDATE=" /home/isucon/env.sh && sed -i "s|^ISUREAL_PREVALIDATE=.*|ISUREAL_PREVALIDATE='"$PREVALIDATE"'|" /home/isucon/env.sh || echo "ISUREAL_PREVALIDATE='"$PREVALIDATE"'" >> /home/isucon/env.sh
    sudo install -D -m 644 /tmp/20-stop.conf /etc/systemd/system/isureal-go.service.d/20-stop.conf && sudo systemctl daemon-reload
    install -m 755 /tmp/isureal.new /home/isucon/webapp/go/isureal
    sudo systemctl enable isureal-go >/dev/null 2>&1; sudo systemctl restart isureal-go
    for i in $(seq 100); do c=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/api/posting-window/current); [ "$c" != 000 ] && break; sleep 0.2; done
    sha1sum /home/isucon/webapp/go/isureal | cut -c1-12' </dev/null | sed "s/^/asia2: /" &
else
  "${SSH[@]}" asia2 'systemctl is-enabled isureal-go >/dev/null 2>&1 && { sudo systemctl disable --now isureal-go; echo "asia2: app disabled"; } || true' </dev/null &
fi
wait
rm -rf "$OUT"
