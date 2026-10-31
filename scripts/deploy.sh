#!/usr/bin/env bash
# usage: scripts/deploy.sh [hosts...]   (default: asia1 europe1 america1)
# Builds webapp/go locally and deploys binary + nginx conf to app hosts, restarts services.
# Host state owned here: /home/isucon/webapp/go/isureal, /etc/nginx/nginx.conf, /etc/nginx/sites-available/isureal.conf
# (+ via scripts/pace.sh on asia1/europe1/america1 every run: root qdisc, per-/24 routes, isucon-pace.service)
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/isucon
if [[ -z ${LOCK_HELD:-} ]]; then exec 9>>/tmp/isucon/env.lock; flock -w 600 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner 2>/dev/null)" >&2; exit 75; }; echo "$(date +%T) deploy $(git rev-parse --short HEAD)" >| /tmp/isucon/env.lock.owner; fi
HOSTS=("$@"); [[ ${#HOSTS[@]} -gt 0 ]] || HOSTS=(asia1 europe1 america1)
OUT=$(mktemp -d)
( cd webapp/go && GOOS=linux GOARCH=amd64 GOAMD64=v3 CGO_ENABLED=0 go build -o "$OUT/isureal" ./cmd/isureal )
JC=${JC:-1}   # 1 = Rust jpegcheckd (webapp/rust/jpegcheckd) does the local JPEG check on every app host; 0 = in-process Go
JC_SOCK=""
if [[ $JC == 1 ]]; then
  # +nightly is explicit: RUSTUP_TOOLCHAIN in the env overrides rust-toolchain.toml
  ( cd webapp/rust/jpegcheckd && cargo +nightly build --release -q )
  cp webapp/rust/jpegcheckd/target/x86_64-unknown-linux-musl/release/jpegcheckd "$OUT/jpegcheckd"
  JC_SOCK=/run/isureal/jpegcheck.sock
  JC_REMOTE='mkdir -p /home/isucon/webapp/rust && install -m 755 /tmp/jpegcheckd.new /home/isucon/webapp/rust/jpegcheckd
      sudo install -m 644 /tmp/isureal-jpegcheckd.service /etc/systemd/system/isureal-jpegcheckd.service && sudo systemctl daemon-reload
      sudo systemctl enable -q isureal-jpegcheckd; sudo systemctl restart isureal-jpegcheckd'
else
  JC_REMOTE='sudo systemctl disable -q --now isureal-jpegcheckd 2>/dev/null || true'
fi
# runs on each app host before isureal-go is restarted
JC_REMOTE=$JC_REMOTE'
      grep -q "^ISUREAL_JPEG_LOCAL_SOCK=" /home/isucon/env.sh && sed -i "s|^ISUREAL_JPEG_LOCAL_SOCK=.*|ISUREAL_JPEG_LOCAL_SOCK='"$JC_SOCK"'|" /home/isucon/env.sh || echo "ISUREAL_JPEG_LOCAL_SOCK='"$JC_SOCK"'" >> /home/isucon/env.sh'
jc_scp() { [[ $JC == 1 ]] || return 0; scp -q "$OUT/jpegcheckd" "$1:/tmp/jpegcheckd.new"; scp -q etc/systemd/isureal-jpegcheckd.service "$1:/tmp/isureal-jpegcheckd.service"; }
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10)
SECRET_FILE=${FWD_SECRET_FILE:-$HOME/.config/isucon2026/fwd_secret}
if [[ ! -s $SECRET_FILE ]]; then mkdir -p "${SECRET_FILE%/*}"; head -c 24 /dev/urandom | base64 | tr -d '/+=' >| "$SECRET_FILE"; chmod 600 "$SECRET_FILE"; fi
SECRET=$(cat "$SECRET_FILE")
FWD=${FWD:-1}   # 1 = europe1/america1 forward non-GET /api/ to asia1
FWD_TARGET=${FWD_TARGET:-192.168.1.10}   # forward target app (asia1=192.168.1.10, asia2=192.168.4.10)
MEM=${MEM:-1}   # 1 = in-memory read model (asia1 pump -> europe1/america1 peers)
MEM_WM=${MEM_WM:-1}   # 1 = writes skip peer acks; europe1/america1 reads wait for the pump's watermark (all 3 hosts)
MEM_WM_HB=${MEM_WM_HB:-0}   # 1 (needs MEM_WM=1) = shorter wm waits: 10ms poll, 16 in-flight pushes to wm peers, wm rides the data push (webapp/go/mem_watermark.go); MEM_WM_SLACK_MS (default 0) loosens the read target
MEM_WM_EARLY=${MEM_WM_EARLY:-0}   # 1 (needs MEM_WM=1) = asia1 pushes a poll's batch + wm to eu/am before applying it locally (cuts asia1 apply time from the eu/am wm wait). Separate knobs, empty/0 = built-in default: MEM_WM_TICK_MS (idle poll period, 20; 10 with HB), MEM_PUSH_INFLIGHT (data pushes per wm peer, 4; 16 with HB), MEM_WM_INFLIGHT (wm-only posts per peer, 8; 16 with HB), MEM_IDLE_CONNS (keep-alive conns per peer, 16; 64 with HB: keep >= push + wm + 2)
MEM_APPLY_CHUNK=${MEM_APPLY_CHUNK:-0}   # N>0 (all hosts, same value) = asia1 tags changes with their xid; eu/am apply a push batch whole-tx-at-a-time, releasing the memory write lock every ~N changes (never inside a tx; wm/seq advance only after the whole batch) (webapp/go/mem_apply_chunk.go)
ASIA_POST_CAP=${ASIA_POST_CAP:-0}   # N>0 (asia1 only) = during GP at most N asia-local POST /api/posts run at once (forwarded eu/am posts never wait), so the pump's push/poll goroutines are not queued behind them on 2 CPUs (webapp/go/gp_postcap.go). ASIA_POST_CAP_WAIT_MS (3000, then run uncapped), ASIA_POST_CAP_LAG_MS (0 = always in GP; >0 = only while the last push RTT exceeds it)
# IPBAN_ASYNC=1: ip_bans reload in a 200ms background loop, requests never wait for it (webapp/go/ip_ban.go). MEM_WM_MAXWAIT_MS (default 300): peer GET waits up to this for the wm before falling back to the primary (1 RTT per query from eu/am). DBTRACE=1: every 10s slog the SQL this app sent, per pool + 80-char prefix (webapp/go/dbtrace.go)
MEM_PUSH_STATS=${MEM_PUSH_STATS:-0}   # 1 = 10s slog "mem: push stats" (pump: bytes/batches/ack; peer: net/lock/apply/pulls) (webapp/go/mem_push_stats.go)
# MEM_PULL_UNLOCKED=1 (eu/am receivers) = release the receiver lock during a gap pull so other pushes / wm keep applying (webapp/go/mem_peers.go fillGap)
MEM_PUSH_GZIP=${MEM_PUSH_GZIP:-0}   # 1 = asia1 gzips push bodies >= 1KiB (receivers always accept gzip; deploy all hosts together)
LOGREPL=${LOGREPL:-0}   # 1 = app hosts subscribe to PG logical replication (webapp/go/mem_logrepl.go) instead of the asia1 pump
PREVALIDATE=${PREVALIDATE:-1}   # 1 = europe1/america1 check JPEGs locally and forward a signed result to FWD_TARGET
MEMORIES_PAR=${MEMORIES_PAR:-1}   # 1 = Memories run posts/RealMojis/comments queries at once (1 DB round trip instead of 3)
MEMORIES_MEM=${MEMORIES_MEM:-1}   # 1 (needs MEM=1) = Memories for periods inside the in-memory hot range come from memory (webapp/go/mem_memories.go)
MEMORIES_CACHE=${MEMORIES_CACHE:-1}   # 1 (needs MEM=1) = Memories/export DB-path answers cached per viewer, dropped by the mem change stream (webapp/go/mem_respcache.go). For asia also set HEAVY_TARGET=127.0.0.1 (asia2 has no mem model)
# MEMORIES_CACHE_ORIGIN (asia1 only, with MEMORIES_CACHE=1 HEAVY_TARGET=127.0.0.1): e.g. 192.168.4.10:8080 = cache misses are computed by the asia2 app (webapp/go/mem_respcache_origin.go)
# MEMORIES_CACHE_ORIGIN_PEERS (europe1/america1, default empty = off): e.g. 192.168.4.10:8080 = their Memories/export cache misses are computed by the asia2 app too (1 HTTP RTT instead of 2+ DB RTTs; request carries the fwd region headers)
# MEMORIES_TARGET (asia1 only): where nginx sends /api/memories; default = HEAVY_TARGET. 127.0.0.1 = asia1's own app (memory path)
HEAVY_LIMIT=${HEAVY_LIMIT:-0}   # N>0 = at most N heavy DB reads (export, Memories/search DB path) at once per app (webapp/go/heavy_limit.go)
LOCALIMG=${LOCALIMG:-1}   # 1 (needs PREVALIDATE=1) = europe1/america1 keep their users' image bytes (fsync), asia1 gets metadata only; PG gets bytes async
ASIA_LOCALIMG=${ASIA_LOCALIMG:-1}   # 1 = asia1 keeps the bytes of images it commits (fsync, /home/isucon/asialocalimg); no image_objects row; other hosts pull from asia1 on DB miss (webapp/go/localimg.go). Turning it off again needs /api/initialize (old images are only on asia1)
LOCAL_SESSION=${LOCAL_SESSION:-0}   # 1 = europe1/america1 commit login/logout in-region (webapp/go/local_session.go); needs MEM=1 MEM_WM=1 LOGREPL=0 STORE=0
STORE=${STORE:-0}   # 1 = asia1 memory-primary write path (NOTES-store.md); requires MEM=1
FWD_GET=${FWD_GET:-0}   # 1 = also forward GET /api/ (except images, notifications) to asia1
# ASIA_LOCALIMG_FAST (all hosts, default 0): 1 = eu/am/asia2 pull asia1-tagged images from asia1 before PG (skips the replica + cross-region primary miss, ~1 RTT); asia1 answers pulls from its memory LRU first (webapp/go/localimg.go)
IMG_PUSH_FILTER=${IMG_PUSH_FILTER:-0}   # 1 = asia1 pushes post/RealMoji images only to regions with a likely viewer (author's friends; public = all); others pull from PG
FWD_PRIORITY=${FWD_PRIORITY:-1}   # 1 = asia1 runs forwarded (eu/am) writes first; local writes go through a semaphore (webapp/go/priority.go)
IMG_DEDUP=${IMG_DEDUP:-0}   # 1 = image bytes cross asia->eu/am once: push asks the peer first (/have), viewers wait for an in-flight push, initialize purges eu/am image caches, 10s slog "imgcache" (webapp/go/imgdedup.go; set on asia1 = sender and europe1/america1 = receivers)
FRONT=${FRONT:-0}   # 1 = Go listens on :80 itself (webapp/go/frontproxy.go) and nginx is stopped+disabled
PACE=${PACE:-0}   # 1 = asia1 shapes egress to europe/america /24s at PACE_RATE (130mbit) with small-packet priority (scripts/pace.sh); 0 = qdisc removed
CWND=${CWND:-0}   # 1 = per-region /24 routes with initcwnd 64 initrwnd 64 + tcp_slow_start_after_idle=0 on all app hosts; 0 = removed
ASIA2_GOMAXPROCS=${ASIA2_GOMAXPROCS:-1}   # asia2 app (JPEG offload / forwarded work) GOMAXPROCS; 1 = leave the 2nd vCPU to PostgreSQL; "" = Go default (2)
DROP_DUP_IDX=${DROP_DUP_IDX:-0}   # 1 = drop redundant indexes on asia2 (posting_windows_active = posting_windows_one_active_region; posts_author_id = prefix of posts_author_posted) so the post tx fits 16 fast-path locks
PGCPU=${PGCPU:-0}   # 1 = install etc/pg/primary-cpu.conf as asia2 conf.d/zz-cpu.conf (max_wal_size 12GB, jit off, ...) + pg_reload_conf; 0 = remove it
IMG_HOTPART=${IMG_HOTPART:-1}   # 1 = new image IDs get a d0d0d0 tag; tagged IDs are read from the contest partition only (webapp/go/image_hotpart.go); 0 = old all-partition lookup
PGFPW=${PGFPW:-0}   # 1 = install etc/pg/primary-fpw-off.conf (full_page_writes=off; torn-page corruption risk on a crash) ; 0 = remove it
FRONT_LOG=${FRONT_LOG-}   # e.g. /home/isucon/front-access.log: nginx "timed" access log from the Go front (default off)
region_of() { case $1 in asia1) echo asia;; europe1) echo europe;; america1) echo america;; esac; }
for h in "${HOSTS[@]}"; do
  (
    scp -q "$OUT/isureal" "$h:/tmp/isureal.new"
    scp -q etc/app/nginx.conf "$h:/tmp/nginx.conf"
    scp -q etc/systemd/isureal-go.service.d/20-stop.conf "$h:/tmp/20-stop.conf"
    scp -q etc/systemd/isureal-go.service.d/10-tune.conf "$h:/tmp/10-tune.conf"
    scp -q etc/systemd/isureal-go.service.d/30-front.conf "$h:/tmp/30-front.conf"
    jc_scp "$h"
    r=$(region_of "$h"); fr=0; [[ $h == asia1 ]] && fr=$FRONT; f=$FWD; g=$FWD_GET; pv=$PREVALIDATE; pvt="$FWD_TARGET:8080"; ht=127.0.0.1; [[ $h == asia1 ]] && f=0 && g=0 && pvt="" && ht=${HEAVY_TARGET-127.0.0.1}; [[ $f == 1 ]] || pvt=""; mt=$ht; [[ $h == asia1 ]] && mt=${MEMORIES_TARGET-$ht}
    ls=$LOCAL_SESSION; { [[ $h == asia1 || $f != 1 || $MEM != 1 || $MEM_WM != 1 || $LOGREPL == 1 || $STORE == 1 ]]; } && ls=0
    sed -e "s/__MEMORIES_TARGET__/$mt/" -e "s/__REGION__/$r/" -e "s/__FWD__/$f/" -e "s/__FWD_GET__/$g/" -e "s/__FWD_SECRET__/$SECRET/" -e "s/__FWD_TARGET__/$FWD_TARGET/" -e "s/__PREVALIDATE__/$pv/" -e "s/__HEAVY_TARGET__/$ht/" -e "s/__LOCAL_SESSION__/$ls/" etc/app/isureal.conf >"$OUT/isureal.$h.conf"
    scp -q "$OUT/isureal.$h.conf" "$h:/tmp/isureal.conf"
    "${SSH[@]}" "$h" 'set -e
      sudo install -m 644 /tmp/nginx.conf /etc/nginx/nginx.conf
      sudo install -m 644 /tmp/isureal.conf /etc/nginx/sites-available/isureal.conf
    grep -q "^ISUREAL_FWD_SECRET=" /home/isucon/env.sh && sed -i "s|^ISUREAL_FWD_SECRET=.*|ISUREAL_FWD_SECRET='"$SECRET"'|" /home/isucon/env.sh || echo "ISUREAL_FWD_SECRET='"$SECRET"'" >> /home/isucon/env.sh
      setenv() { grep -q "^$1=" /home/isucon/env.sh && sed -i "s|^$1=.*|$1=$2|" /home/isucon/env.sh || echo "$1=$2" >> /home/isucon/env.sh; }
      setenv ISUREAL_DB_READ_HOST '"${READ_HOST:-192.168.4.10}"'
      setenv ISUREAL_NODE_NAME '"$h"'
      setenv ISUREAL_IMG_HOTPART '"$IMG_HOTPART"'
      setenv PGSSLMODE '"${PGSSL:-allow}"'
      setenv ISUREAL_PPROF_ADDR '"${PPROF-127.0.0.1:6060}"'
      setenv ISUREAL_MEM '"$MEM"'
      setenv ISUREAL_MEM_WATERMARK '"$MEM_WM"'
      setenv ISUREAL_MEM_WM_HB '"$MEM_WM_HB"'
      setenv ISUREAL_MEM_WM_EARLY '"$MEM_WM_EARLY"'
      setenv ISUREAL_MEM_APPLY_CHUNK '"$MEM_APPLY_CHUNK"'
      if [ "'"$h"'" = asia2 ]; then setenv ISUREAL_COLD_ORIGIN ''; else setenv ISUREAL_COLD_ORIGIN '"${COLD_ORIGIN:-}"'; fi   # e.g. 192.168.4.10:8080: PG fallbacks of public GETs go to asia2 (cold_origin.go)
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_ASIA_POST_CAP '"$ASIA_POST_CAP"'; else setenv ISUREAL_ASIA_POST_CAP 0; fi
      setenv ISUREAL_ASIA_POST_CAP_WAIT_MS '"${ASIA_POST_CAP_WAIT_MS:-3000}"'
      setenv ISUREAL_ASIA_POST_CAP_LAG_MS '"${ASIA_POST_CAP_LAG_MS:-0}"'
      setenv ISUREAL_MEM_WM_TICK_MS '"${MEM_WM_TICK_MS:-0}"'
      setenv ISUREAL_MEM_PUSH_INFLIGHT '"${MEM_PUSH_INFLIGHT:-0}"'
      setenv ISUREAL_MEM_WM_INFLIGHT '"${MEM_WM_INFLIGHT:-0}"'
      setenv ISUREAL_MEM_IDLE_CONNS '"${MEM_IDLE_CONNS:-0}"'
      setenv ISUREAL_MEM_WM_SLACK_MS '"${MEM_WM_SLACK_MS:-0}"'
      setenv ISUREAL_MEM_WM_MAXWAIT_MS '"${MEM_WM_MAXWAIT_MS:-300}"'
      setenv ISUREAL_DBTRACE '"${DBTRACE:-0}"'
      setenv ISUREAL_IPBAN_ASYNC '"${IPBAN_ASYNC:-1}"'
      setenv ISUREAL_MEM_PUSH_STATS '"$MEM_PUSH_STATS"'
      setenv ISUREAL_MEM_PUSH_GZIP '"$MEM_PUSH_GZIP"'
      setenv ISUREAL_MEM_REORDER_WAIT_MS '"${MEM_REORDER_WAIT_MS:-50}"'
      setenv ISUREAL_MEM_PULL_UNLOCKED '"${MEM_PULL_UNLOCKED:-0}"'
      setenv ISUREAL_DB_KEEP_IDLE '"${DB_KEEP_IDLE:-0}"'
      setenv ISUREAL_POST_BATCH '"${POST_BATCH:-1}"'
      setenv ISUREAL_GP_FAIR '"${GP_FAIR:-0}"'
      setenv ISUREAL_GP_FAIR_SLOTS '"${GP_FAIR_SLOTS:-32}"'
      setenv ISUREAL_GP_FAIR_FWD_LEAD_MS '"${GP_FAIR_FWD_LEAD_MS:-1000}"'
      setenv ISUREAL_GP_FAIR_POST_EXEMPT '"${GP_FAIR_POST_EXEMPT:-0}"'
      setenv ISUREAL_FRIEND_BATCH '"${FRIEND_BATCH:-1}"'
      setenv ISUREAL_MEM_LOGREPL '"$LOGREPL"'
      setenv ISUREAL_LOCAL_ONLY_WRITE '"${LOCAL_ONLY_WRITE:-1}"'
      setenv ISUREAL_SIGN_SIMD_MIN '"${SIGN_SIMD_MIN:-}"'
      setenv ISUREAL_DB_IMG_OPEN '"${DB_IMG_OPEN:-12}"'
      setenv ISUREAL_IMG_PREEXTEND_MB '"${IMG_PREEXTEND_MB:-0}"'
      setenv ISUREAL_EXPORT_REBUILD '"${EXPORT_REBUILD:-0}"'
      setenv ISUREAL_EXPORT_CACHE_MB '"${EXPORT_CACHE_MB:-0}"'
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_IMAGE_MEM_MB 300; else setenv ISUREAL_IMAGE_MEM_MB '"${PEER_IMAGE_MEM_MB:-300}"'; fi
      setenv ISUREAL_DB_IMG_READ_OPEN '"${DB_IMG_READ_OPEN:-16}"'
      setenv ISUREAL_POST_SLOT_EXEMPT '"${POST_SLOT_EXEMPT:-0}"'
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_IMAGE_CACHE_DIR '"${ASIA1_IMG_DISK:--}"'; else setenv ISUREAL_IMAGE_CACHE_DIR /home/isucon/imgcache; fi
      setenv ISUREAL_ASIA_LOCALIMG_SYNC '"${ASIA_LOCALIMG_SYNC:-}"'
      setenv ISUREAL_ASIA_LOCALIMG_FAST '"${ASIA_LOCALIMG_FAST:-0}"'
      setenv ISUREAL_IMGV_PEER_INDEX '"${IMGV_PEER_INDEX:-1}"'
      setenv ISUREAL_IMG_SENDFILE '"${IMG_SENDFILE:-1}"'
      setenv ISUREAL_PREVALIDATE '"$pv"'
      setenv ISUREAL_PREVALIDATE_TARGET '"$pvt"'
      setenv ISUREAL_LOCALIMG '"$LOCALIMG"'
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_ASIA_LOCALIMG '"$ASIA_LOCALIMG"'; else setenv ISUREAL_ASIA_LOCALIMG 0; fi
      setenv ISUREAL_LOCAL_SESSION '"$ls"'
      setenv ISUREAL_MEMORIES_PAR '"$MEMORIES_PAR"'
      setenv ISUREAL_IMG_DEDUP '"$IMG_DEDUP"'
      setenv ISUREAL_MEMORIES_MEM '"$MEMORIES_MEM"'
      setenv ISUREAL_MEMORIES_CACHE '"$MEMORIES_CACHE"'
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_MEMORIES_CACHE_ORIGIN '"${MEMORIES_CACHE_ORIGIN-192.168.4.10:8080}"'; else setenv ISUREAL_MEMORIES_CACHE_ORIGIN '"${MEMORIES_CACHE_ORIGIN_PEERS-}"'; fi
      setenv ISUREAL_HEAVY_LIMIT '"$HEAVY_LIMIT"'
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_DB_MAX_OPEN 60; else setenv ISUREAL_DB_MAX_OPEN 25; fi
      setenv ISUREAL_JPEG_LOCAL_SLOTS '"${JPEG_LOCAL_SLOTS:-2}"'
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_JPEG_REMOTES '"${JPEG_REMOTES-192.168.4.10:8080/2/400,192.168.2.10:8080/8/1200,192.168.3.10:8080/8/1200}"'; else setenv ISUREAL_JPEG_REMOTES ""; fi
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_MEM_PEERS 192.168.2.10:8080,192.168.3.10:8080; setenv ISUREAL_MEM_PUMP ""; else setenv ISUREAL_MEM_PEERS ""; setenv ISUREAL_MEM_PUMP 192.168.1.10:8080; fi
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_STORE '"$STORE"'; else setenv ISUREAL_STORE 0; fi
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_IMG_PEERS '"${IMG_PEERS-}"'; else setenv ISUREAL_IMG_PEERS ""; fi
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_IMG_PUSH_FILTER '"$IMG_PUSH_FILTER"'; else setenv ISUREAL_IMG_PUSH_FILTER 0; fi
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_FWD_PRIORITY_LOCAL_SLOTS '"${FWD_LOCAL_SLOTS:-128}"'; fi
      setenv ISUREAL_FWD_PRIORITY_POST_SLOTS '"${FWD_POST_SLOTS:-0}"'   # N>0: asia-local POST /api/posts use their own N slots, so login/friend-requests do not wait behind slow posts (priority.go)
      setenv ISUREAL_POST_TIMING '"${POST_TIMING:-0}"'   # 1: 10s slog "post timing" (jpeg / localimg(_sync) / apply / pgflush / ack)
      setenv ISUREAL_ASIA_LOCALIMG_WB '"${ASIA_LOCALIMG_WB:-0}"'   # 1: start writeback at write so the group syncfs is shorter (localimg_groupsync.go)
      setenv ISUREAL_ASIA_LOCALIMG_FSYNC '"${ASIA_LOCALIMG_FSYNC:-0}"'   # 1: per-writer parallel fdatasync of data+meta + parent dir fsync instead of the whole-fs syncfs; gp: only while the asia1 window is a GP (gpWindowDeadline), group syncfs otherwise (localimg_fsync.go)
      if [ "'"$h"'" = asia1 ]; then setenv ISUREAL_FWD_PRIORITY '"$FWD_PRIORITY"'; else setenv ISUREAL_FWD_PRIORITY 0; fi
      setenv ISUREAL_FRONT '"$fr"'
      setenv ISUREAL_FRONT_FWD '"$f"'
      setenv ISUREAL_FRONT_FWD_GET '"$g"'
      setenv ISUREAL_FRONT_FWD_TARGET '"$FWD_TARGET"':8080
      if [ "'"$ht"'" = 127.0.0.1 ] || [ -z "'"$ht"'" ]; then setenv ISUREAL_FRONT_HEAVY_TARGET ""; else setenv ISUREAL_FRONT_HEAVY_TARGET '"$ht"':8080; fi
      setenv ISUREAL_FRONT_LOG "'"$FRONT_LOG"'"
      sudo nginx -t -q
      sudo install -D -m 644 /tmp/20-stop.conf /etc/systemd/system/isureal-go.service.d/20-stop.conf
      sudo install -D -m 644 /tmp/10-tune.conf /etc/systemd/system/isureal-go.service.d/10-tune.conf
      sudo install -D -m 644 /tmp/30-front.conf /etc/systemd/system/isureal-go.service.d/30-front.conf && sudo systemctl daemon-reload
      '"$JC_REMOTE"'
      install -m 755 /tmp/isureal.new /home/isucon/webapp/go/isureal
      sudo systemctl restart isureal-go
      if [ "'"$fr"'" = 1 ]; then
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
  scp -q etc/systemd/isureal-go.service.d/40-oom-asia2.conf asia2:/tmp/40-oom.conf
  scp -q etc/systemd/postgresql.service.d/50-oom.conf asia2:/tmp/50-pgoom.conf
  jc_scp asia2
  scp -q etc/pg/primary-cpu.conf asia2:/tmp/zz-cpu.conf
  scp -q etc/pg/primary-fpw-off.conf asia2:/tmp/zz-fpw.conf
  "${SSH[@]}" asia2 'set -e
    pgconf() { if [ "$2" = 1 ]; then sudo install -m 644 -o postgres -g postgres "/tmp/$1" "/etc/postgresql/16/main/conf.d/$1"; else sudo rm -f "/etc/postgresql/16/main/conf.d/$1"; fi; }
    grep -q "^ISUREAL_IMG_HOTPART=" /home/isucon/env.sh && sed -i "s|^ISUREAL_IMG_HOTPART=.*|ISUREAL_IMG_HOTPART='"$IMG_HOTPART"'|" /home/isucon/env.sh || echo "ISUREAL_IMG_HOTPART='"$IMG_HOTPART"'" >> /home/isucon/env.sh
    pgconf zz-cpu.conf '"$PGCPU"'; pgconf zz-fpw.conf '"$PGFPW"'
    sudo -u postgres psql -XAtqc "select pg_reload_conf()" >/dev/null
    if [ "'"$DROP_DUP_IDX"'" = 1 ]; then sudo -u postgres psql -d isureal -XAtq -c "DROP INDEX CONCURRENTLY IF EXISTS posting_windows_active" -c "DROP INDEX CONCURRENTLY IF EXISTS posts_author_id"; fi
    if [ -n "'"$ASIA2_GOMAXPROCS"'" ]; then grep -q "^GOMAXPROCS=" /home/isucon/env.sh && sed -i "s|^GOMAXPROCS=.*|GOMAXPROCS='"$ASIA2_GOMAXPROCS"'|" /home/isucon/env.sh || echo "GOMAXPROCS='"$ASIA2_GOMAXPROCS"'" >> /home/isucon/env.sh; else sed -i "/^GOMAXPROCS=/d" /home/isucon/env.sh; fi
    grep -q "^ISUREAL_DB_MAX_OPEN=" /home/isucon/env.sh && sed -i "s|^ISUREAL_DB_MAX_OPEN=.*|ISUREAL_DB_MAX_OPEN=30|" /home/isucon/env.sh || echo "ISUREAL_DB_MAX_OPEN=30" >> /home/isucon/env.sh
    grep -q "^PGSSLMODE=" /home/isucon/env.sh && sed -i "s|^PGSSLMODE=.*|PGSSLMODE='"${PGSSL:-allow}"'|" /home/isucon/env.sh || echo "PGSSLMODE='"${PGSSL:-allow}"'" >> /home/isucon/env.sh
    grep -q "^ISUREAL_FWD_SECRET=" /home/isucon/env.sh && sed -i "s|^ISUREAL_FWD_SECRET=.*|ISUREAL_FWD_SECRET='"$SECRET"'|" /home/isucon/env.sh || echo "ISUREAL_FWD_SECRET='"$SECRET"'" >> /home/isucon/env.sh
    grep -q "^ISUREAL_MEMORIES_PAR=" /home/isucon/env.sh && sed -i "s|^ISUREAL_MEMORIES_PAR=.*|ISUREAL_MEMORIES_PAR='"$MEMORIES_PAR"'|" /home/isucon/env.sh || echo "ISUREAL_MEMORIES_PAR='"$MEMORIES_PAR"'" >> /home/isucon/env.sh
    grep -q "^ISUREAL_HEAVY_LIMIT=" /home/isucon/env.sh && sed -i "s|^ISUREAL_HEAVY_LIMIT=.*|ISUREAL_HEAVY_LIMIT='"$HEAVY_LIMIT"'|" /home/isucon/env.sh || echo "ISUREAL_HEAVY_LIMIT='"$HEAVY_LIMIT"'" >> /home/isucon/env.sh
    grep -q "^ISUREAL_LOCALIMG=" /home/isucon/env.sh && sed -i "s|^ISUREAL_LOCALIMG=.*|ISUREAL_LOCALIMG='"$LOCALIMG"'|" /home/isucon/env.sh || echo "ISUREAL_LOCALIMG='"$LOCALIMG"'" >> /home/isucon/env.sh
    grep -q "^ISUREAL_ASIA_LOCALIMG=" /home/isucon/env.sh && sed -i "s|^ISUREAL_ASIA_LOCALIMG=.*|ISUREAL_ASIA_LOCALIMG=0|" /home/isucon/env.sh || echo "ISUREAL_ASIA_LOCALIMG=0" >> /home/isucon/env.sh
    grep -q "^ISUREAL_PREVALIDATE=" /home/isucon/env.sh && sed -i "s|^ISUREAL_PREVALIDATE=.*|ISUREAL_PREVALIDATE='"$PREVALIDATE"'|" /home/isucon/env.sh || echo "ISUREAL_PREVALIDATE='"$PREVALIDATE"'" >> /home/isucon/env.sh
    sudo install -D -m 644 /tmp/20-stop.conf /etc/systemd/system/isureal-go.service.d/20-stop.conf
    sudo install -D -m 644 /tmp/40-oom.conf /etc/systemd/system/isureal-go.service.d/40-oom.conf
    sudo install -D -m 644 /tmp/50-pgoom.conf /etc/systemd/system/postgresql@16-main.service.d/50-oom.conf && sudo systemctl daemon-reload
    '"$JC_REMOTE"'
    install -m 755 /tmp/isureal.new /home/isucon/webapp/go/isureal
    sudo systemctl enable isureal-go >/dev/null 2>&1; sudo systemctl restart isureal-go
    for i in $(seq 100); do c=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/api/posting-window/current); [ "$c" != 000 ] && break; sleep 0.2; done
    sha1sum /home/isucon/webapp/go/isureal | cut -c1-12' </dev/null | sed "s/^/asia2: /" &
else
  "${SSH[@]}" asia2 'systemctl is-enabled isureal-go >/dev/null 2>&1 && { sudo systemctl disable --now isureal-go; echo "asia2: app disabled"; } || true' </dev/null &
fi
wait
# always runs on all 3 app hosts so PACE=0/CWND=0 reverts everywhere, even on a single-host deploy
LOCK_HELD=1 PACE=$PACE CWND=$CWND scripts/pace.sh asia1 europe1 america1
rm -rf "$OUT"
