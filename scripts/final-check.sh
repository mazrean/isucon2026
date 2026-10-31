#!/usr/bin/env bash
# usage: scripts/final-check.sh [hosts...]   (default: asia1 europe1 america1 asia2)
# Read-only pre/post-final check. Restarts nothing, takes no env.lock, touches no portal.
# Per host: boot_id, df, service is-active/is-enabled, env.sh (ISUREAL_* / PGSSLMODE, secrets masked),
# experiment leftovers (PACE qdisc, CWND /24 routes, isucon-pace unit, PG slots / sync / watchdogs on asia2),
# pprof / log settings, local GET codes. Last line: "OK" or "NG: reason; ..." (exit 1 on NG).
# WARN lines are things to decide on (e.g. pprof on), not failures.
set -uo pipefail
cd "$(dirname "$0")/.."
HOSTS=("$@"); [[ ${#HOSTS[@]} -gt 0 ]] || HOSTS=(asia1 europe1 america1 asia2)
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10)
DF_NG=${DF_NG:-90}   # Use% at or above this on / or /home = NG
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT

# Runs on each host as isucon: $1 = host alias, $2 = DF_NG. Prints "NG ..." / "WARN ..." for the summary.
REMOTE=$(cat <<'EOF'
h=$1; dfng=$2; walok=${3:-0}
ng() { echo "NG $h: $*"; }; warn() { echo "WARN $h: $*"; }
echo "-- boot_id $(cat /proc/sys/kernel/random/boot_id)  up since $(uptime -s)"
echo "-- df"
df -hP / /home 2>/dev/null | awk 'NR==1||!seen[$1]++'
for m in / /home; do u=$(df -P "$m" 2>/dev/null | awk 'NR==2{sub(/%/,"",$5);print $5}'); [ -n "$u" ] && [ "$u" -ge "$dfng" ] && ng "df $m ${u}%"; done
envget() { sed -n "s/^$1=//p" /home/isucon/env.sh | tail -1 | tr -d "\"'"; }
front=$(envget ISUREAL_FRONT)
echo "-- services (active enabled)"
chk() {  # chk unit want_active want_enabled(enabled|off)
  a=$(systemctl is-active "$1" 2>/dev/null); e=$(systemctl is-enabled "$1" 2>/dev/null); [ -n "$e" ] || e=not-found
  printf '  %-22s %-9s %-9s (want %s %s)\n' "$1" "$a" "$e" "$2" "$3"
  [ "$a" = "$2" ] || ng "$1 is-active=$a (want $2)"
  case "$3" in
    enabled) [ "$e" = enabled ] || ng "$1 is-enabled=$e (want enabled)";;
    off) case "$e" in enabled|enabled-runtime) ng "$1 is-enabled=$e (want disabled/absent)";; esac;;
  esac
}
chk isureal-go active enabled
case $h in
  asia2) chk nginx inactive off; chk postgresql active enabled
         chk pg-logrepl-watchdog inactive off; chk pg-sync-watchdog inactive off;;
  asia1) if [ "$front" = 1 ]; then chk nginx inactive off; else chk nginx active enabled; fi; chk postgresql inactive off;;
  *)     chk nginx active enabled; chk postgresql inactive off;;
esac
chk isucon-pace inactive off
echo "-- env.sh (ISUREAL_* / PGSSLMODE, secrets masked)"
grep -E '^(ISUREAL_[A-Z0-9_]*|PGSSLMODE)=' /home/isucon/env.sh | sed -E 's/^([A-Z0-9_]*(SECRET|PASS|PASSWORD|TOKEN)[A-Z0-9_]*=).*/\1***/' | sort | sed 's/^/  /'
dup=$(grep -oE '^[A-Z0-9_]+=' /home/isucon/env.sh | sort | uniq -d | tr -d '=' | tr '\n' ' ')
[ -n "$dup" ] && warn "env.sh duplicate keys: $dup"
js=$(systemctl is-active isureal-jpegcheckd 2>/dev/null)
echo "  isureal-jpegcheckd: $js $(systemctl is-enabled isureal-jpegcheckd 2>/dev/null)"
[ -n "$(envget ISUREAL_JPEG_LOCAL_SOCK)" ] && [ "$js" != active ] && warn "ISUREAL_JPEG_LOCAL_SOCK set but isureal-jpegcheckd $js (falls back to in-process)"
echo "-- store WAL"
sw=$(envget ISUREAL_STORE_WAL); sa=$(envget ISUREAL_STORE_AWAIT_PG)
echo "  ISUREAL_STORE=$(envget ISUREAL_STORE) ISUREAL_STORE_WAL=${sw:-<unset>} ISUREAL_STORE_AWAIT_PG=${sa:-<unset>}"
if [ -n "$sw" ] || [ "$sa" = 0 ]; then
  [ "$walok" = 1 ] || ng "store WAL / AWAIT_PG=0 on but not adopted (rerun with STORE_WAL_OK=1 if adopted)"
  if [ -n "$sw" ]; then
    if [ -d "$sw" ]; then echo "  wal dir: $(ls -la "$sw" | tail -n +2 | tr '\n' ' ')"; else ng "ISUREAL_STORE_WAL dir $sw missing"; fi
    rec=$(sudo -n journalctl -u isureal-go -b --no-pager -o cat 2>/dev/null | grep -E 'store wal: (recovered|open failed|truncate failed|create table failed)' | tail -3)
    echo "$rec" | sed 's/^/  log: /'
    echo "$rec" | grep -q 'store wal: recovered' || ng "no 'store wal: recovered' log since boot"
    echo "$rec" | grep -qE 'open failed|truncate failed' && ng "store wal error in log"
  fi
fi
echo "-- leftovers"
dev=$(ip route show default | awk '{for(i=1;i<NF;i++) if($i=="dev"){print $(i+1); exit}}')
root=$(tc qdisc show dev "$dev" root 2>/dev/null | head -1)
echo "  qdisc $dev: $root"
case "$root" in *htb*|*tbf*|*netem*) ng "root qdisc on $dev is not default ($root) - PACE leftover";; esac
ip route | grep -E 'initcwnd|initrwnd' | sed 's/^/  route: /'
r24=$(ip route | grep -E '^192\.168\.[0-9]+\.0/24 .*(initcwnd|initrwnd|congctl)')
[ -n "$r24" ] && ng "per-/24 cwnd routes present - CWND leftover: $(echo $r24)"
[ -e /etc/default/isucon-pace ] && ng "/etc/default/isucon-pace exists"
echo "  tcp_slow_start_after_idle=$(cat /proc/sys/net/ipv4/tcp_slow_start_after_idle) tcp_notsent_lowat=$(cat /proc/sys/net/ipv4/tcp_notsent_lowat)"
if [ "$h" = asia2 ]; then
  q=$(sudo -n -u postgres psql -XAt \
     -c "select 'slots='||count(*) from pg_replication_slots" \
     -c "select 'walsenders='||count(*) from pg_stat_replication" \
     -c "select 'sync_standby=['||current_setting('synchronous_standby_names')||']'" \
     -c "select 'synchronous_commit='||current_setting('synchronous_commit')" \
     -c "select 'wal_level='||current_setting('wal_level')" \
     -c "select 'pending_restart='||coalesce(string_agg(name,','),'') from pg_settings where pending_restart" 2>&1)
  echo "$q" | sed 's/^/  pg: /'
  echo "$q" | grep -qx 'slots=0' || ng "pg_replication_slots not 0 ($(echo "$q" | grep '^slots=' || echo query failed))"
  echo "$q" | grep -qx 'sync_standby=\[\]' || ng "synchronous_standby_names not empty ($(echo "$q" | grep '^sync_standby=' || echo query failed))"
  echo "$q" | grep -qx 'pending_restart=' || warn "pg $(echo "$q" | grep '^pending_restart=')"
  for f in pg-sync.mode pg-logrepl.mode; do [ -e /etc/postgresql/16/main/$f ] && echo "  $f: $(cat /etc/postgresql/16/main/$f)"; done
  echo "  conf.d: $(ls /etc/postgresql/16/main/conf.d | tr '\n' ' ')"
fi
echo "-- pprof / logs"
pp=$(envget ISUREAL_PPROF_ADDR); ll=$(envget ISUREAL_LOG_LEVEL)
echo "  ISUREAL_PPROF_ADDR=${pp:-<unset>}  ISUREAL_LOG_LEVEL=${ll:-<unset> = debug; action log emits guard lines + 5xx only}  ISUREAL_FRONT_LOG=$(envget ISUREAL_FRONT_LOG)"
[ -n "$pp" ] && warn "pprof on ($pp)"
if [ "$h" != asia2 ] && [ -r /etc/nginx/nginx.conf ]; then
  grep -hE '^\s*access_log' /etc/nginx/nginx.conf /etc/nginx/sites-enabled/* 2>/dev/null | sed 's/^\s*/  nginx: /'
fi
echo "-- GET (localhost)"
ports="80 8080"; [ "$h" = asia2 ] && ports=8080
for p in $ports; do for path in /api/ /api/posting-window/current; do
  c=$(curl -s -o /dev/null -m 5 -w '%{http_code}' "http://127.0.0.1:$p$path")
  echo "  :$p$path -> $c"
  case "$c" in 000|5*) ng "GET :$p$path -> $c";; esac
done; done
true   # pin exit status: non-zero means ssh itself failed
EOF
)

for h in "${HOSTS[@]}"; do
  "${SSH[@]}" "$h" bash -s -- "$h" "$DF_NG" "${STORE_WAL_OK:-0}" <<<"$REMOTE" >"$OUT/$h" 2>&1 || echo "NG $h: ssh failed (rc=$?)" >>"$OUT/$h" &
done
wait

for h in "${HOSTS[@]}"; do
  echo "================ $h"
  grep -vE '^(NG|WARN) ' "$OUT/$h"
done

# local: pseudo-bench FW openings that were never closed (scripts/fw-local.sh)
if [[ -s logs/fw-local.log ]]; then
  open=$(awk '$2=="OPEN"{o[$3" "$4" "$5]++} $2=="CLOSE"{o[$3" "$4" "$5]--} END{for(k in o) if(o[k]>0) print k}' logs/fw-local.log)
  [[ -n $open ]] && echo "NG local: SG rule still open: $(echo $open)" >"$OUT/_local"
fi

echo "================ summary"
grep -h '^WARN ' "$OUT"/* 2>/dev/null
mapfile -t NGS < <(grep -h '^NG ' "$OUT"/* 2>/dev/null | sed 's/^NG //')
if [[ ${#NGS[@]} -eq 0 ]]; then
  echo "OK ($(date +%T), $(git rev-parse --short HEAD), hosts: ${HOSTS[*]})"
else
  ( IFS=';'; echo "NG: ${NGS[*]}" ); exit 1
fi
