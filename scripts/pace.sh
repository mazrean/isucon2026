#!/usr/bin/env bash
# usage: PACE=0|1 CWND=0|1 scripts/pace.sh [hosts...]   (default: asia1 europe1 america1; deploy.sh calls it)
# Per-destination egress pacing + per-destination route cwnd on the app hosts (true-limit.md §7 #1).
# The router's netem link asia1 -> europe/america (~150Mbit) is full: the queue sits in the router
# (RTT 100 -> 130-285ms, Send-Q 1-6MB, bufferbloat). Shaping just under the link rate moves the queue
# into our own qdisc, where fq_codel keeps it short and small packets / API responses go first.
#   PACE=1  (hosts in PACE_HOSTS, default asia1): root HTB, each shaped /24 ceil PACE_RATE (130mbit),
#           packets < 1024B (ACKs, small responses) prio 0, rest prio 1 (no sport-8080 rule: /internal/mem/dump, batches pulls leave asia1 from :8080), leaves fq_codel; rest unshaped.
#           Root HTB replaces mq (one qdisc lock for both ena queues) -> asia1 only by default.
#   CWND=1  (all hosts given, both directions): own /24 routes to the other regions' subnets with
#           initcwnd 64 initrwnd 64, plus tcp_slow_start_after_idle=0. A 361KB body at 100ms RTT
#           needs ~3 RTT from cwnd 32, ~2 from 64.
#   PACE_BBR=1    congctl bbr on those /24 routes (scoped; true-limit.md lists BBR as 0-small/worse)
#   PACE_LOWAT=1  tcp_notsent_lowat=131072: only shrinks the *unsent* part of Send-Q (the Go writer
#                 blocks earlier); it does not fix in-flight bloat, the HTB/fq_codel does.
#   All 0 on a host -> qdisc deleted (kernel default mq + fq_codel), /24 routes deleted (the /16 from
#   isucon-route-tune stays), sysctls back to the sysctl.d / kernel default, unit + files removed.
# Host state owned here: root qdisc of the default-route iface, routes 192.168.{1,2,3,4}.0/24 via gw,
#   net.ipv4.tcp_notsent_lowat (runtime), tcp_slow_start_after_idle (runtime, while CWND=1),
#   /usr/local/sbin/isucon-pace.sh, /etc/default/isucon-pace, /etc/systemd/system/isucon-pace.service
#   (re-applies at boot so a reboot keeps the measured state).
# Measure with scripts/pace-stat.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/isucon
if [[ -z ${LOCK_HELD:-} ]]; then exec 9>>/tmp/isucon/env.lock; flock -w 600 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner 2>/dev/null)" >&2; exit 75; }; echo "$(date +%T) pace $(git rev-parse --short HEAD)" >| /tmp/isucon/env.lock.owner; fi
HOSTS=("$@"); [[ ${#HOSTS[@]} -gt 0 ]] || HOSTS=(asia1 europe1 america1)
PACE=${PACE:-0}
CWND=${CWND:-0}
PACE_HOSTS=${PACE_HOSTS:-asia1}
PACE_RATE=${PACE_RATE:-130mbit}
PACE_SMALL_RATE=${PACE_SMALL_RATE:-20mbit}
PACE_BULK_RATE=${PACE_BULK_RATE:-110mbit}   # small + bulk = PACE_RATE so prio 0 really wins when borrowing
PACE_DEFAULT_RATE=${PACE_DEFAULT_RATE:-10gbit}
PACE_BBR=${PACE_BBR:-0}
PACE_LOWAT=${PACE_LOWAT:-0}
shape_dsts() {   # override with PACE_DSTS_<host>
  local v="PACE_DSTS_$1"; [[ -n ${!v:-} ]] && { echo "${!v}"; return; }
  case $1 in
    asia1) echo "192.168.2.0/24 192.168.3.0/24";;
    europe1) echo "192.168.1.0/24 192.168.3.0/24";;
    america1) echo "192.168.1.0/24 192.168.2.0/24";;
  esac
}
route_dsts() {   # other regions (asia = 192.168.1/.4); own subnet is skipped remotely as well
  case $1 in
    asia1|asia2) echo "192.168.2.0/24 192.168.3.0/24";;
    europe1) echo "192.168.1.0/24 192.168.4.0/24 192.168.3.0/24";;
    america1) echo "192.168.1.0/24 192.168.4.0/24 192.168.2.0/24";;
  esac
}
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10)
for h in "${HOSTS[@]}"; do
  (
    q=0; [[ $PACE == 1 && " $PACE_HOSTS " == *" $h "* ]] && q=1
    if [[ $q == 1 || $CWND == 1 || $PACE_BBR == 1 || $PACE_LOWAT == 1 ]]; then
      conf="QDISC=$q RATE=$PACE_RATE SMALL_RATE=$PACE_SMALL_RATE BULK_RATE=$PACE_BULK_RATE DEFAULT_RATE=$PACE_DEFAULT_RATE
SHAPE_DSTS=\"$(shape_dsts "$h")\" ROUTE_DSTS=\"$(route_dsts "$h")\" CWND=$CWND BBR=$PACE_BBR LOWAT=$PACE_LOWAT"
      scp -q etc/os/isucon-pace.sh etc/systemd/isucon-pace.service "$h:/tmp/"
      printf '%s\n' "$conf" | "${SSH[@]}" "$h" 'set -e; cat > /tmp/isucon-pace.conf
        sudo install -m 755 /tmp/isucon-pace.sh /usr/local/sbin/isucon-pace.sh
        sudo install -m 644 /tmp/isucon-pace.conf /etc/default/isucon-pace
        sudo install -m 644 /tmp/isucon-pace.service /etc/systemd/system/isucon-pace.service
        sudo systemctl daemon-reload; sudo systemctl enable -q isucon-pace.service
        sudo /usr/local/sbin/isucon-pace.sh /etc/default/isucon-pace'
    else
      scp -q etc/os/isucon-pace.sh "$h:/tmp/isucon-pace.sh"
      "${SSH[@]}" "$h" 'set -e
        if [ -f /etc/systemd/system/isucon-pace.service ]; then sudo systemctl disable -q --now isucon-pace.service || true; fi
        sudo rm -f /etc/systemd/system/isucon-pace.service /etc/default/isucon-pace /usr/local/sbin/isucon-pace.sh
        sudo systemctl daemon-reload
        sudo sh /tmp/isucon-pace.sh /dev/null' </dev/null
    fi
  ) 2>&1 | sed "s/^/$h: pace: /" &
done
wait
