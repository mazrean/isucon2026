#!/bin/sh
# Installed by scripts/pace.sh as /usr/local/sbin/isucon-pace.sh; config in /etc/default/isucon-pace
# (or the file given as $1). Idempotent: every run first removes what it owns, then re-adds per config.
#   QDISC=1      root HTB on the default-route iface; per /24 in SHAPE_DSTS: ceil RATE, prio 0 "small"
#                (tot_len < 1024: ACKs, tiny responses/pushes; guaranteed SMALL_RATE, borrows first),
#                prio 1 "bulk" (rest, BULK_RATE + borrow); each leaf fq_codel (sparse flows go first). Everything else -> 1:30 (DEFAULT_RATE, unshaped).
#   QDISC=0      root qdisc deleted -> kernel default (mq + fq_codel on ena).
#   ROUTE_DSTS   /24s that get their own route via the gw (the /16 from isucon-route-tune stays):
#                CWND=1 -> initcwnd 64 initrwnd 64 (else 32/64, same as the /16), BBR=1 -> congctl bbr.
#                Routes exist only if CWND=1 or BBR=1.
#   CWND=1       also net.ipv4.tcp_slow_start_after_idle=0 (CWND=0 -> value from sysctl.d, else 1)
#   LOWAT=1      net.ipv4.tcp_notsent_lowat=131072 (else kernel default 4294967295)
set -eu
CONF=${1:-/etc/default/isucon-pace}
QDISC=0 RATE=130mbit SMALL_RATE=20mbit BULK_RATE=110mbit DEFAULT_RATE=10gbit SHAPE_DSTS="" ROUTE_DSTS="" CWND=0 BBR=0 LOWAT=0
[ -f "$CONF" ] && . "$CONF"
gw=$(ip -4 -o route show default | awk '{for(i=1;i<NF;i++){if($i=="via"){print $(i+1);exit}}}')
dev=$(ip -4 -o route show default | awk '{for(i=1;i<NF;i++){if($i=="dev"){print $(i+1);exit}}}')
[ -n "$dev" ] && [ -n "$gw" ] || { echo "no default route" >&2; exit 1; }

# --- qdisc ---
if tc qdisc show dev "$dev" root | grep -q '^qdisc htb 1:'; then tc qdisc del dev "$dev" root; fi
if [ "$QDISC" = 1 ] && [ -n "$SHAPE_DSTS" ]; then
  tc qdisc add dev "$dev" root handle 1: htb default 30 r2q 100
  tc class add dev "$dev" parent 1: classid 1:30 htb rate "$DEFAULT_RATE" ceil "$DEFAULT_RATE" quantum 60000
  tc qdisc add dev "$dev" parent 1:30 handle 30: fq_codel
  i=1
  for n in $SHAPE_DSTS; do
    p=$((i * 10)); s=$((p + 1)); b=$((p + 2))
    # burst: at 130mbit the per-tick token is ~65KB and GSO skbs are up to 64KB
    tc class add dev "$dev" parent 1: classid 1:$p htb rate "$RATE" ceil "$RATE" burst 128k cburst 128k quantum 60000
    tc class add dev "$dev" parent 1:$p classid 1:$s htb rate "$SMALL_RATE" ceil "$RATE" burst 128k cburst 128k prio 0 quantum 15000
    tc class add dev "$dev" parent 1:$p classid 1:$b htb rate "$BULK_RATE" ceil "$RATE" burst 128k cburst 128k prio 1 quantum 60000
    tc qdisc add dev "$dev" parent 1:$s handle $s: fq_codel
    tc qdisc add dev "$dev" parent 1:$b handle $b: fq_codel
    tc filter add dev "$dev" parent 1: protocol ip prio 1 u32 match ip dst "$n" match u16 0x0000 0xfc00 at 2 flowid 1:$s
    tc filter add dev "$dev" parent 1: protocol ip prio 3 u32 match ip dst "$n" flowid 1:$b
    i=$((i + 1))
  done
fi

# --- per-destination routes (only the /24s we own; the /16 is isucon-route-tune's) ---
for n in 192.168.1.0/24 192.168.2.0/24 192.168.3.0/24 192.168.4.0/24; do
  if ip route show "$n" | grep -q "via $gw"; then ip route del "$n" via "$gw" dev "$dev"; fi
done
if [ "$CWND" = 1 ] || [ "$BBR" = 1 ]; then
  cw=32; [ "$CWND" = 1 ] && cw=64
  cc=""; [ "$BBR" = 1 ] && cc="congctl bbr"
  for n in $ROUTE_DSTS; do
    case $n in "$(ip -4 -o addr show dev "$dev" | awk '{print $4; exit}' | sed 's|\.[0-9]*/.*|.0/24|')") continue;; esac  # never our own subnet
    # shellcheck disable=SC2086
    ip route replace "$n" via "$gw" dev "$dev" initcwnd $cw initrwnd 64 $cc
  done
fi

# --- sysctl (runtime only; reverting restores the persisted value) ---
if [ "$CWND" = 1 ]; then ssai=0; else
  ssai=$(sed -n 's/^net.ipv4.tcp_slow_start_after_idle *= *//p' /etc/sysctl.d/*.conf 2>/dev/null | tail -1); ssai=${ssai:-1}
fi
[ "$(sysctl -n net.ipv4.tcp_slow_start_after_idle)" = "$ssai" ] || sysctl -q -w net.ipv4.tcp_slow_start_after_idle="$ssai"
lw=4294967295; [ "$LOWAT" = 1 ] && lw=131072
[ "$(sysctl -n net.ipv4.tcp_notsent_lowat)" = "$lw" ] || sysctl -q -w net.ipv4.tcp_notsent_lowat="$lw"

echo "qdisc=$(tc qdisc show dev "$dev" root | awk '{print $2; exit}') routes=[$(ip -4 route show | grep -E '^192\.168\.[0-9]+\.0/24 via' | awk '{printf "%s ", $1}')] ssai=$ssai lowat=$lw"
