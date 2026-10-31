#!/usr/bin/env bash
# usage: scripts/pace-stat.sh [host=asia1] [interval_s=5] [count=36]   (run alongside a bench; read-only, no lock)
# Per sample, one line: srtt p50/p90/max (ms) + summed Send-Q + sockets with retrans for TCP to the
# shaped/remote /24s, per-class tc deltas (sent MB, drops, backlog), bw_out_allowance_exceeded delta.
# Works with PACE=0 too (no htb classes -> root qdisc counters only), so run it for a baseline as well.
# DSTS overrides the subnets (default: the other regions as seen from host).
set -euo pipefail
H=${1:-asia1}; IV=${2:-5}; N=${3:-36}
case $H in
  asia1|asia2) D=${DSTS:-"192.168.2.0/24 192.168.3.0/24"};;
  europe1) D=${DSTS:-"192.168.1.0/24 192.168.4.0/24 192.168.3.0/24"};;
  america1) D=${DSTS:-"192.168.1.0/24 192.168.4.0/24 192.168.2.0/24"};;
  *) D=${DSTS:?set DSTS};;
esac
ssh -o BatchMode=yes -o ConnectTimeout=10 "$H" "IV=$IV N=$N D='$D' bash -s" <<'REMOTE'
set -u; export LC_ALL=C
dev=$(ip -4 -o route show default | awk '{for(i=1;i<NF;i++)if($i=="dev"){print $(i+1);exit}}')
filt=""; for n in $D; do filt="${filt:+$filt or }dst $n"; done
tcsnap() {   # "<id> <bytes> <drops> <backlog_bytes>" per htb class / leaf qdisc (or root qdisc)
  { tc -s class show dev "$dev"; tc -s qdisc show dev "$dev"; } | awk '
    /^(class|qdisc)/ { par="root"; for (i=4;i<NF;i++) if ($i=="parent") par=$(i+1); id=($1=="qdisc" ? "q:" $2 ":" $3 "<" par : "c:" $3 "<" par) }
    /Sent/ { b=$2; d=$7; sub(/,/,"",d) }
    /backlog/ { print id, b, d, $2 }'
}
allow() { ethtool -S "$dev" 2>/dev/null | awk '/bw_out_allowance_exceeded/{print $2}'; }
echo "dev=$dev dsts=$D root=$(tc qdisc show dev "$dev" root | head -1)"
echo "route: $(ip route get $(echo $D | awk '{print $1}' | sed 's|0/24|10|') | head -1)"
echo "sysctl: cc=$(sysctl -n net.ipv4.tcp_congestion_control) ssai=$(sysctl -n net.ipv4.tcp_slow_start_after_idle) lowat=$(sysctl -n net.ipv4.tcp_notsent_lowat)"
first=$(tcsnap); prev=$first; a0=$(allow); ap=$a0
for i in $(seq "$N"); do
  sleep "$IV"
  cur=$(tcsnap); a=$(allow)
  rtt=$(ss -tin "( $filt )" | grep -oE ' rtt:[0-9.]+' | cut -d: -f2 | sort -n | awk '{v[NR]=$1} END{if(NR==0){print "n=0";exit} printf "n=%d p50=%.0f p90=%.0f max=%.0f", NR, v[int(NR*0.5)+(NR*0.5>int(NR*0.5))], v[int(NR*0.9)+(NR*0.9>int(NR*0.9))], v[NR]}')
  sq=$(ss -tn "( $filt )" | awk 'NR>1{s+=$3; if($3>m)m=$3} END{printf "sendq=%.1fMB maxq=%.1fMB", s/1e6, m/1e6}')
  rx=$(ss -tin "( $filt )" | grep -c 'retrans:[0-9]*/[1-9]' || true)
  cls=$(join <(echo "$prev" | sort) <(echo "$cur" | sort) | awk -v iv="$IV" '$5>$2 || $6>$3 || $7!="0b" { printf " %s:%.0fMbit/d%d/q%s", $1, ($5-$2)*8/iv/1e6, $6-$3, $7 }')
  echo "$(date +%T) srtt[$rtt] $sq retrans_socks=$rx allow+$(( ${a:-0} - ${ap:-0} )) |$cls"
  prev=$cur; ap=$a
done
echo "--- total over $((IV * N))s: bw_out_allowance_exceeded +$(( ${a:-0} - ${a0:-0} ))"
join <(echo "$first" | sort) <(echo "$cur" | sort) | awk '{ printf "  %-22s sent=%8.1fMB drops=%d\n", $1, ($5-$2)/1e6, $6-$3 }'
REMOTE
