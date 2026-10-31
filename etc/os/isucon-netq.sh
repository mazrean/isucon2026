#!/bin/sh
# Installed by scripts/os-knobs.sh as /usr/local/sbin/isucon-netq.sh (NETQ=1), run at boot by isucon-netq.service.
#   $1 = on : stop irqbalance, IRQ of queue N -> CPU N, XPS: CPU N transmits on tx-N (tx completion stays on the
#             CPU that sent; irqbalance had swapped asia1's queue<->CPU mapping during the day).
#   $1 = off: xps_cpus back to 0 (hash-based tx queue), start irqbalance again (it re-spreads the IRQs).
# qdisc is not touched (DEAD-ENDS: qdisc swap alone = no effect; final-check reads the root qdisc).
set -eu
dev=$(ip -4 -o route show default | awk '{for(i=1;i<NF;i++){if($i=="dev"){print $(i+1);exit}}}')
[ -n "$dev" ] || { echo "no default route" >&2; exit 1; }
ncpu=$(nproc)
if [ "${1:-on}" = on ]; then
  systemctl stop irqbalance 2>/dev/null || true
  grep -E "$dev-Tx-Rx-[0-9]+" /proc/interrupts | while read -r line; do
    irq=$(echo "$line" | cut -d: -f1 | tr -d ' ')
    q=$(echo "$line" | grep -oE 'Tx-Rx-[0-9]+' | grep -oE '[0-9]+$')
    echo $((q % ncpu)) > "/proc/irq/$irq/smp_affinity_list" 2>/dev/null || true
  done
  for t in /sys/class/net/"$dev"/queues/tx-*; do
    n=${t##*tx-}; printf '%x' $((1 << (n % ncpu))) > "$t/xps_cpus" 2>/dev/null || true
  done
else
  for t in /sys/class/net/"$dev"/queues/tx-*; do echo 0 > "$t/xps_cpus" 2>/dev/null || true; done
  if systemctl is-enabled -q irqbalance 2>/dev/null; then systemctl start irqbalance 2>/dev/null || true; fi
fi
for i in $(grep -E "$dev-Tx-Rx" /proc/interrupts | cut -d: -f1); do printf 'irq%s->%s ' "$i" "$(cat /proc/irq/$i/smp_affinity_list)"; done
for t in /sys/class/net/"$dev"/queues/tx-*; do printf '%s xps=%s ' "${t##*/}" "$(cat $t/xps_cpus)"; done
echo "irqbalance=$(systemctl is-active irqbalance 2>/dev/null)"
