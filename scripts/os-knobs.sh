#!/usr/bin/env bash
# usage: VM=0|1 NETQ=0|1 scripts/os-knobs.sh   (deploy.sh calls it every run, so 0 reverts)
# Kernel knobs measured in the 14:46 sweep (no restart of any service, all reboot-persistent):
#   VM=1   (VM_HOSTS, default "asia1 asia2"): /etc/sysctl.d/98-isucon-vm.conf (watermark_boost 0,
#          watermark_scale 125, min_free 128MB, compaction_proactiveness 0, dirty_background 64MB / dirty 512MB).
#          0 -> file removed and the kernel defaults written back at runtime (removing the file alone keeps them).
#   NETQ=1 (NETQ_HOSTS, default "asia1"): irqbalance stopped, queue N IRQ -> CPU N, XPS CPU N -> tx-N
#          (/usr/local/sbin/isucon-netq.sh + isucon-netq.service). 0 -> unit removed, xps 0, irqbalance started.
# Host state owned here: /etc/sysctl.d/98-isucon-vm.conf, the vm.* values in it,
#   /usr/local/sbin/isucon-netq.sh, /etc/systemd/system/isucon-netq.service, xps_cpus, irqbalance active state.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/isucon
if [[ -z ${LOCK_HELD:-} ]]; then exec 9>>/tmp/isucon/env.lock; flock -w 600 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner 2>/dev/null)" >&2; exit 75; }; echo "$(date +%T) os-knobs $(git rev-parse --short HEAD)" >| /tmp/isucon/env.lock.owner; fi
VM=${VM:-0}
NETQ=${NETQ:-0}
VM_HOSTS=${VM_HOSTS:-asia1 asia2}
NETQ_HOSTS=${NETQ_HOSTS:-asia1}
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10)
for h in asia1 asia2; do
  (
    v=0; [[ $VM == 1 && " $VM_HOSTS " == *" $h "* ]] && v=1
    n=0; [[ $NETQ == 1 && " $NETQ_HOSTS " == *" $h "* ]] && n=1
    scp -q etc/os/sysctl.d/98-isucon-vm.conf etc/os/isucon-netq.sh etc/systemd/isucon-netq.service "$h:/tmp/"
    "${SSH[@]}" "$h" "V=$v N=$n sh -s" <<'REMOTE'
set -eu
if [ "$V" = 1 ]; then
  sudo install -m 644 /tmp/98-isucon-vm.conf /etc/sysctl.d/98-isucon-vm.conf
  sudo sysctl -q -p /etc/sysctl.d/98-isucon-vm.conf
elif [ -e /etc/sysctl.d/98-isucon-vm.conf ] || [ "$(sysctl -n vm.watermark_boost_factor)" != 15000 ] || [ "$(sysctl -n vm.dirty_bytes)" != 0 ]; then
  sudo rm -f /etc/sysctl.d/98-isucon-vm.conf
  # dirty_ratio/dirty_background_ratio zero the *_bytes; min_free 67584 = value before the knob on both hosts
  sudo sysctl -q -w vm.watermark_boost_factor=15000 vm.watermark_scale_factor=10 vm.min_free_kbytes=67584 \
    vm.compaction_proactiveness=20 vm.dirty_background_ratio=10 vm.dirty_ratio=20
fi
if [ "$N" = 1 ]; then
  sudo install -m 755 /tmp/isucon-netq.sh /usr/local/sbin/isucon-netq.sh
  sudo install -m 644 /tmp/isucon-netq.service /etc/systemd/system/isucon-netq.service
  sudo systemctl daemon-reload; sudo systemctl enable -q isucon-netq.service
  sudo /usr/local/sbin/isucon-netq.sh on
elif [ -e /etc/systemd/system/isucon-netq.service ] || ! systemctl is-active -q irqbalance; then
  sudo systemctl disable -q --now isucon-netq.service 2>/dev/null || true
  sudo rm -f /etc/systemd/system/isucon-netq.service /usr/local/sbin/isucon-netq.sh
  sudo systemctl daemon-reload
  sudo sh /tmp/isucon-netq.sh off
fi
echo "vm: boost=$(sysctl -n vm.watermark_boost_factor) scale=$(sysctl -n vm.watermark_scale_factor) min_free=$(sysctl -n vm.min_free_kbytes) dirty_bg=$(sysctl -n vm.dirty_background_bytes)/$(sysctl -n vm.dirty_background_ratio)% netq=$(systemctl is-active isucon-netq 2>/dev/null) irqbalance=$(systemctl is-active irqbalance 2>/dev/null)"
REMOTE
  ) 2>&1 | sed "s/^/$h: os-knobs: /" &
done
wait
