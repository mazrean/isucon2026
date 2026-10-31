#!/usr/bin/env bash
# usage: scripts/host-tune.sh [hosts...]   (default: asia1 europe1 america1 asia2)
# Idempotent OS / systemd tuning. Everything persists across reboot (sysctl.d, drop-ins, oneshot unit).
# Host state owned here:
#   /etc/sysctl.d/99-zz-isucon-tune.conf
#   /usr/local/sbin/isucon-route-tune.sh, /etc/systemd/system/isucon-route-tune.service
#   /etc/systemd/system/isureal-go.service.d/10-tune.conf, /etc/systemd/system/nginx.service.d/10-limits.conf
#   disabled units listed in DISABLE below
# RESTART=0 skips restarting isureal-go / nginx (drop-ins then take effect at next restart/reboot).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/isucon
if [[ -z ${LOCK_HELD:-} ]]; then exec 9>/tmp/isucon/env.lock; flock -w 300 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner 2>/dev/null)" >&2; exit 75; }; echo "$(date +%T) host-tune $(git rev-parse --short HEAD)" >| /tmp/isucon/env.lock.owner; fi
HOSTS=("$@"); [[ ${#HOSTS[@]} -gt 0 ]] || HOSTS=(asia1 europe1 america1 asia2)
RESTART=${RESTART:-1}
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10)
FILES=(etc/os/sysctl.d/99-zz-isucon-tune.conf etc/os/isucon-route-tune.sh etc/systemd/isucon-route-tune.service
       etc/systemd/isureal-go.service.d/10-tune.conf etc/systemd/nginx.service.d/10-limits.conf)
for h in "${HOSTS[@]}"; do
  (
    "${SSH[@]}" "$h" 'rm -rf /tmp/host-tune && mkdir -p /tmp/host-tune' </dev/null
    scp -q "${FILES[@]}" "$h:/tmp/host-tune/"
    "${SSH[@]}" "$h" "RESTART=$RESTART bash -s" <<'REMOTE' 2>&1 | sed "s/^/$h: /"
set -eu
T=/tmp/host-tune
# never touched: ssh, snapd, amazon-ssm-agent, isureal-*, envcheck*, isucon-register, chrony, cron, rsyslog, journald
DISABLE="unattended-upgrades.service ModemManager.service multipathd.service multipathd.socket udisks2.service
  apt-daily.timer apt-daily-upgrade.timer fwupd-refresh.timer motd-news.timer man-db.timer
  update-notifier-download.timer update-notifier-motd.timer e2scrub_all.timer dpkg-db-backup.timer"
for u in $DISABLE; do
  if systemctl is-enabled -q "$u" 2>/dev/null || systemctl is-active -q "$u" 2>/dev/null; then
    sudo systemctl disable --now -q "$u" 2>/dev/null || true
  fi
done
sudo install -m 644 $T/99-zz-isucon-tune.conf /etc/sysctl.d/99-zz-isucon-tune.conf
sudo sysctl -q -p /etc/sysctl.d/99-zz-isucon-tune.conf
sudo install -m 755 $T/isucon-route-tune.sh /usr/local/sbin/isucon-route-tune.sh
sudo install -m 644 $T/isucon-route-tune.service /etc/systemd/system/isucon-route-tune.service
changed_go=0; changed_ngx=0
if [ -f /etc/systemd/system/isureal-go.service ] || systemctl cat isureal-go >/dev/null 2>&1; then
  sudo install -d /etc/systemd/system/isureal-go.service.d
  cmp -s $T/10-tune.conf /etc/systemd/system/isureal-go.service.d/10-tune.conf || { sudo install -m 644 $T/10-tune.conf /etc/systemd/system/isureal-go.service.d/10-tune.conf; changed_go=1; }
fi
if systemctl cat nginx >/dev/null 2>&1; then
  sudo install -d /etc/systemd/system/nginx.service.d
  cmp -s $T/10-limits.conf /etc/systemd/system/nginx.service.d/10-limits.conf || { sudo install -m 644 $T/10-limits.conf /etc/systemd/system/nginx.service.d/10-limits.conf; changed_ngx=1; }
fi
sudo systemctl daemon-reload
sudo systemctl enable -q isucon-route-tune.service
sudo systemctl restart isucon-route-tune.service
if [ "$RESTART" = 1 ]; then
  if [ $changed_go = 1 ] && systemctl is-active -q isureal-go; then sudo systemctl restart isureal-go; echo "restarted isureal-go"; fi
  if [ $changed_ngx = 1 ] && systemctl is-active -q nginx; then sudo nginx -t -q && sudo systemctl restart nginx; echo "restarted nginx"; fi
fi
echo "ok: $(sysctl -n net.ipv4.tcp_slow_start_after_idle net.core.somaxconn | tr '\n' ' ') route: $(ip route show 192.168.0.0/16)"
REMOTE
  ) &
done
wait
