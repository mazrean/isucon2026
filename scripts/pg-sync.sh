#!/bin/bash
# 同期レプリケーション切替 (asia2)。実体は pg-sync-watchdog が健全なスタンバイだけで
#   synchronous_standby_names = 'ANY n (...)' / synchronous_commit = remote_apply を組んで reload する。
# usage: scripts/pg-sync.sh on|off|status
set -eu
SSH="ssh -o BatchMode=yes -o ConnectTimeout=10 asia2"
case "${1:-status}" in
  on|off)
    $SSH "echo $1 | sudo tee /etc/postgresql/16/main/pg-sync.mode >/dev/null" </dev/null
    if [ "$1" = off ]; then
      # watchdog が止まっていても確実に外す (即時)
      $SSH "sudo systemctl is-active -q pg-sync-watchdog || { printf \"synchronous_standby_names = ''\nsynchronous_commit = off\n\" | sudo tee /etc/postgresql/16/main/conf.d/zz-sync.conf >/dev/null; sudo -u postgres psql -Atc 'select pg_reload_conf()'; }" </dev/null
    fi
    sleep 4 ;;
  status) ;;
  *) echo "usage: $0 on|off|status" >&2; exit 2 ;;
esac
$SSH 'cat /etc/postgresql/16/main/pg-sync.mode 2>/dev/null; sudo -u postgres psql -X -d isureal -c "show synchronous_standby_names" -c "show synchronous_commit" -c "select application_name,state,sync_state,replay_lag,pg_wal_lsn_diff(pg_current_wal_lsn(),replay_lsn) as lag_bytes from pg_stat_replication order by 1" -c "select * from isucon_sync_members order by 1"; sudo journalctl -u pg-sync-watchdog -n 5 --no-pager -o cat' </dev/null
