#!/bin/bash
# asia2 をプライマリ、asia1/europe1/america1 をホットスタンバイにする (冪等)。
# usage:
#   scripts/pg-replica-setup.sh primary          # hba/role/conf/table/watchdog を asia2 に適用 (reload のみ)
#   scripts/pg-replica-setup.sh walserve start|stop  # asia2 の walsave を 192.168.4.10:18081 で HTTP 配信
#   scripts/pg-replica-setup.sh standby [hosts...]   # スタンバイ化して enable --now (既定: 3 台)
#   scripts/pg-replica-setup.sh status
# 前提: スタンバイの /var/lib/postgresql/16/main は asia2 と同一イメージ (system id 一致) で停止状態。
#       それ以外 (データが進んでいる/壊れた) なら pg_basebackup -R -S <slot> でやり直すこと。
set -eu
cd "$(dirname "$0")/.."
SSH="ssh -o BatchMode=yes -o ConnectTimeout=10"
PGC=/etc/postgresql/16/main
REPL_PASS=isurepl2026
# env.lock は設定変更・再起動の瞬間だけ取る (WAL 追従待ち・確認中は保持しない)
mkdir -p /tmp/isucon
lock() { exec 9>/tmp/isucon/env.lock; flock 9; }
unlock() { flock -u 9; exec 9>&-; }

primary() {
  lock
  $SSH asia2 "sudo tee $PGC/conf.d/isucon-repl.conf >/dev/null" < etc/pg/primary-repl.conf
  $SSH asia2 "sudo tee $PGC/conf.d/isucon-tuning.conf >/dev/null" < etc/pg/primary-tuning.conf
  $SSH asia2 "sudo tee /tmp/pg-hba-add >/dev/null" < etc/pg/primary-hba.conf
  $SSH asia2 "grep -v '^#' /tmp/pg-hba-add | while read -r l; do sudo grep -qxF \"\$l\" $PGC/pg_hba.conf || echo \"\$l\" | sudo tee -a $PGC/pg_hba.conf >/dev/null; done" </dev/null
  $SSH asia2 "sudo -u postgres psql -X -v ON_ERROR_STOP=1 -d isureal" <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='replicator') THEN
    CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '$REPL_PASS';
  END IF;
END \$\$;
$(cat etc/pg/isucon_sync_members.sql)
DO \$\$ DECLARE h text; BEGIN
  FOREACH h IN ARRAY ARRAY['asia1','europe1','america1'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=h) THEN
      PERFORM pg_create_physical_replication_slot(h, true);
    END IF;
  END LOOP;
END \$\$;
SELECT pg_reload_conf();
SQL
  $SSH asia2 "sudo tee /usr/local/bin/pg-sync-watchdog >/dev/null && sudo chmod 755 /usr/local/bin/pg-sync-watchdog" < scripts/pg-sync-watchdog.sh
  $SSH asia2 "sudo tee /etc/systemd/system/pg-sync-watchdog.service >/dev/null" < etc/pg/pg-sync-watchdog.service
  $SSH asia2 "test -f $PGC/pg-sync.mode || echo off | sudo tee $PGC/pg-sync.mode >/dev/null; sudo systemctl daemon-reload && sudo systemctl enable pg-sync-watchdog && sudo systemctl restart pg-sync-watchdog" </dev/null
  unlock
}

walserve() {
  case "$1" in
    # systemd-run の一時ユニットで起動 (ssh セッションを掴まない)
    start) $SSH asia2 "sudo systemd-run --unit=pg-walserve -p User=postgres -p WorkingDirectory=/var/lib/postgresql/walsave python3 -m http.server 18081 --bind 192.168.4.10; sleep 1; ss -ltn | grep 18081" </dev/null ;;
    stop)  $SSH asia2 "sudo systemctl stop pg-walserve 2>/dev/null; sudo pkill -f '[h]ttp.server 18081' || true" </dev/null ;;
  esac
}

standby() {
  for h in "$@"; do
    echo "== $h"
    lock
    sed "s/@HOST@/$h/g" etc/pg/standby.conf | $SSH "$h" "sudo tee $PGC/conf.d/isucon-standby.conf >/dev/null && sudo chown postgres:postgres $PGC/conf.d/isucon-standby.conf && sudo chmod 640 $PGC/conf.d/isucon-standby.conf"
    $SSH "$h" "sudo -u postgres touch /var/lib/postgresql/16/main/standby.signal && sudo systemctl enable --now postgresql && sleep 2 && systemctl is-active postgresql@16-main" </dev/null
    unlock
  done
}

restart_primary() { # restart 必須パラメータ (shared_buffers/max_connections) の反映。公式ベンチ外で。
  lock
  $SSH asia2 "sudo -u postgres psql -XAtc \"select name,setting,pending_restart from pg_settings where pending_restart\"; sudo systemctl restart postgresql@16-main && sudo -u postgres psql -XAtc 'show shared_buffers' -c 'show max_connections'" </dev/null
  unlock
}

status() {
  for h in asia1 europe1 america1; do
    echo "== $h"; $SSH "$h" "sudo -u postgres psql -XAtc \"select pg_is_in_recovery(), status, sender_host, latest_end_lsn, now()-pg_last_xact_replay_timestamp() from pg_stat_wal_receiver right join (select 1) x on true\"" </dev/null || true
  done
  $SSH asia2 "sudo -u postgres psql -X -c 'select application_name,client_addr,state,sync_state,write_lag,replay_lag,pg_wal_lsn_diff(pg_current_wal_lsn(),replay_lsn) lag_bytes from pg_stat_replication order by 1' -c 'select slot_name,active,wal_status,restart_lsn from pg_replication_slots'" </dev/null
}

cmd="${1:-status}"; shift || true
case "$cmd" in
  primary) primary ;;
  walserve) walserve "${1:-start}" ;;
  standby) [ $# -gt 0 ] || set -- asia1 europe1 america1; standby "$@" ;;
  restart-primary) restart_primary ;;
  status) status ;;
  *) echo "usage: $0 primary|walserve start|stop|standby [hosts]|status" >&2; exit 2 ;;
esac
