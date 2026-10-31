#!/usr/bin/env bash
# Official bench with LOGREPL=1 + asia2 pg-logrepl-watchdog, then revert to the pump model.
# usage: scripts/logrepl-bench.sh "<note>"   (driver only; user-approved 13:0x)
set -uo pipefail
cd "$(dirname "$0")/.."
NOTE=${1:-LOGREPL=1}
exec 9>>/tmp/isucon/env.lock
flock -w 1200 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner)" >&2; exit 75; }
echo "$(date +%T) driver logrepl-bench $(git rev-parse --short HEAD) $NOTE" >| /tmp/isucon/env.lock.owner
export LOCK_HELD=1
psql2() { ssh -o BatchMode=yes asia2 "sudo -u postgres psql -d isureal -AtX -c \"$1\"" </dev/null; }
scp -q scripts/pg-logrepl-watchdog.sh etc/pg/pg-logrepl-watchdog.service asia2:/tmp/
ssh asia2 'set -e
  sudo install -m 755 /tmp/pg-logrepl-watchdog.sh /usr/local/bin/pg-logrepl-watchdog
  sudo install -m 644 /tmp/pg-logrepl-watchdog.service /etc/systemd/system/
  echo on | sudo tee /etc/postgresql/16/main/pg-logrepl.mode >/dev/null
  sudo systemctl disable --now pg-sync-watchdog 2>/dev/null || true
  sudo systemctl daemon-reload && sudo systemctl enable --now pg-logrepl-watchdog' </dev/null
LOGREPL=1 scripts/deploy.sh 2>&1 | tail -4
for i in $(seq 1 30); do
  n=$(psql2 "select count(*) from pg_stat_replication where sync_state in ('sync','quorum')")
  [[ $n == 3 ]] && break; sleep 1
done
echo "sync members: $n after ${i}s"; psql2 "show synchronous_standby_names"; psql2 "select * from mem_logrepl_members"
out=$(scripts/portal-bench.sh run 2>&1)
line=$(grep 'job=' <<<"$out" | tail -1)
echo "$out" | grep -E '整合性|失敗|"messages"' -A3 | head -8
echo "$line"
printf '%s\t%s\t%s\t%s\n' "$(date +%H:%M)" "$(git rev-parse --short HEAD)" "$line" "$NOTE" >> logs/SCORES.tsv
if grep -q 'pass=false' <<<"$line" || grep -qE '見え|invalid_session' "$(ls -t logs/bench/*.log | head -1)"; then
  mkdir -p logs/faildump; scripts/fail-dump.sh "$(ls -t logs/bench/*.log | head -1)" "logs/faildump/$(date +%H%M).txt"
fi
for h in asia1 europe1 america1; do echo "== $h"; ssh $h 'sudo journalctl -u isureal-go --since "-6 min" --no-pager -o cat | grep -E "mem-logrepl" | grep -vE "ready slot" | tail -4 | cut -c1-200' </dev/null; done
ssh asia2 'sudo journalctl -u pg-logrepl-watchdog --since "-6 min" --no-pager -o cat | tail -8 | cut -c1-200' </dev/null
[[ ${KEEP:-0} == 1 ]] && { echo "KEEP=1: leaving LOGREPL on"; exit 0; }
echo "== revert"
ssh asia2 'echo off | sudo tee /etc/postgresql/16/main/pg-logrepl.mode >/dev/null; sleep 4; sudo systemctl disable --now pg-logrepl-watchdog; sudo rm -f /etc/postgresql/16/main/conf.d/zz-zlogrepl.conf; sudo systemctl enable --now pg-sync-watchdog 2>/dev/null || true' </dev/null
ssh asia2 "sudo -u postgres psql -AtX -c 'ALTER SYSTEM RESET synchronous_standby_names' -c 'select pg_reload_conf()'" </dev/null
scripts/deploy.sh 2>&1 | tail -2
sleep 3
psql2 "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_type='logical' and not active"
psql2 "select count(*) from pg_replication_slots"; ssh asia2 "sudo -u postgres psql -AtX -c 'show synchronous_standby_names'" </dev/null
