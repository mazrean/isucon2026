#!/usr/bin/env bash
# Trial of LOGREPL=1 on the hosts (no official bench). Run under the env lock via batch-less flock.
set -u
cd "$(dirname "$0")/.."
psql2() { ssh -o BatchMode=yes asia2 "sudo -u postgres psql -d isureal -AtX -c \"$1\"" </dev/null; }
LOGREPL=1 scripts/deploy.sh 2>&1 | tail -4
sleep 8
psql2 "select slot_name, active, confirmed_flush_lsn from pg_replication_slots"
psql2 "select application_name, state, sync_state, replay_lag from pg_stat_replication"
psql2 "insert into mem_logrepl_members(name,blocked) values ('asia1',false),('europe1',false),('america1',false) on conflict (name) do update set blocked=false"
ssh asia2 "sudo -u postgres psql -AtX -c \"ALTER SYSTEM SET synchronous_standby_names = 'ANY 3 (asia1,europe1,america1)'\" -c 'select pg_reload_conf()'" </dev/null
sleep 3
psql2 "select application_name, state, sync_state, replay_lag from pg_stat_replication"
for h in asia1 europe1 america1; do echo "== $h"; ssh $h 'sudo journalctl -u isureal-go --since "-1 min" --no-pager -o cat | grep -iE "logrepl|panic|error" | tail -6 | cut -c1-220' </dev/null; done
echo "== initialize"; curl -s -m 70 -o /dev/null -w "%{http_code} %{time_total}s\n" -X POST http://192.168.1.10/api/initialize
sleep 3
for h in asia1 europe1 america1; do echo "== $h"; ssh $h 'sudo journalctl -u isureal-go --since "-90 s" --no-pager -o cat | grep -iE "logrepl" | tail -4 | cut -c1-220' </dev/null; done
psql2 "select application_name, state, sync_state, replay_lag from pg_stat_replication"
echo "== app logrepl state after init"; for h in asia1 europe1 america1; do ssh $h 'sudo journalctl -u isureal-go --since "-2 min" --no-pager -o cat | grep -iE "logrepl" | grep -iE "ready|error|fail|lost" | tail -3 | cut -c1-220' </dev/null; done
[[ ${KEEP:-0} == 1 ]] && { echo "KEEP=1: leaving LOGREPL on"; exit 0; }
echo "== revert"
ssh asia2 "sudo -u postgres psql -AtX -c 'ALTER SYSTEM RESET synchronous_standby_names' -c 'select pg_reload_conf()'" </dev/null
scripts/deploy.sh 2>&1 | tail -2
sleep 3
psql2 "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_type='logical' and not active"
psql2 "select slot_name, active from pg_replication_slots"
ssh asia2 "sudo -u postgres psql -AtX -c 'show synchronous_standby_names'" </dev/null
