#!/bin/bash
# asia2 (primary) 上で動く synchronous_standby_names の watchdog。
# /usr/local/bin/pg-sync-watchdog として配置、pg-sync-watchdog.service から起動。
#   reset : sync 設定を空にする (PG 起動前に実行 → 起動直後に書き込みがブロックされない)
#   run   : 1 秒ごとに監視。mode=on のとき健全なスタンバイだけで 'ANY n (...)' を組む
# 希望モード: /etc/postgresql/16/main/pg-sync.mode (on|off, scripts/pg-sync.sh が書く)
# 管理する conf: /etc/postgresql/16/main/conf.d/zz-sync.conf
# 順序保証 (isucon_sync_members):
#   外す時  : in_sync=false を commit (synchronous_commit=local) → 集合から除外 → reload
#   戻す時  : 集合に追加 → reload → sync_state が quorum/sync を確認 → in_sync=true
set -u
STANDBYS=(asia1 europe1 america1)
MODE_FILE=/etc/postgresql/16/main/pg-sync.mode
CONF=/etc/postgresql/16/main/conf.d/zz-sync.conf
DB=isureal
DOWN_SEC=5      # この秒数以上 不健全なら外す
UP_SEC=2        # この秒数以上 健全なら戻す
MAX_LAG=16777216 # sent - replay がこれ以下 (bytes)
log() { echo "$(date '+%F %T') $*"; }
psqlq() { psql -X -q -At -d "$DB" -v ON_ERROR_STOP=1 "$@"; }

write_conf() { # $1 = names (space separated, may be empty), $2 = mode
  local names="$1" val=""
  if [ -n "$names" ]; then
    local n; n=$(wc -w <<<"$names"); val="ANY $n ($(tr ' ' ',' <<<"$names"))"
  fi
  local sc=on; [ "$2" = on ] && sc=remote_apply
  printf "# managed by pg-sync-watchdog %s\nsynchronous_standby_names = '%s'\nsynchronous_commit = %s\n" "$(date '+%F %T')" "$val" "$sc" > "$CONF.tmp"
  mv -f "$CONF.tmp" "$CONF"
}
reload() { psqlq -c "select pg_reload_conf()" >/dev/null; }
set_flag() { # $1 name, $2 true|false
  psqlq -c "set synchronous_commit=local; set statement_timeout='3s'; update isucon_sync_members set in_sync=$2, updated_at=now() where name='$1'" >/dev/null
}

if [ "${1:-}" = reset ]; then
  write_conf "" off; psqlq -c "select pg_reload_conf()" >/dev/null 2>&1 || true
  log "reset: synchronous_standby_names=''"; exit 0
fi

declare -A last_ok first_ok flag
cur=""        # 現在 synchronous_standby_names に入れている集合
inited=0
while true; do
  sleep 1
  now=$(date +%s)
  mode=$(cat "$MODE_FILE" 2>/dev/null || echo off); [ "$mode" = on ] || mode=off
  out=$(psqlq -F'|' -c "
    select s.n,
      coalesce((select r.state='streaming' and r.reply_time > now()-interval '3 s'
                       and pg_wal_lsn_diff(r.sent_lsn, r.replay_lsn) < $MAX_LAG
                from pg_stat_replication r where r.application_name=s.n
                order by r.reply_time desc limit 1), false)
      and coalesce((select wal_status not in ('lost','unreserved') from pg_replication_slots where slot_name=s.n), false),
      coalesce((select r.sync_state from pg_stat_replication r where r.application_name=s.n order by r.reply_time desc limit 1),'none')
    from unnest(array['asia1','europe1','america1']) s(n)" 2>/dev/null) || { inited=0; continue; }
  if [ $inited = 0 ]; then
    # 起動時: すべて in_sync=false、sync 集合は空から始める
    ok=1; for h in "${STANDBYS[@]}"; do set_flag "$h" false || ok=0; flag[$h]=false; first_ok[$h]=0; last_ok[$h]=$now; done
    [ $ok = 1 ] || continue
    cur=""; write_conf "" "$mode"; reload; inited=1; log "init: mode=$mode flags=false set=''"; continue
  fi
  declare -A healthy=() sstate=()
  while IFS='|' read -r n h st; do healthy[$n]=$h; sstate[$n]=$st; done <<<"$out"
  new="$cur"
  for h in "${STANDBYS[@]}"; do
    if [ "${healthy[$h]}" = t ]; then
      last_ok[$h]=$now; [ "${first_ok[$h]}" = 0 ] && first_ok[$h]=$now
    else
      first_ok[$h]=0
    fi
    inset=0; [[ " $cur " == *" $h "* ]] && inset=1
    if [ $mode = off ] || [ $((now - last_ok[$h])) -ge $DOWN_SEC ]; then
      want=0
    elif [ $inset = 1 ]; then
      want=1
    elif [ "${first_ok[$h]}" != 0 ] && [ $((now - first_ok[$h])) -ge $UP_SEC ]; then
      want=1
    else
      want=0
    fi
    if [ $want = 0 ] && [ "${flag[$h]}" != false ]; then
      if set_flag "$h" false; then log "$h in_sync=false (healthy=${healthy[$h]} mode=$mode)"
      else log "WARN: $h in_sync=false の書き込み失敗 (書き込み停止回避のため除外は続行)"; fi
      flag[$h]=false
    fi
    if [ $want = 0 ] && [ $inset = 1 ]; then
      new=$(tr ' ' '\n' <<<"$new" | grep -vx "$h" | xargs)
    elif [ $want = 1 ] && [ $inset = 0 ]; then
      new=$(echo $new $h | tr ' ' '\n' | sort -u | xargs)
    fi
  done
  if [ "$new" != "$cur" ]; then
    write_conf "$new" "$mode"; reload; log "set synchronous_standby_names: '$cur' -> '$new' (mode=$mode)"; cur="$new"
    continue   # sync_state の反映は次周で確認
  fi
  for h in $cur; do
    if [ "${flag[$h]}" != true ] && [[ "${sstate[$h]}" == quorum || "${sstate[$h]}" == sync ]] && [ "${healthy[$h]}" = t ]; then
      if set_flag "$h" true; then flag[$h]=true; log "$h in_sync=true (sync_state=${sstate[$h]})"; fi
    fi
  done
  # mode が変わった場合 synchronous_commit を合わせる
  want_sc=on; [ $mode = on ] && want_sc=remote_apply
  grep -q "synchronous_commit = $want_sc\$" "$CONF" || { write_conf "$cur" "$mode"; reload; log "synchronous_commit -> $want_sc"; }
done
