#!/bin/bash
# asia2 (primary) 上で動く「論理レプリケーション版メモリモデル (ISUREAL_MEM_LOGREPL=1)」の watchdog。
# /usr/local/bin/pg-logrepl-watchdog として配置、pg-logrepl-watchdog.service から起動。
#   reset : 同期対象を空にする (PG 起動前に実行 → 起動直後に remote_apply の marker がブロックされない)
#   run   : 1 秒ごとに監視し、健全な app だけで synchronous_standby_names = 'ANY n (...)' を組む
#
# 対象: 各 app の購読 (application_name = スロット名 = ノード名。webapp/go/mem_logrepl.go の lrSlotName)
# 健全: pg_stat_replication で state=streaming、reply_time が新しい、送信済みと反映済み (replay_lsn) の差が小さい、
#       replay_lag が小さい (NULL = 遅れ無し扱い)、スロットが lost でない。
# app 側 (mem_logrepl.go checkLoop, 200ms ごと): 自分の walsender が sync/quorum かつ
#   mem_logrepl_members.blocked=false (行が無ければ blocked 扱い) のときだけメモリから読む。lease 1s。
# 順序保証:
#   外す時: blocked=true を commit (synchronous_commit=local) → その app の walsender を terminate
#           → 2.5s 待つ (app は blocked / 受信途絶で必ずメモリ読みをやめている) → 集合から除外 → reload
#   戻す時: 集合に追加 → reload → sync_state が quorum/sync を確認 → blocked=false
# 管理する conf: /etc/postgresql/16/main/conf.d/zz-zlogrepl.conf (zz-sync.conf より後に読まれるので、
#   synchronous_standby_names はこちらが勝つ)。synchronous_commit は触らない (app が marker で remote_apply を使う)。
#   pg-sync-watchdog (物理スタンバイ用) とは同じ GUC を奪い合うので同時に動かさない (unit で Conflicts)。
# 希望モード: /etc/postgresql/16/main/pg-logrepl.mode (on|off、無ければ on)。off なら全員 blocked + 集合は空。
set -u
read -r -a MEMBERS <<<"${LR_MEMBERS:-asia1 europe1 america1}"
MODE_FILE=${LR_MODE_FILE:-/etc/postgresql/16/main/pg-logrepl.mode}
CONF=${LR_CONF:-/etc/postgresql/16/main/conf.d/zz-zlogrepl.conf}
DB=${LR_DB:-isureal}
TABLE=mem_logrepl_members
DOWN_SEC=${LR_DOWN_SEC:-3}        # この秒数以上 不健全なら外す
UP_SEC=${LR_UP_SEC:-2}            # この秒数以上 健全なら戻す
MAX_LAG=${LR_MAX_LAG:-67108864}   # sent_lsn - replay_lsn の上限 (bytes)
MAX_LAG_SEC=${LR_MAX_LAG_SEC:-5}  # replay_lag の上限 (秒)
DRAIN_SEC=${LR_DRAIN_SEC:-2.5}    # blocked → 除外 の間に待つ秒数 (app の lease 1s + 確認間隔 200ms より十分長く)
log() { echo "$(date '+%F %T') $*"; }
psqlq() { psql -X -q -At -d "$DB" -v ON_ERROR_STOP=1 "$@"; }

write_conf() { # $1 = names (space separated, may be empty)
  local names="$1" val=""
  if [ -n "$names" ]; then
    local n; n=$(wc -w <<<"$names"); val="ANY $n ($(tr ' ' ',' <<<"$names"))"
  fi
  printf "# managed by pg-logrepl-watchdog %s\nsynchronous_standby_names = '%s'\n" "$(date '+%F %T')" "$val" > "$CONF.tmp"
  mv -f "$CONF.tmp" "$CONF"
}
reload() { psqlq -c "select pg_reload_conf()" >/dev/null; }
set_blocked() { # $1 name, $2 true|false
  psqlq -c "set synchronous_commit=local; set statement_timeout='3s';
    insert into $TABLE(name, blocked) values ('$1', $2) on conflict (name) do update set blocked = excluded.blocked" >/dev/null
}
terminate() { # $1 name: その app の walsender を切る (再接続しても blocked なので読まない)
  psqlq -c "select pg_terminate_backend(pid) from pg_stat_replication where application_name='$1'" >/dev/null
}
drain() { sleep "$DRAIN_SEC"; }

if [ "${1:-}" = reset ]; then
  write_conf ""; psqlq -c "select pg_reload_conf()" >/dev/null 2>&1 || true
  log "reset: synchronous_standby_names=''"; exit 0
fi

names_sql=$(printf "'%s'," "${MEMBERS[@]}"); names_sql="array[${names_sql%,}]"
declare -A last_ok first_ok blocked
cur=""        # 現在 synchronous_standby_names に入れている集合
inited=0
while true; do
  sleep 1
  now=$(date +%s)
  mode=$(cat "$MODE_FILE" 2>/dev/null || echo on); [ "$mode" = off ] || mode=on
  out=$(psqlq -F'|' -c "
    select s.n,
      coalesce((select r.state = 'streaming' and r.reply_time > now() - interval '3 s'
                       and coalesce(pg_wal_lsn_diff(r.sent_lsn, r.replay_lsn), 0) < $MAX_LAG
                       and coalesce(r.replay_lag, interval '0') < interval '$MAX_LAG_SEC s'
                from pg_stat_replication r where r.application_name = s.n
                order by r.reply_time desc nulls last limit 1), false)
      and coalesce((select wal_status not in ('lost', 'unreserved') from pg_replication_slots where slot_name = s.n), false),
      coalesce((select r.sync_state from pg_stat_replication r where r.application_name = s.n
                order by r.reply_time desc nulls last limit 1), 'none')
    from unnest($names_sql) s(n)" 2>/dev/null) || { inited=0; continue; }
  if [ $inited = 0 ]; then
    # 起動時: 全員 blocked にしてから集合を空にする (前回の集合が残っていても、読む app が居ない状態で外す)
    psqlq -c "set synchronous_commit=local; create table if not exists $TABLE (name text primary key, blocked bool not null default true)" >/dev/null 2>&1 || continue
    ok=1; for h in "${MEMBERS[@]}"; do set_blocked "$h" true || ok=0; blocked[$h]=true; first_ok[$h]=0; last_ok[$h]=0; done
    [ $ok = 1 ] || continue
    drain
    cur=""; write_conf ""; reload; inited=1; log "init: mode=$mode all blocked, set=''"; continue
  fi
  declare -A healthy=() sstate=()
  while IFS='|' read -r n h st; do healthy[$n]=$h; sstate[$n]=$st; done <<<"$out"
  removed=""
  added=""
  for h in "${MEMBERS[@]}"; do
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
    if [ $want = 0 ] && { [ $inset = 1 ] || [ "${blocked[$h]}" != true ]; }; then
      # 1) blocked=true  2) walsender を terminate
      if set_blocked "$h" true; then
        blocked[$h]=true
      else
        log "WARN: $h blocked=true の書き込み失敗 (lease 切れを待って除外は続行)"
      fi
      terminate "$h" || log "WARN: $h terminate 失敗"
      log "$h blocked=true, walsender terminated (healthy=${healthy[$h]} state=${sstate[$h]} mode=$mode)"
      [ $inset = 1 ] && removed="$removed $h"
    elif [ $want = 1 ] && [ $inset = 0 ]; then
      added="$added $h"
    fi
  done
  if [ -n "$removed" ]; then
    # 3) app がメモリ読みをやめるのを待つ  4) 集合から外して reload
    drain
    new=$cur; for h in $removed; do new=$(tr ' ' '\n' <<<"$new" | grep -vx "$h" | xargs); done
    write_conf "$new"; reload; log "set synchronous_standby_names: '$cur' -> '$new' (removed:$removed)"; cur="$new"
    continue
  fi
  if [ -n "$added" ]; then
    new=$(echo $cur $added | tr ' ' '\n' | sort -u | xargs)
    write_conf "$new"; reload; log "set synchronous_standby_names: '$cur' -> '$new' (added:$added)"; cur="$new"
    continue   # sync_state の反映は次周で確認してから blocked=false
  fi
  for h in $cur; do
    if [ "${blocked[$h]}" != false ] && [[ "${sstate[$h]}" == quorum || "${sstate[$h]}" == sync ]] && [ "${healthy[$h]}" = t ]; then
      if set_blocked "$h" false; then blocked[$h]=false; log "$h blocked=false (sync_state=${sstate[$h]})"; fi
    fi
  done
done
