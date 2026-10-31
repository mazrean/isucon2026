#!/usr/bin/env bash
# usage: scripts/reboot-test.sh   (driver only) -- lock -> bare deploy -> snapshot data -> reboot all 4 hosts
# (ORDER=parallel (default, like the post-contest check) | serial = asia2, asia1, europe1, america1)
# -> wait for new boot_ids and the app on :8080 -> compare data snapshot -> final-check.sh. No bench.
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p /tmp/isucon
exec 9>>/tmp/isucon/env.lock
flock -w 1200 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner)" >&2; exit 75; }
echo "$(date +%T) driver reboot-test $(git rev-parse --short HEAD)" >| /tmp/isucon/env.lock.owner
export LOCK_HELD=1
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=5)
if [ "${SKIP_DEPLOY:-0}" != 1 ]; then
  scripts/deploy.sh 2>&1 | tail -3 || exit 4
fi
HOSTS=(asia2 asia1 europe1 america1)
snap() {
  timeout 30 "${SSH[@]}" asia2 "sudo -u postgres psql -d isureal -Atc \"select 'posts', count(*), max(created_at) from posts union all select 'comments', count(*), max(created_at) from comments union all select 'friendships', count(*), null from friendships union all select 'sessions', count(*), null from login_sessions union all select 'friend_requests', count(*), null from friend_requests\"" </dev/null
  timeout 30 "${SSH[@]}" asia1 'cd /home/isucon/asialocalimg && ls -t meta/*/* 2>/dev/null | head -5 | while read f; do id=${f##*/}; echo "$id $(cat data/${id:0:2}/$id 2>/dev/null | sha256sum | cut -c1-16) $(cat $f | sha256sum | cut -c1-16)"; done; find data -type f | wc -l; ls seg/*.seg 2>/dev/null | while read f; do echo "$f $(stat -c %s $f) $(sha256sum < $f | cut -c1-16)"; done' </dev/null
}
before=$(snap); echo "$before"
declare -A old
for h in "${HOSTS[@]}"; do old[$h]=$(timeout 15 "${SSH[@]}" $h cat /proc/sys/kernel/random/boot_id </dev/null); done
t0=$(date +%s)
if [ "${ORDER:-parallel}" = parallel ]; then
  for h in "${HOSTS[@]}"; do timeout 15 "${SSH[@]}" $h sudo systemctl reboot </dev/null >/dev/null 2>&1 & done; wait
fi
for h in "${HOSTS[@]}"; do
  [ "${ORDER:-parallel}" = parallel ] || timeout 15 "${SSH[@]}" $h sudo systemctl reboot </dev/null >/dev/null 2>&1
  for _ in $(seq 1 120); do
    sleep 5
    n=$(timeout 8 "${SSH[@]}" $h cat /proc/sys/kernel/random/boot_id </dev/null 2>/dev/null)
    [ -n "$n" ] && [ "$n" != "${old[$h]}" ] && break
  done
  [ -n "$n" ] && [ "$n" != "${old[$h]}" ] || { echo "NG: $h did not come back"; exit 5; }
  for _ in $(seq 1 60); do
    code=$(timeout 8 "${SSH[@]}" $h "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/api/posting-window/current" </dev/null 2>/dev/null)
    [ -n "$code" ] && [ "$code" != 000 ] && break
    sleep 3
  done
  echo "$(date +%T) $h up: new boot_id, app http=$code, $(( $(date +%s) - t0 ))s"
  if [ $h = asia2 ]; then
    timeout 15 "${SSH[@]}" $h "systemctl is-active postgresql@16-main; sudo journalctl -u postgresql@16-main -b -o short-monotonic --no-pager | grep -m1 'ready to accept'" </dev/null
  fi
done
after=$(snap)
if [ "$before" = "$after" ]; then echo "DATA OK: snapshot identical"; else echo "DATA NG: snapshot differs"; diff <(echo "$before") <(echo "$after"); fi
scripts/final-check.sh 2>&1 | tail -8
