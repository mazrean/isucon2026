#!/usr/bin/env bash
# usage: scripts/reboot-test.sh   (driver only) -- lock -> bare deploy -> reboot asia2, asia1, europe1, america1 one by one
# (wait for a new boot_id and the app on :8080) -> final-check.sh. No bench.
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
for h in asia2 asia1 europe1 america1; do
  old=$(timeout 15 "${SSH[@]}" $h cat /proc/sys/kernel/random/boot_id </dev/null)
  t0=$(date +%s)
  timeout 15 "${SSH[@]}" $h sudo systemctl reboot </dev/null >/dev/null 2>&1
  for _ in $(seq 1 120); do
    sleep 5
    n=$(timeout 8 "${SSH[@]}" $h cat /proc/sys/kernel/random/boot_id </dev/null 2>/dev/null)
    [ -n "$n" ] && [ "$n" != "$old" ] && break
  done
  [ -n "$n" ] && [ "$n" != "$old" ] || { echo "NG: $h did not come back"; exit 5; }
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
scripts/final-check.sh 2>&1 | tail -8
