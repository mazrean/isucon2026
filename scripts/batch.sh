#!/usr/bin/env bash
# usage: scripts/batch.sh "<note>"   (driver only) -- lock -> deploy -> official bench -> append score to logs/SCORES.tsv
# extra env (FWD_TARGET, FWD_GET, ...) is passed through to deploy.sh
set -uo pipefail
cd "$(dirname "$0")/.."
NOTE=${1:-}
mkdir -p /tmp/isucon logs
exec 9>>/tmp/isucon/env.lock
flock -w 1200 9 || { echo "env busy: $(cat /tmp/isucon/env.lock.owner)" >&2; exit 75; }
echo "$(date +%T) driver batch $(git rev-parse --short HEAD) $NOTE" >| /tmp/isucon/env.lock.owner
export LOCK_HELD=1
scripts/deploy.sh 2>&1 | tail -5 || exit 4
out=$(scripts/portal-bench.sh run 2>&1)
line=$(grep 'job=' <<<"$out" | tail -1)
echo "$out" | grep -E '整合性|失敗|"messages"' -A3 | head -8
echo "$line"
if grep -q 'pass=false' <<<"$line"; then
  mkdir -p logs/faildump
  scripts/fail-dump.sh "$(ls -t logs/bench/*.log | head -1)" "logs/faildump/$(date +%H%M).txt"
fi
printf '%s\t%s\t%s\t%s\n' "$(date +%H:%M)" "$(git rev-parse --short HEAD)" "$line" "$NOTE" >> logs/SCORES.tsv
