#!/usr/bin/env bash
# usage: scripts/portal-bench.sh run|status [job_id]   (driver only)
# portal: isuxportal protobuf. enqueue body = field1 varint target_id(346) = CNoC
# job states: 0 PENDING, 1 RUNNING, 2 ERRORED, 3 CANCELLED, 4 FINISHED
set -euo pipefail
cd "$(dirname "$0")/.."
F=scripts/portal-fetch.sh
LOGDIR=logs/bench; mkdir -p "$LOGDIR"
ENQ_B64=${ENQ_B64:-CNoC}
poll() {
  local id=$1 s
  while :; do
    s=$($F GET /api/contestant/benchmark_jobs/$id 2>/dev/null | python3 -I scripts/pbjob.py state || echo x)
    [[ $s == 2 || $s == 3 || $s == 4 ]] && break
    echo "$(date +%T) job $id state=$s" >&2; sleep 15
  done
  $F GET /api/contestant/benchmark_jobs/$id 2>/dev/null >"$LOGDIR/job$id.pb"
  python3 -I scripts/pbjob.py full <"$LOGDIR/job$id.pb" | tee "$LOGDIR/job$id.log"
}
case ${1:-run} in
run)
  mkdir -p /tmp/isucon
  if [[ -z ${LOCK_HELD:-} ]]; then exec 9>/tmp/isucon/env.lock; flock -w 900 9 || { echo "env busy" >&2; exit 75; }; echo "$(date +%T) OFFICIAL bench $(git rev-parse --short HEAD)" >| /tmp/isucon/env.lock.owner; fi
  id=$($F POST /api/contestant/benchmark_jobs "$ENQ_B64" | python3 -I scripts/pbraw.py | awk '/^  1: /{print $2; exit}')
  [[ -n $id ]] || { echo "enqueue failed (already queued?)" >&2; exit 11; }
  echo "enqueued job $id" >&2
  poll "$id" ;;
status)
  if [[ -n ${2:-} ]]; then poll "$2"; else $F GET /api/contestant/benchmark_jobs | python3 -I scripts/pbraw.py | head -40; fi ;;
esac
