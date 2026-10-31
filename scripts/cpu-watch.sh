#!/usr/bin/env bash
# usage: scripts/cpu-watch.sh <secs> -> per-host mpstat summary (avg %usr %sys %idle) over the window
S=${1:-150}
for h in asia1 europe1 america1 asia2; do
  ( ssh -o BatchMode=yes -o ConnectTimeout=10 $h "mpstat 5 $((S/5)) | awk '/Average/ {print \"$h usr=\"\$3\" sys=\"\$5\" iowait=\"\$6\" soft=\"\$8\" idle=\"\$NF}'; top -bn1 -o %CPU | sed -n 8,10p | awk '{print \"  \"\$9\"% \"\$12}'" </dev/null ) &
done; wait
