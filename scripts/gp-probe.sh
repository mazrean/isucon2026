#!/usr/bin/env bash
# usage: scripts/gp-probe.sh <outdir> -- waits for the next POST /api/initialize on asia1,
# then captures asia1 CPU profile + mpstat on asia1/asia2/europe1 over the late days + grand prix.
set -uo pipefail
OUT=${1:?outdir}; mkdir -p "$OUT"
DELAY=${DELAY:-40}; SECS=${SECS:-35}
base=$(ssh asia1 'sudo grep -c "POST /api/initialize" /var/log/nginx/access.log' </dev/null)
until [ "$(ssh asia1 'sudo grep -c "POST /api/initialize" /var/log/nginx/access.log' </dev/null)" -gt "$base" ]; do sleep 2; done
echo "initialize seen $(date +%T)"; sleep "$DELAY"
ssh asia1 "curl -s -o /tmp/gp.pprof 'http://127.0.0.1:6060/debug/pprof/profile?seconds=$SECS'" </dev/null &
for h in asia1 asia2 europe1; do ssh $h "mpstat 1 $SECS | awk -v h=$h '/^[0-9].*all/ {printf \"%s %s usr=%s sys=%s iow=%s idle=%s\n\", h, \$1, \$4, \$6, \$7, \$NF}'" </dev/null >"$OUT/mpstat.$h" & done
wait
scp -q asia1:/tmp/gp.pprof "$OUT/gp.pprof"; echo done
