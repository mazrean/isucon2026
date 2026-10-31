#!/usr/bin/env bash
# pseudobench check against Go (:8080) and against the Rust front (:8081, which
# proxies everything it does not serve to Go); the PASS/FAIL lists must match.
# (Locally there is one asia server, so region-dependent items FAIL on both.)
# usage: pb-compare.sh <pseudobench-binary> <workdir>
set -uo pipefail
pb=$1
work=$2
timeout 300 "$pb" check -target http://127.0.0.1:8080 >"$work/pb-go.txt" 2>&1
timeout 300 "$pb" check -target http://127.0.0.1:8081 >"$work/pb-rs.txt" 2>&1
sed -E 's/ -- .*//' "$work/pb-go.txt" | grep '^\[' >"$work/pb-a.txt"
sed -E 's/ -- .*//' "$work/pb-rs.txt" | grep '^\[' >"$work/pb-b.txt"
echo "items: $(wc -l <"$work/pb-a.txt")  go PASS: $(grep -c '^\[PASS' "$work/pb-a.txt")  rs PASS: $(grep -c '^\[PASS' "$work/pb-b.txt")"
grep -iE 'memor|export|エクスポート|思い出' "$work/pb-b.txt"
if diff "$work/pb-a.txt" "$work/pb-b.txt"; then echo "PASS/FAIL lists identical"; else echo "DIFFERENT"; exit 1; fi
