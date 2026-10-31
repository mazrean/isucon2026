#!/usr/bin/env bash
# usage: scripts/pseudobench.sh check|load [secs] [extra flags...]
#   check: initialize → 整合性チェック (FAIL があれば exit 1)
#   load : initialize → 疑似負荷 secs 秒 (最大 30, 既定 25)
# 環境変数: PSEUDOBENCH_TARGET (既定 https://57.183.58.128), PSEUDOBENCH_NOLOCK=1 でロック確認を省略
set -euo pipefail
mode=${1:-check}
shift || true
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
bin=/tmp/isucon/pseudobench.$$
mkdir -p /tmp/isucon
(cd "$root/tools/pseudobench" && go build -o "$bin" .)
trap 'rm -f "$bin"' EXIT

run() {
	if [[ ${PSEUDOBENCH_NOLOCK:-} == 1 ]]; then
		"$bin" "$@"
	else
		# 他メンバーがデプロイ/計測中 (lock 保持中) なら initialize を打たない。実行中は lock を保持する。
		flock -n -E 75 /tmp/isucon/env.lock "$bin" "$@" || {
			rc=$?
			[[ $rc == 75 ]] && echo "pseudobench: /tmp/isucon/env.lock is held ($(cat /tmp/isucon/env.lock.owner 2>/dev/null)); wait and retry" >&2
			return $rc
		}
	fi
}

case $mode in
check)
	run check "$@"
	;;
load)
	secs=25
	if [[ ${1:-} =~ ^[0-9]+$ ]]; then
		secs=$1
		shift
	fi
	run load -secs "$secs" "$@"
	;;
*)
	echo "usage: $0 check|load [secs]" >&2
	exit 2
	;;
esac
