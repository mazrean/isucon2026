#!/usr/bin/env bash
# Local Go server for diff tests: :8080 against the local PG (15433).
# usage: run-go.sh <go-binary> <workdir>
set -euo pipefail
bin=$1
work=$2
here=$(cd -- "$(dirname -- "$0")" && pwd)
mkdir -p "$work/imgcache-go"
cd "$here/../../go"
export TZ=UTC ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=${DIFF_PGPORT:-15434} ISUREAL_LOG_LEVEL=error ISUREAL_MEM=1
export ISUREAL_IMAGE_CACHE_DIR="$work/imgcache-go"
exec "$bin" >"$work/go.log" 2>&1
