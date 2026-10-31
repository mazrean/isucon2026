#!/usr/bin/env bash
# Local Rust server for diff tests: :8081 against the local PG (15433), proxy → :8080.
# usage: run-rs.sh <workdir>
set -euo pipefail
work=$1
here=$(cd -- "$(dirname -- "$0")" && pwd)
export TZ=UTC ISUREAL_DB_HOST=127.0.0.1 ISUREAL_DB_PORT=${DIFF_PGPORT:-15434} ISUREAL_LOG_LEVEL=error
export ISUREAL_IMAGE_CACHE_DIR="$work/imgcache-go" ISUREAL_RS_LISTEN=127.0.0.1:8081 ISUREAL_RS_UPSTREAM=127.0.0.1:8080
exec "$here/../isureal-rs/target/x86_64-unknown-linux-musl/release/isureal-rs" >"$work/rs.log" 2>&1
