#!/usr/bin/env bash
# Private PG16 for the Rust diff test (port 15434), cloned from the shared
# local PG on 15433 (which other agents re-initialize concurrently).
# usage: pg-own.sh <workdir>
set -euo pipefail
work=$1
bin=/tmp/claude-1000/pgdl/root/usr/lib/postgresql/16/bin
data=$work/pgdata
sock=/tmp/claude-1000/pgsock-rs
mkdir -p "$sock"
if [[ ! -d $data ]]; then
	"$bin/initdb" -D "$data" -U postgres --auth=trust >/dev/null
fi
"$bin/pg_ctl" -D "$data" -l "$work/pg.log" -o "-p 15434 -k $sock -c timezone=UTC -c fsync=off -c synchronous_commit=off -c max_connections=200 -c shared_buffers=256MB" start
sleep 2
psql -h 127.0.0.1 -p 15434 -U postgres -c "CREATE ROLE isureal LOGIN PASSWORD 'isureal' SUPERUSER" || true
psql -h 127.0.0.1 -p 15434 -U postgres -c "CREATE DATABASE isureal OWNER isureal" || true
PGPASSWORD=isureal pg_dump -h 127.0.0.1 -p 15433 -U isureal -d isureal --no-owner --no-privileges |
	PGPASSWORD=isureal psql -q -h 127.0.0.1 -p 15434 -U isureal -d isureal >"$work/restore.log" 2>&1
echo restored
