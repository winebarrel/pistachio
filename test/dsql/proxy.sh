#!/usr/bin/env bash
# start_proxy serves internal/dsqlproxy in front of a database of its own on
# the server PGHOST, PGPORT and PGUSER name, and sets PROXY_ADDR to the
# address it listens on and PROXY_PID to its process. The database is created
# again on each call. The role must be a superuser: the proxy marks an index
# being built invalid by writing to pg_catalog.pg_index.
# Usage: start_proxy database work_dir
start_proxy() {
  local db=$1 dir=$2
  local backend="postgres://${PGUSER:-postgres}@${PGHOST:-localhost}:${PGPORT:-5432}"
  psql -X -q -v ON_ERROR_STOP=1 "$backend/postgres" \
    -c "DROP DATABASE IF EXISTS $db" -c "CREATE DATABASE $db"
  go build -o "$dir/proxy" ./test/dsql/proxy
  "$dir/proxy" -backend "$backend/$db?sslmode=disable" > "$dir/addr" &
  PROXY_PID=$!
  # The proxy prints its address once it listens.
  for _ in $(seq 100); do
    [ -s "$dir/addr" ] && break
    kill -0 "$PROXY_PID" 2>/dev/null || { echo "The proxy did not start." >&2; return 1; }
    sleep 0.1
  done
  PROXY_ADDR=$(head -1 "$dir/addr")
}
