#!/usr/bin/env bash
# Runs the DSQL scenario tests against a live Aurora DSQL cluster, or with
# DSQL_PROXY set, against the proxy in test/dsql/proxy in front of a local
# PostgreSQL server.
#
# DSQL_HOST names the cluster endpoint. DSQL_REGION defaults to the region in
# the endpoint. The AWS CLI must be signed in to an identity that may connect
# as admin, and the public schema of the cluster must be empty: each scenario
# starts from nothing and drops what it creates.
#
# The proxy runs on the server PGHOST, PGPORT and PGUSER name, in a database
# pista_dsql that each run creates again. The role needs CREATEDB.
#
# The schemas come from aws-samples/aurora-dsql-samples, downloaded at a pinned
# commit into a temporary directory. To move to a newer commit, resolve it with
#   git ls-remote https://github.com/aws-samples/aurora-dsql-samples HEAD
# and replace SAMPLES_SHA.
set -euo pipefail

cd "$(dirname "$0")/../.."

if [ -z "${DSQL_PROXY:-}" ]; then
  : "${DSQL_HOST:?set DSQL_HOST to the cluster endpoint, or DSQL_PROXY to run against the proxy}"
  region=${DSQL_REGION:-$(cut -d. -f3 <<<"$DSQL_HOST")}
fi

SAMPLES_SHA=26aeb81819f0f48fdb4f520469ba7ed39dd4be09
SAMPLES_URL=https://raw.githubusercontent.com/aws-samples/aurora-dsql-samples/$SAMPLES_SHA

DSQL_SAMPLES=$(mktemp -d)
proxy_pid=
cleanup() {
  if [ -n "$proxy_pid" ]; then
    kill "$proxy_pid" 2>/dev/null || true
  fi
  rm -rf "$DSQL_SAMPLES"
}
trap cleanup EXIT
export DSQL_SAMPLES

echo "Downloading samples..."
curl -fsSL "$SAMPLES_URL/sample-amazon-aurora-dsql-agent/infra/schema.sql" -o "$DSQL_SAMPLES/agent.sql"
curl -fsSL "$SAMPLES_URL/typescript/drizzle/drizzle/0000_init.sql" -o "$DSQL_SAMPLES/drizzle_0000.sql"
curl -fsSL "$SAMPLES_URL/typescript/drizzle/drizzle/0001_enable_foreign_keys.sql" -o "$DSQL_SAMPLES/drizzle_0001.sql"

echo "Building pista..."
go build -o pista ./cmd/pista
export PISTA="./pista"

if [ -n "${DSQL_PROXY:-}" ]; then
  echo "Starting the proxy..."
  backend="postgres://${PGUSER:-postgres}@${PGHOST:-localhost}:${PGPORT:-5432}"
  psql -X -q -v ON_ERROR_STOP=1 "$backend/postgres" \
    -c 'DROP DATABASE IF EXISTS pista_dsql' -c 'CREATE DATABASE pista_dsql'
  go build -o "$DSQL_SAMPLES/proxy" ./test/dsql/proxy
  "$DSQL_SAMPLES/proxy" -backend "$backend/pista_dsql?sslmode=disable" > "$DSQL_SAMPLES/addr" &
  proxy_pid=$!
  # The proxy prints its address once it listens.
  for _ in $(seq 100); do
    [ -s "$DSQL_SAMPLES/addr" ] && break
    kill -0 "$proxy_pid" 2>/dev/null || { echo "The proxy did not start." >&2; exit 1; }
    sleep 0.1
  done
  export PISTA_CONN_STR="postgres://admin@$(head -1 "$DSQL_SAMPLES/addr")/postgres?sslmode=disable"
else
  # A token lasts 15 minutes by default, which a run can outlast.
  PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname "$DSQL_HOST" --region "$region" --expires-in 3600)
  export PGPASSWORD
  # The port is written out: make exports PGPORT for the local server, and a
  # connection string without one would take it.
  export PISTA_CONN_STR="postgres://admin@$DSQL_HOST:5432/postgres?sslmode=require"
fi
export PISTA_ENGINE=dsql

rc=0
for script in test/dsql/*.test.sh; do
  echo ""
  echo "=== $(basename "$script" .test.sh) ==="
  if bash "$script"; then
    :
  else
    rc=1
  fi
done

echo ""
if [ "$rc" -eq 0 ]; then
  echo "All DSQL scenarios passed."
else
  echo "Some DSQL scenarios failed."
fi

exit "$rc"
