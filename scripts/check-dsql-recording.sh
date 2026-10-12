#!/usr/bin/env bash
# Records TestDSQL through internal/dsqlproxy in front of a local PostgreSQL
# server, and fails when the recording differs from testdata/dsql/dsql.yaml,
# which a live Aurora DSQL cluster made. A difference means the proxy does not
# answer the way the cluster did, or the committed recording is out of date.
#
# The proxy runs on the server PGHOST, PGPORT and PGUSER name, in a database
# pista_dsql_recording that each run creates again. See test/dsql/proxy.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

recording=testdata/dsql/dsql.yaml

work=$(mktemp -d)
PROXY_PID=
cleanup() {
  if [ -n "$PROXY_PID" ]; then
    kill "$PROXY_PID" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

source test/dsql/proxy.sh
start_proxy pista_dsql_recording "$work"

# pgstub leaves a recording alone when the answers are the same, so recording
# over a copy of the committed file changes it only where the proxy answers
# differently.
cp "$recording" "$work/dsql.yaml"
status=0
TEST_PISTA_DSQL_CONN_STR="postgres://admin@$PROXY_ADDR/postgres?sslmode=disable" \
  TEST_PISTA_DSQL_RECORDING="$work/dsql.yaml" \
  go test -count=1 -v -run '^TestDSQL$' . || status=$?

if ! diff -u "$recording" "$work/dsql.yaml"; then
  echo "error: TestDSQL recorded through the proxy differs from $recording" >&2
  exit 1
fi
exit "$status"
