#!/usr/bin/env bash
# Runs the DSQL scenario tests against a live Aurora DSQL cluster. CI does not
# run them.
#
# DSQL_HOST names the cluster endpoint. DSQL_REGION defaults to the region in
# the endpoint. The AWS CLI must be signed in to an identity that may connect
# as admin, and the public schema of the cluster must be empty: each scenario
# starts from nothing and drops what it creates.
#
# The schemas come from aws-samples/aurora-dsql-samples, downloaded at a pinned
# commit into a temporary directory. To move to a newer commit, resolve it with
#   git ls-remote https://github.com/aws-samples/aurora-dsql-samples HEAD
# and replace SAMPLES_SHA.
set -euo pipefail

cd "$(dirname "$0")/../.."

: "${DSQL_HOST:?set DSQL_HOST to the cluster endpoint}"
region=${DSQL_REGION:-$(cut -d. -f3 <<<"$DSQL_HOST")}

SAMPLES_SHA=26aeb81819f0f48fdb4f520469ba7ed39dd4be09
SAMPLES_URL=https://raw.githubusercontent.com/aws-samples/aurora-dsql-samples/$SAMPLES_SHA

DSQL_SAMPLES=$(mktemp -d)
trap 'rm -rf "$DSQL_SAMPLES"' EXIT
export DSQL_SAMPLES

echo "Downloading samples..."
curl -fsSL "$SAMPLES_URL/sample-amazon-aurora-dsql-agent/infra/schema.sql" -o "$DSQL_SAMPLES/agent.sql"
curl -fsSL "$SAMPLES_URL/typescript/drizzle/drizzle/0000_init.sql" -o "$DSQL_SAMPLES/drizzle_0000.sql"
curl -fsSL "$SAMPLES_URL/typescript/drizzle/drizzle/0001_enable_foreign_keys.sql" -o "$DSQL_SAMPLES/drizzle_0001.sql"

echo "Building pista..."
go build -o pista ./cmd/pista
export PISTA="./pista"

# A token lasts 15 minutes by default, which a run can outlast.
PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname "$DSQL_HOST" --region "$region" --expires-in 3600)
export PGPASSWORD
# The port is written out: make exports PGPORT for the local server, and a
# connection string without one would take it.
export PISTA_CONN_STR="postgres://admin@$DSQL_HOST:5432/postgres?sslmode=require"
export PISTA_ENGINE=dsql
# The samples write CREATE INDEX ASYNC.
export PISTA_DSQL_IGNORE_ASYNC=true

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
