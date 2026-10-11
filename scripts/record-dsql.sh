#!/usr/bin/env bash
# Records testdata/dsql/dsql.yaml again against a live Aurora DSQL cluster.
#
# DSQL_HOST names the cluster endpoint. DSQL_REGION defaults to the region in
# the endpoint. The AWS CLI must be signed in to an identity that may connect
# as admin, and the public schema of the cluster must be empty: TestDSQL
# starts from nothing and drops what it creates.
#
# The recording is committed, so the run fails when the file names the
# cluster or holds something that looks like an AWS credential.
set -euo pipefail

: "${DSQL_HOST:?set DSQL_HOST to the cluster endpoint}"
region=${DSQL_REGION:-$(cut -d. -f3 <<<"$DSQL_HOST")}
recording=testdata/dsql/dsql.yaml

PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname "$DSQL_HOST" --region "$region")
export PGPASSWORD
# The port is written out: make exports PGPORT for the local server, and a
# connection string without one would take it.
export TEST_PISTA_DSQL_CONN_STR="postgres://admin@$DSQL_HOST:5432/postgres?sslmode=require"

# The file is not removed first: pgstub leaves it alone when the answers are
# the same, so recording again with no change leaves no diff. pgstub writes
# the recording even when the test fails, so the check below runs either
# way, and the test's status is the script's in the end.
status=0
go test -count=1 -v -run '^TestDSQL$' . || status=$?

cluster=${DSQL_HOST%%.*}
if [ -f "$recording" ] && grep -nEi -e "$cluster" -e 'on\.aws|amazonaws\.com|arn:aws' -e '(AKIA|ASIA)[0-9A-Z]{16}' -e 'X-Amz-' "$recording"; then
  echo "error: $recording names the cluster or holds a credential; do not commit it" >&2
  exit 1
fi
exit "$status"
