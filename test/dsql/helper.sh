#!/usr/bin/env bash
# Helpers for the DSQL scenario tests, on top of the CLI scenario helpers.
# run.sh sets the connection, --engine dsql and --dsql-ignore-async through
# the environment, so the scenario helpers run against DSQL unchanged.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../scenario/helper.sh"

: "${DSQL_SAMPLES:?run the DSQL scenarios with test/dsql/run.sh}"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# reset_db drops everything in public with pista itself: DSQL cannot drop and
# create the public schema the way setup_db does.
reset_db() {
  : > "$WORK/empty.sql"
  pista_apply "$WORK/empty.sql" > /dev/null
}

# derive writes a copy of a file with a perl substitution applied to the whole
# text, for a step that edits a downloaded sample.
# Usage: derive out_file src_file perl_expr
derive() {
  perl -0pe "$3" "$2" > "$1"
}
