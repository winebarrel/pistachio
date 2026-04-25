#!/usr/bin/env bash
# Scenario test: --omit-schema dump → plan round-trip
# Verifies that dump --omit-schema output can be fed back to plan
# with no diff, covering all object types including materialized views.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/omit_schema"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: dump --omit-schema produces no public. prefix ---
step "01 dump --omit-schema strips schema"
dump_output=$("$PIST" dump --omit-schema 2>&1)
if echo "$dump_output" | grep -q 'public\.'; then
  fail "dump output contains 'public.' prefix"
  echo "    $(echo "$dump_output" | grep 'public\.' | head -3)" >&2
else
  pass
fi

# --- Step 2: dump --omit-schema contains all object types ---
step "02 dump contains all object types"
has_all=true
echo "$dump_output" | grep -q 'CREATE TABLE' || has_all=false
echo "$dump_output" | grep -q 'CREATE OR REPLACE VIEW' || has_all=false
echo "$dump_output" | grep -q 'CREATE MATERIALIZED VIEW' || has_all=false
echo "$dump_output" | grep -q 'CREATE TYPE' || has_all=false
echo "$dump_output" | grep -q 'CREATE DOMAIN' || has_all=false
echo "$dump_output" | grep -q 'CREATE INDEX' || has_all=false
if $has_all; then
  pass
else
  fail "missing object types in dump"
  echo "    $dump_output" >&2
fi

# --- Step 3: dump --omit-schema → plan has no diff ---
step "03 dump --omit-schema → plan no diff"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
echo "$dump_output" > "$tmp_dir/schema.sql"
plan_output=$("$PIST" plan "$tmp_dir/schema.sql" 2>&1)
if echo "$plan_output" | grep -q 'No changes'; then
  pass
else
  fail "plan shows changes for omit-schema dump"
  echo "    $plan_output" >&2
fi

# --- Step 4: matview index is in dump --omit-schema ---
step "04 matview index in omit-schema dump"
if echo "$dump_output" | grep -q 'idx_user_stats_cnt'; then
  pass
else
  fail "matview index not found in dump"
fi

# --- Step 5: dump --omit-schema → apply to empty DB → no drift ---
step "05 dump --omit-schema → apply → no drift"
setup_db ""
apply_output=$("$PIST" apply "$tmp_dir/schema.sql" 2>&1) || { fail "apply failed: $apply_output"; summary; exit 1; }
plan_output=$("$PIST" plan "$tmp_dir/schema.sql" 2>&1)
if echo "$plan_output" | grep -q 'No changes'; then
  pass
else
  fail "drift after apply with omit-schema dump"
  echo "    $plan_output" >&2
fi

summary
