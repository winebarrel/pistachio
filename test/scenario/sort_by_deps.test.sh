#!/usr/bin/env bash
# Scenario test: dump --sort-by-deps -> psql restore round-trip.
# Verifies that a dependency-ordered dump loads top to bottom with psql
# (no forward references), and that the default name-ordered dump does not.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/sort_by_deps"

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: dump --sort-by-deps orders dependencies first ---
step "01 dump --sort-by-deps orders dependencies first"
if ! dump_output=$("$PISTA" dump --sort-by-deps 2>&1); then
  fail "dump failed: $dump_output"
else
  echo "$dump_output" > "$tmp_dir/deps.sql"
  zref_line=$(grep -n '^-- public.zref' "$tmp_dir/deps.sql" | cut -d: -f1)
  aref_line=$(grep -n '^-- public.aref' "$tmp_dir/deps.sql" | cut -d: -f1)
  base_line=$(grep -n '^-- public.z_base' "$tmp_dir/deps.sql" | cut -d: -f1)
  deriv_line=$(grep -n '^-- public.a_derived' "$tmp_dir/deps.sql" | cut -d: -f1)
  if [ "$zref_line" -lt "$aref_line" ] && [ "$base_line" -lt "$deriv_line" ]; then
    pass
  else
    fail "referenced objects not ordered before their dependents"
    echo "    $dump_output" >&2
  fi
fi

# --- Step 2: psql restores the --sort-by-deps dump into an empty schema ---
step "02 psql restores --sort-by-deps dump"
setup_db ""
if restore_output=$(psql -X "$PISTA_CONN_STR" -q -v ON_ERROR_STOP=1 -f "$tmp_dir/deps.sql" 2>&1); then
  pass
else
  fail "psql restore failed: $restore_output"
fi

# --- Step 3: restored schema has no drift ---
step "03 restored schema has no drift"
if ! plan_output=$("$PISTA" plan "$tmp_dir/deps.sql" 2>&1); then
  fail "plan failed: $plan_output"
elif echo "$plan_output" | grep -q 'No changes'; then
  pass
else
  fail "drift after psql restore"
  echo "    $plan_output" >&2
fi

# --- Step 4: default name-ordered dump fails to restore with psql ---
# This confirms --sort-by-deps is what makes the dump replayable here.
step "04 default dump fails psql restore (forward reference)"
setup_db "$DATA/init.sql"
if ! default_output=$("$PISTA" dump 2>&1); then
  fail "dump failed: $default_output"
else
  echo "$default_output" > "$tmp_dir/default.sql"
  setup_db ""
  if psql -X "$PISTA_CONN_STR" -q -v ON_ERROR_STOP=1 -f "$tmp_dir/default.sql" >/dev/null 2>&1; then
    fail "default-order dump restored without error (expected forward-reference failure)"
  else
    pass
  fi
fi

summary
