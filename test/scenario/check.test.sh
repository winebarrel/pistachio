#!/usr/bin/env bash
# Scenario test: plan --check exit codes
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/check"

# Run pista plan --check and echo the exit code. Never fails the script
# itself; callers assert on the echoed code.
plan_check_rc() {
  local rc=0
  "$PISTA" plan --check "$@" >/dev/null 2>&1 || rc=$?
  echo "$rc"
}

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: diff present -> exit 2 ---
step "01 check exits 2 on diff"
rc=$(plan_check_rc "$DATA/steps/01_add_column.sql")
if [ "$rc" = "2" ]; then
  pass
else
  fail "expected exit code 2, got $rc"
fi

# --- Step 2: plan output is still printed with --check ---
step "02 check still prints the plan"
plan_output=$("$PISTA" plan --check "$DATA/steps/01_add_column.sql" 2>&1) && rc=0 || rc=$?
if [ "$rc" != "2" ]; then
  fail "expected exit code 2, got $rc"
elif echo "$plan_output" | grep -qF 'ADD COLUMN name'; then
  pass
else
  fail "expected ADD COLUMN name in plan output"
  echo "    $plan_output" >&2
fi

# --- Step 3: no diff -> exit 0 ---
step "03 check exits 0 after apply"
apply_output=$(pista_apply "$DATA/steps/01_add_column.sql") || { fail "apply failed: $apply_output"; true; }
rc=$(plan_check_rc "$DATA/steps/01_add_column.sql")
if [ "$rc" = "0" ]; then
  pass
else
  fail "expected exit code 0, got $rc"
fi

# --- Step 4: suppressed drop only -> exit 0 ---
step "04 check exits 0 on skipped drop only"
rc=$(plan_check_rc "$DATA/steps/02_drop_table.sql")
if [ "$rc" = "0" ]; then
  pass
else
  fail "expected exit code 0, got $rc"
fi

# --- Step 5: suppressed drop allowed -> exit 2 ---
step "05 check exits 2 when drop is allowed"
rc=0
"$PISTA" plan --check --allow-drop all "$DATA/steps/02_drop_table.sql" >/dev/null 2>&1 || rc=$?
if [ "$rc" = "2" ]; then
  pass
else
  fail "expected exit code 2, got $rc"
fi

# --- Step 6: error -> exit 1 ---
step "06 check exits 1 on error"
rc=0
"$PISTA" -c postgres://invalid@localhost:1/none plan --check "$DATA/steps/01_add_column.sql" >/dev/null 2>&1 || rc=$?
if [ "$rc" = "1" ]; then
  pass
else
  fail "expected exit code 1, got $rc"
fi

summary
