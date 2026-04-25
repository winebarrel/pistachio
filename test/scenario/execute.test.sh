#!/usr/bin/env bash
# Scenario test: -- pist:execute directive
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/execute"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: execute adds function ---
step "01 execute creates function"
plan_output=$(pist_plan "$DATA/steps/01_add_function.sql") || { fail "plan failed: $plan_output"; true; }
if echo "$plan_output" | grep -qF 'pist:execute'; then
  # Apply and verify function exists
  apply_output=$(pist_apply "$DATA/steps/01_add_function.sql") || { fail "apply failed: $apply_output"; true; }
  func_exists=$(psql -X "$PIST_CONN_STR" -tAc "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'get_user_count')")
  if [ "$func_exists" = "t" ]; then
    pass
  else
    fail "function not created"
  fi
else
  fail "expected pist:execute in plan"
  echo "    $plan_output" >&2
fi

# --- Step 2: check SQL skips execution (function already exists) ---
step "02 check SQL skips existing function"
apply_output=$(pist_apply "$DATA/steps/02_function_exists_skip.sql") || { fail "apply failed: $apply_output"; true; }
# The function should NOT appear in apply output (skipped)
if echo "$apply_output" | grep -qF 'CREATE OR REPLACE FUNCTION'; then
  fail "function should have been skipped by check SQL"
  echo "    $apply_output" >&2
else
  pass
fi

# --- Step 3: execute without check always runs ---
step "03 execute without check always runs"
apply_output=$(pist_apply "$DATA/steps/03_always_execute.sql") || { fail "apply failed: $apply_output"; true; }
if echo "$apply_output" | grep -qF 'CREATE OR REPLACE FUNCTION'; then
  pass
else
  fail "expected function in apply output"
  echo "    $apply_output" >&2
fi

summary
