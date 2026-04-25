#!/usr/bin/env bash
# Scenario test: --index-concurrently flag
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/index_concurrently"

pist_plan_concurrent() {
  "$PIST" plan --allow-drop all --index-concurrently "$@" 2>&1
}

pist_apply_concurrent() {
  "$PIST" apply --allow-drop all --index-concurrently "$@" 2>&1
}

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: ADD INDEX with CONCURRENTLY ---
step "01 add index (concurrently)"
plan_output=$(pist_plan_concurrent "$DATA/steps/01_add_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY'; then
  fail "expected CREATE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply_concurrent "$DATA/steps/01_add_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_concurrent "$DATA/steps/01_add_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 2: ADD UNIQUE INDEX with CONCURRENTLY ---
step "02 add unique index (concurrently)"
plan_output=$(pist_plan_concurrent "$DATA/steps/02_add_unique_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE UNIQUE INDEX CONCURRENTLY'; then
  fail "expected CREATE UNIQUE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply_concurrent "$DATA/steps/02_add_unique_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_concurrent "$DATA/steps/02_add_unique_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 3: CHANGE INDEX with CONCURRENTLY (drop + create) ---
step "03 change index (concurrently)"
plan_output=$(pist_plan_concurrent "$DATA/steps/03_change_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'DROP INDEX CONCURRENTLY'; then
  fail "expected DROP INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY'; then
  fail "expected CREATE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply_concurrent "$DATA/steps/03_change_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_concurrent "$DATA/steps/03_change_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 4: DROP INDEX with CONCURRENTLY ---
step "04 drop index (concurrently)"
plan_output=$(pist_plan_concurrent "$DATA/steps/04_drop_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'DROP INDEX CONCURRENTLY'; then
  fail "expected DROP INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply_concurrent "$DATA/steps/04_drop_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_concurrent "$DATA/steps/04_drop_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 5: per-index directive (no --index-concurrently flag) ---
# Reset to initial schema
setup_db "$DATA/init.sql"

step "05 per-index directive (pist:concurrently)"
plan_output=$(pist_plan "$DATA/steps/05_directive.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY idx_users_name'; then
  fail "expected CREATE INDEX CONCURRENTLY for idx_users_name"
  echo "    $plan_output" >&2
elif echo "$plan_output" | grep -qF 'CREATE UNIQUE INDEX CONCURRENTLY'; then
  fail "idx_users_email should NOT be CONCURRENTLY"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply "$DATA/steps/05_directive.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan "$DATA/steps/05_directive.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

summary
