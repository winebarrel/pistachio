#!/usr/bin/env bash
# Scenario test: inline CREATE INDEX CONCURRENTLY (without -- pist:concurrently directive)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/inline_concurrently"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: ADD inline CONCURRENTLY index → no drift after apply ---
step "01 add index (inline CONCURRENTLY)"
plan_output=$(pist_plan "$DATA/steps/01_add_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY'; then
  fail "expected CREATE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply "$DATA/steps/01_add_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan "$DATA/steps/01_add_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 2: CHANGE inline CONCURRENTLY index → DROP + CREATE both CONCURRENTLY ---
step "02 change index (inline CONCURRENTLY)"
plan_output=$(pist_plan "$DATA/steps/02_change_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'DROP INDEX CONCURRENTLY'; then
  fail "expected DROP INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY'; then
  fail "expected CREATE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply "$DATA/steps/02_change_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan "$DATA/steps/02_change_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 3: ADD UNIQUE inline CONCURRENTLY index ---
step "03 add unique index (inline CONCURRENTLY)"
plan_output=$(pist_plan "$DATA/steps/03_add_unique.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE UNIQUE INDEX CONCURRENTLY'; then
  fail "expected CREATE UNIQUE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply "$DATA/steps/03_add_unique.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan "$DATA/steps/03_add_unique.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 4: matview index with inline CONCURRENTLY ---
setup_db "$DATA/init.sql"

step "04 matview index (inline CONCURRENTLY)"
plan_output=$(pist_plan "$DATA/steps/04_matview_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY idx_user_names'; then
  fail "expected CREATE INDEX CONCURRENTLY idx_user_names in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply "$DATA/steps/04_matview_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan "$DATA/steps/04_matview_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 5: --with-tx without --disable-index-concurrently fails on inline form ---
setup_db "$DATA/init.sql"

step "05 with-tx + inline CONCURRENTLY fails"
apply_output=$("$PIST" apply --allow-drop all --with-tx "$DATA/steps/01_add_index.sql" 2>&1) && {
  fail "expected apply to fail with --with-tx + inline CONCURRENTLY"
  echo "    $apply_output" >&2
  true
}
if echo "$apply_output" | grep -qF 'CONCURRENTLY'; then
  pass
else
  fail "expected error mentioning CONCURRENTLY"
  echo "    $apply_output" >&2
fi

summary
