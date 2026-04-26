#!/usr/bin/env bash
# Scenario test: --disable-index-concurrently flag overrides -- pist:concurrently directive
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/disable_index_concurrently"

pist_plan_disable() {
  "$PIST" plan --allow-drop all --disable-index-concurrently "$@" 2>&1
}

pist_apply_disable() {
  "$PIST" apply --allow-drop all --disable-index-concurrently "$@" 2>&1
}

assert_no_concurrently() {
  local label="$1"
  local output="$2"
  if echo "$output" | grep -qF 'CONCURRENTLY'; then
    fail "$label should not contain CONCURRENTLY"
    echo "    $output" >&2
    return 1
  fi
  return 0
}

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: ADD INDEX with directive but disabled ---
step "01 add index (directive disabled)"
plan_output=$(pist_plan_disable "$DATA/steps/01_add_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE INDEX idx_users_name'; then
  fail "expected CREATE INDEX idx_users_name in plan"
  echo "    $plan_output" >&2
elif ! assert_no_concurrently "step 01 plan" "$plan_output"; then
  true
else
  apply_output=$(pist_apply_disable "$DATA/steps/01_add_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_disable "$DATA/steps/01_add_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 2: CHANGE INDEX with directive but disabled (drop + create) ---
step "02 change index (directive disabled)"
plan_output=$(pist_plan_disable "$DATA/steps/02_change_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qE 'DROP INDEX (public\.)?idx_users_name;'; then
  fail "expected DROP INDEX idx_users_name in plan"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qF 'CREATE INDEX idx_users_name'; then
  fail "expected CREATE INDEX idx_users_name in plan"
  echo "    $plan_output" >&2
elif ! assert_no_concurrently "step 02 plan" "$plan_output"; then
  true
else
  apply_output=$(pist_apply_disable "$DATA/steps/02_change_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_disable "$DATA/steps/02_change_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 3: DROP INDEX (no directive in desired anyway) ---
step "03 drop index"
plan_output=$(pist_plan_disable "$DATA/steps/03_drop_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qE 'DROP INDEX (public\.)?idx_users_name;'; then
  fail "expected DROP INDEX idx_users_name in plan"
  echo "    $plan_output" >&2
elif ! assert_no_concurrently "step 03 plan" "$plan_output"; then
  true
else
  apply_output=$(pist_apply_disable "$DATA/steps/03_drop_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_disable "$DATA/steps/03_drop_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 4: matview index with directive but disabled ---
setup_db "$DATA/init.sql"

step "04 matview index (directive disabled)"
plan_output=$(pist_plan_disable "$DATA/steps/04_matview_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE INDEX idx_user_names'; then
  fail "expected CREATE INDEX idx_user_names in plan"
  echo "    $plan_output" >&2
elif ! assert_no_concurrently "step 04 plan" "$plan_output"; then
  true
else
  apply_output=$(pist_apply_disable "$DATA/steps/04_matview_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_disable "$DATA/steps/04_matview_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 5: --with-tx works when CONCURRENTLY is suppressed ---
setup_db "$DATA/init.sql"

step "05 with-tx + disabled directive"
apply_output=$("$PIST" apply --allow-drop all --disable-index-concurrently --with-tx "$DATA/steps/05_with_tx.sql" 2>&1) || { fail "apply failed: $apply_output"; true; }
if ! echo "$apply_output" | grep -qF 'CREATE UNIQUE INDEX idx_users_email'; then
  fail "expected CREATE UNIQUE INDEX idx_users_email in apply output"
  echo "    $apply_output" >&2
elif ! assert_no_concurrently "step 05 apply" "$apply_output"; then
  true
else
  drift=$(pist_plan_disable "$DATA/steps/05_with_tx.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 6: --with-tx WITHOUT --disable-index-concurrently fails ---
setup_db "$DATA/init.sql"

step "06 with-tx without disable flag fails"
apply_output=$("$PIST" apply --allow-drop all --with-tx "$DATA/steps/05_with_tx.sql" 2>&1) && {
  fail "expected apply to fail with --with-tx + CONCURRENTLY directive"
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
