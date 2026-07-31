#!/usr/bin/env bash
# Scenario test: -- pista:concurrently directive
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/index_concurrently"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: ADD INDEX with CONCURRENTLY directive ---
step "01 add index (concurrently)"
plan_output=$(pista_plan "$DATA/steps/01_add_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY'; then
  fail "expected CREATE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pista_apply "$DATA/steps/01_add_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pista_plan "$DATA/steps/01_add_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 2: ADD UNIQUE INDEX with CONCURRENTLY directive ---
step "02 add unique index (concurrently)"
plan_output=$(pista_plan "$DATA/steps/02_add_unique_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE UNIQUE INDEX CONCURRENTLY'; then
  fail "expected CREATE UNIQUE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pista_apply "$DATA/steps/02_add_unique_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pista_plan "$DATA/steps/02_add_unique_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 3: CHANGE INDEX with CONCURRENTLY directive (drop + create) ---
step "03 change index (concurrently)"
plan_output=$(pista_plan "$DATA/steps/03_change_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'DROP INDEX CONCURRENTLY'; then
  fail "expected DROP INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY'; then
  fail "expected CREATE INDEX CONCURRENTLY in plan"
  echo "    $plan_output" >&2
else
  apply_output=$(pista_apply "$DATA/steps/03_change_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pista_plan "$DATA/steps/03_change_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 4: DROP INDEX (pure drop never uses CONCURRENTLY) ---
step "04 drop index (pure drop)"
plan_output=$(pista_plan "$DATA/steps/04_drop_index.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qE 'DROP INDEX (public\.)?idx_users_name;'; then
  fail "expected DROP INDEX idx_users_name in plan"
  echo "    $plan_output" >&2
elif echo "$plan_output" | grep -qF 'DROP INDEX CONCURRENTLY'; then
  fail "pure drop should not use CONCURRENTLY"
  echo "    $plan_output" >&2
else
  apply_output=$(pista_apply "$DATA/steps/04_drop_index.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pista_plan "$DATA/steps/04_drop_index.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 5: per-index directive applies only to indexes with the directive ---
# Reset to initial schema
setup_db "$DATA/init.sql"

step "05 per-index directive (only directive index uses CONCURRENTLY)"
plan_output=$(pista_plan "$DATA/steps/05_directive.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'CREATE INDEX CONCURRENTLY idx_users_name'; then
  fail "expected CREATE INDEX CONCURRENTLY for idx_users_name"
  echo "    $plan_output" >&2
elif echo "$plan_output" | grep -qF 'CREATE UNIQUE INDEX CONCURRENTLY'; then
  fail "idx_users_email should NOT be CONCURRENTLY"
  echo "    $plan_output" >&2
else
  apply_output=$(pista_apply "$DATA/steps/05_directive.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pista_plan "$DATA/steps/05_directive.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 6: --try-tx skips the transaction when CONCURRENTLY index DDL is generated ---
setup_db "$DATA/init.sql"

step "06 try-tx skips transaction (concurrently)"
apply_output=$("$PISTA" apply --allow-drop all --try-tx "$DATA/steps/06_try_tx.sql" 2>&1) || { fail "apply failed: $apply_output"; true; }
if ! echo "$apply_output" | grep -qF 'CREATE INDEX CONCURRENTLY idx_users_name'; then
  fail "expected CREATE INDEX CONCURRENTLY idx_users_name in apply output"
  echo "    $apply_output" >&2
elif ! echo "$apply_output" | grep -qF -- '-- Transaction skipped'; then
  fail "expected -- Transaction skipped in apply output"
  echo "    $apply_output" >&2
elif echo "$apply_output" | grep -qF -- '-- Transaction started'; then
  fail "transaction should not be started with CONCURRENTLY index DDL"
  echo "    $apply_output" >&2
else
  drift=$(pista_plan "$DATA/steps/06_try_tx.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 7: --try-tx uses a transaction when the diff has no index changes ---
step "07 try-tx uses transaction (no index change)"
apply_output=$("$PISTA" apply --allow-drop all --try-tx "$DATA/steps/07_try_tx_no_index_change.sql" 2>&1) || { fail "apply failed: $apply_output"; true; }
if ! echo "$apply_output" | grep -qF -- '-- Transaction started'; then
  fail "expected -- Transaction started in apply output"
  echo "    $apply_output" >&2
elif ! echo "$apply_output" | grep -qF -- '-- Transaction committed'; then
  fail "expected -- Transaction committed in apply output"
  echo "    $apply_output" >&2
elif echo "$apply_output" | grep -qF -- '-- Transaction skipped'; then
  fail "transaction should not be skipped without index changes"
  echo "    $apply_output" >&2
else
  drift=$(pista_plan "$DATA/steps/07_try_tx_no_index_change.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

summary
