#!/usr/bin/env bash
# Scenario test: safely adding a NOT NULL column
# Steps:
#   1. Add nullable column
#   2. Add CHECK constraint with NOT VALID
#   3. Remove NOT VALID (run VALIDATE CONSTRAINT)
#   4. Set column to NOT NULL
#   5. Drop CHECK constraint
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/add_not_null_column"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: Add nullable column ---
run_step "01 add nullable column" "ADD COLUMN email" "$DATA/steps/01_add_nullable_column.sql"

# --- Step 2: Add CHECK constraint with NOT VALID ---
run_step "02 add check constraint (NOT VALID)" "NOT VALID" "$DATA/steps/02_add_check_not_valid.sql"

# --- Step 3: Remove NOT VALID (run VALIDATE CONSTRAINT) ---
step "03 validate constraint (remove NOT VALID)"
plan_output=$(pist_plan "$DATA/steps/03_validate_constraint.sql") || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'VALIDATE CONSTRAINT chk_email_not_null'; then
  fail "expected VALIDATE CONSTRAINT in plan"
  echo "    actual: $plan_output" >&2
else
  apply_output=$(pist_apply "$DATA/steps/03_validate_constraint.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan "$DATA/steps/03_validate_constraint.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 4: Set column to NOT NULL ---
run_step "04 set not null" "SET NOT NULL" "$DATA/steps/04_set_not_null.sql"

# --- Step 5: Drop CHECK constraint ---
run_step "05 drop check constraint" "DROP CONSTRAINT chk_email_not_null" "$DATA/steps/05_drop_check.sql"

summary
