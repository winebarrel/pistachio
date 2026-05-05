#!/usr/bin/env bash
# Scenario test: --bulk-alter combines per-table actions, leaves FK / VALIDATE
# CONSTRAINT split, and produces drift-free state on a real database.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/bulk_alter"

pist_plan_bulk() {
  "$PIST" plan --allow-drop all --bulk-alter "$@" 2>&1
}

pist_apply_bulk() {
  "$PIST" apply --allow-drop all --bulk-alter "$@" 2>&1
}

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: multi-action merge on a single table ---
step "01 multi-action merge"
plan_output=$(pist_plan_bulk "$DATA/steps/01_multi_action.sql") || { fail "plan failed: $plan_output"; true; }
# Expect a single ALTER TABLE with multiple comma-separated actions.
if ! echo "$plan_output" | grep -qE '^ALTER TABLE public\.users$'; then
  fail "expected merged ALTER TABLE public.users header on its own line"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qE '^  ADD COLUMN name text NOT NULL,$'; then
  fail "expected indented action lines with trailing comma"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qE '^  DROP COLUMN legacy,$'; then
  fail "expected DROP COLUMN to be merged"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qE '^  ADD CONSTRAINT users_id_pos CHECK \(id > 0\);$'; then
  fail "expected ADD CONSTRAINT as final merged action"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply_bulk "$DATA/steps/01_multi_action.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_bulk "$DATA/steps/01_multi_action.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 2: FK additions stay split (ALTER TABLE ONLY) ---
step "02 fk stays split from merged ALTER TABLE"
plan_output=$(pist_plan_bulk "$DATA/steps/02_add_fk.sql") || { fail "plan failed: $plan_output"; true; }
# orders gets ADD COLUMN user_id, ADD COLUMN total merged; FK is a separate
# ALTER TABLE ONLY ... statement.
if ! echo "$plan_output" | grep -qE '^ALTER TABLE public\.orders$'; then
  fail "expected merged ALTER TABLE public.orders header"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qE '^  ADD COLUMN user_id integer,$'; then
  fail "expected ADD COLUMN user_id merged"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qE '^  ADD COLUMN total numeric\(10,2\);$'; then
  fail "expected ADD COLUMN total as final merged action"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qF 'ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY'; then
  fail "expected FK as separate ALTER TABLE ONLY statement"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply_bulk "$DATA/steps/02_add_fk.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_bulk "$DATA/steps/02_add_fk.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 3: VALIDATE CONSTRAINT stays split from merged ADDs ---
# Seed a NOT VALID constraint directly so the next plan triggers a VALIDATE.
psql -X "$PIST_CONN_STR" -q -v ON_ERROR_STOP=1 \
  -c 'ALTER TABLE public.users DROP CONSTRAINT users_id_pos;' \
  -c 'ALTER TABLE public.users ADD CONSTRAINT users_id_pos CHECK (id > 0) NOT VALID;'

step "03 validate stays split from merged ADDs"
plan_output=$(pist_plan_bulk "$DATA/steps/03_validate.sql") || { fail "plan failed: $plan_output"; true; }
# Single ADD COLUMN remains as a single-line statement (no merge needed).
if ! echo "$plan_output" | grep -qF 'ALTER TABLE public.users ADD COLUMN extra text;'; then
  fail "expected single-line ADD COLUMN extra (no merge for one action)"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qF 'ALTER TABLE public.users VALIDATE CONSTRAINT users_id_pos;'; then
  fail "expected VALIDATE CONSTRAINT as a separate single-line statement"
  echo "    $plan_output" >&2
elif echo "$plan_output" | grep -qE '^  VALIDATE CONSTRAINT'; then
  fail "VALIDATE CONSTRAINT must NOT be merged into an ALTER TABLE group"
  echo "    $plan_output" >&2
else
  apply_output=$(pist_apply_bulk "$DATA/steps/03_validate.sql") || { fail "apply failed: $apply_output"; true; }
  drift=$(pist_plan_bulk "$DATA/steps/03_validate.sql") || { fail "drift check failed: $drift"; true; }
  if echo "$drift" | grep -q 'No changes'; then
    pass
  else
    fail "drift after apply"
    echo "    $drift" >&2
  fi
fi

summary
