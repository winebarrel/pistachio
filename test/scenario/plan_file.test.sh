#!/usr/bin/env bash
# Scenario test: plan --out and apply-from
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/plan_file"
PLAN_FILE="$(mktemp -t pista-plan.XXXXXX.json)"
trap 'rm -f "$PLAN_FILE"' EXIT

# Run pista apply-from and echo the exit code, for a step that asserts on it.
apply_from_rc() {
  local rc=0
  "$PISTA" apply-from "$@" >/dev/null 2>&1 || rc=$?
  echo "$rc"
}

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 1: plan --out writes the file and still prints the plan ---
step "01 plan --out writes a plan file"
plan_output=$("$PISTA" plan --allow-drop all --out "$PLAN_FILE" "$DATA/steps/01_add_column.sql" 2>&1) || { fail "plan failed: $plan_output"; true; }
if ! echo "$plan_output" | grep -qF 'ADD COLUMN name'; then
  fail "expected ADD COLUMN name in plan output"
elif ! grep -qF 'ALTER TABLE public.users ADD COLUMN name text;' "$PLAN_FILE"; then
  fail "expected the statement in the plan file"
else
  pass
fi

# --- Step 2: apply-from runs it ---
step "02 apply-from applies the plan file"
apply_output=$("$PISTA" apply-from "$PLAN_FILE" 2>&1) || { fail "apply-from failed: $apply_output"; true; }
if ! echo "$apply_output" | grep -qF 'ALTER TABLE public.users ADD COLUMN name text;'; then
  fail "expected the statement in the output"
  echo "    $apply_output" >&2
else
  pass
fi

# --- Step 3: the schema is what the plan said, so there is nothing left ---
step "03 no drift after apply-from"
plan_output=$(pista_plan "$DATA/steps/01_add_column.sql") || { fail "plan failed: $plan_output"; true; }
if echo "$plan_output" | grep -qF 'No changes'; then
  pass
else
  fail "expected no changes"
  echo "    $plan_output" >&2
fi

# --- Step 4: a plan file with nothing to do ---
step "04 apply-from reports no changes"
"$PISTA" plan --allow-drop all --out "$PLAN_FILE" "$DATA/steps/01_add_column.sql" >/dev/null 2>&1
apply_output=$("$PISTA" apply-from "$PLAN_FILE" 2>&1) || { fail "apply-from failed: $apply_output"; true; }
if echo "$apply_output" | grep -qF 'No changes'; then
  pass
else
  fail "expected no changes"
  echo "    $apply_output" >&2
fi

# --- Step 5: a schema change under the plan stops the apply ---
step "05 apply-from stops on drift"
"$PISTA" plan --allow-drop all --out "$PLAN_FILE" "$DATA/steps/02_add_index.sql" >/dev/null 2>&1
run_sql 'ALTER TABLE public.users ADD COLUMN email text'
rc=$(apply_from_rc "$PLAN_FILE")
index_count=$(psql -X "$PISTA_CONN_STR" -tAq -c "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'users_name_idx'")
if [ "$rc" != "1" ]; then
  fail "expected exit code 1, got $rc"
elif [ "$index_count" != "0" ]; then
  fail "expected nothing to have run"
else
  pass
fi

# --- Step 6: --force applies it anyway ---
step "06 apply-from --force applies over drift"
apply_output=$("$PISTA" apply-from --force "$PLAN_FILE" 2>&1) || { fail "apply-from --force failed: $apply_output"; true; }
index_count=$(psql -X "$PISTA_CONN_STR" -tAq -c "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'users_name_idx'")
if ! echo "$apply_output" | grep -qF -e '-- Warning:'; then
  fail "expected a drift warning"
  echo "    $apply_output" >&2
elif [ "$index_count" != "1" ]; then
  fail "expected the index to have been created"
else
  pass
fi

# --- Step 7: a file this pista cannot read ---
step "07 apply-from refuses another format version"
echo '{"version": 0}' > "$PLAN_FILE"
rc=$(apply_from_rc "$PLAN_FILE")
if [ "$rc" = "1" ]; then
  pass
else
  fail "expected exit code 1, got $rc"
fi

summary
