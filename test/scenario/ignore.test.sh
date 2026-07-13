#!/usr/bin/env bash
# Scenario test: -- pista:ignore directive
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/ignore"

setup_db "$DATA/init.sql"

# --- Step 1: ignored table produces no DDL for itself ---
step "01 ignored table is not altered or dropped"
plan_output=$(pista_plan "$DATA/steps/01_ignore_legacy.sql") || { fail "plan failed: $plan_output"; true; }
if echo "$plan_output" | grep -qE '(ALTER|DROP|CREATE) TABLE .*legacy'; then
  fail "plan must not emit DDL for the ignored table"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qF 'ADD COLUMN email'; then
  fail "expected the managed table to still diff"
  echo "    $plan_output" >&2
elif ! echo "$plan_output" | grep -qF -e '-- ignored: public.legacy'; then
  fail "expected an -- ignored: comment for the ignored table"
  echo "    $plan_output" >&2
else
  pass
fi

# --- Step 2: apply leaves the ignored table intact, drift-free ---
step "02 apply keeps ignored table's column"
apply_output=$(pista_apply "$DATA/steps/01_ignore_legacy.sql") || { fail "apply failed: $apply_output"; true; }
name_col=$(psql -X "$PISTA_CONN_STR" -tAc "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='legacy' AND column_name='name')")
if [ "$name_col" != "t" ]; then
  fail "ignored table's 'name' column was dropped"
else
  drift=$(pista_plan "$DATA/steps/01_ignore_legacy.sql")
  if echo "$drift" | grep -qF 'No changes'; then
    pass
  else
    fail "expected no drift after apply"
    echo "    $drift" >&2
  fi
fi

summary
