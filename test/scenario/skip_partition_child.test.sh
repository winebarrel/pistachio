#!/usr/bin/env bash
# Scenario test: --skip-partition-child
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/skip_partition_child"

setup_db "$DATA/init.sql"

# --- Step 1: without the flag the partition the desired schema omits is dropped ---
step "01 partition is dropped without the flag"
plan_output=$(pista_plan "$DATA/steps/01_parent_only.sql") || { fail "plan failed: $plan_output"; true; }
if echo "$plan_output" | grep -qF 'DROP TABLE public.events_p2024;'; then
  pass
else
  fail "expected a DROP for the partition"
  echo "    $plan_output" >&2
fi

# --- Step 2: the flag leaves it alone ---
step "02 partition is left alone with the flag"
plan_output=$("$PISTA" plan --allow-drop all --skip-partition-child "$DATA/steps/01_parent_only.sql" 2>&1) || { fail "plan failed: $plan_output"; true; }
if echo "$plan_output" | grep -qF -e '-- No changes'; then
  pass
else
  fail "expected no changes"
  echo "    $plan_output" >&2
fi

# --- Step 3: a change to the parent still applies, drift-free ---
step "03 parent change applies and reaches the partition"
apply_output=$("$PISTA" apply --allow-drop all --skip-partition-child "$DATA/steps/02_parent_column.sql" 2>&1) || { fail "apply failed: $apply_output"; true; }
child_col=$(psql -X "$PISTA_CONN_STR" -tAc "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='events_p2024' AND column_name='data')")
if [ "$child_col" != "t" ]; then
  fail "expected the added column to reach the partition"
  echo "    $apply_output" >&2
else
  drift=$("$PISTA" plan --allow-drop all --skip-partition-child "$DATA/steps/02_parent_column.sql" 2>&1)
  if echo "$drift" | grep -qF -e '-- No changes'; then
    pass
  else
    fail "expected no drift after apply"
    echo "    $drift" >&2
  fi
fi

# --- Step 4: a partition created afterwards does not show up as a change ---
step "04 new partition does not enter the plan"
psql -X "$PISTA_CONN_STR" -q -v ON_ERROR_STOP=1 -c "CREATE TABLE public.events_p2025 PARTITION OF public.events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')"
plan_output=$("$PISTA" plan --allow-drop all --skip-partition-child "$DATA/steps/02_parent_column.sql" 2>&1) || { fail "plan failed: $plan_output"; true; }
if echo "$plan_output" | grep -qF -e '-- No changes'; then
  pass
else
  fail "expected no changes for the new partition"
  echo "    $plan_output" >&2
fi

# --- Step 5: dump writes the parent alone ---
step "05 dump omits the partitions"
dump_output=$("$PISTA" dump --skip-partition-child 2>&1) || { fail "dump failed: $dump_output"; true; }
if echo "$dump_output" | grep -qF 'PARTITION OF'; then
  fail "expected no partition in the dump"
  echo "    $dump_output" >&2
elif ! echo "$dump_output" | grep -qF 'PARTITION BY RANGE (created_at)'; then
  fail "expected the partitioned parent in the dump"
  echo "    $dump_output" >&2
elif ! echo "$dump_output" | grep -qF 'CREATE TABLE public.users'; then
  fail "expected the other tables in the dump"
  echo "    $dump_output" >&2
else
  pass
fi

# --- Step 6: the dump plans clean when fed back under the same flag ---
step "06 dump output round-trips"
tmp_sql=$(mktemp)
trap 'rm -f "$tmp_sql"' EXIT
"$PISTA" dump --skip-partition-child > "$tmp_sql"
plan_output=$("$PISTA" plan --allow-drop all --skip-partition-child "$tmp_sql" 2>&1) || { fail "plan failed: $plan_output"; true; }
if echo "$plan_output" | grep -qF -e '-- No changes'; then
  pass
else
  fail "expected no changes when the dump is fed back"
  echo "    $plan_output" >&2
fi

summary
