#!/usr/bin/env bash
# Scenario test: dump --split and the multi-file input it produces.
# The schema holds one of every object kind dump gives its own file. The
# steps write the split dump, feed every file back as the desired schema,
# edit one file the way a user would, and write the dump again over the
# result. What is pinned here is the round trip the design rests on: a dump
# fed back plans clean, whether it is one file or a directory of them.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/dump_split"

SPLIT_DIR=$(mktemp -d)
trap 'rm -rf "$SPLIT_DIR"' EXIT

EXPECTED_FILES="address.sql
code_seq.sql
customers.sql
email.sql
open_orders.sql
order_counts.sql
order_status.sql
orders.sql"

# --- Setup: load the initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: the initial schema round-trips with no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: the split dump writes one file per object, and creates the
#     directory it is pointed at ---
step "01 split writes one file per object"
dump_output=$("$PISTA" dump --split "$SPLIT_DIR/out" 2>&1) || { fail "dump failed: $dump_output"; true; }
if ! echo "$dump_output" | grep -qF 'Wrote 8 file(s)'; then
  fail "expected 8 files in the dump output"
  echo "    $dump_output" >&2
else
  got=$(ls "$SPLIT_DIR/out")
  want=$(echo "$EXPECTED_FILES" | sed 's/^/public./')
  if [ "$got" = "$want" ]; then
    pass
  else
    fail "unexpected file list"
    echo "    want: $want" >&2
    echo "    got:  $got" >&2
  fi
fi

# --- Step 2: every file fed back together plans clean ---
run_step_no_diff "02 split dump round-trips" "$SPLIT_DIR"/out/*.sql || true

# --- Step 3: a column added in one of the files is the whole plan ---
cp "$DATA/steps/01_customers_add_column.sql" "$SPLIT_DIR/out/public.customers.sql"
run_step "03 edit one split file" \
  "ALTER TABLE public.customers ADD COLUMN note text;" \
  "$SPLIT_DIR"/out/*.sql || true

# --- Step 4: the dump written again over the same directory carries it ---
step "04 re-split carries the change"
dump_output=$("$PISTA" dump --split "$SPLIT_DIR/out" 2>&1) || { fail "dump failed: $dump_output"; true; }
if grep -qF 'note text' "$SPLIT_DIR/out/public.customers.sql"; then
  pass
else
  fail "expected the new column in the re-written file"
  cat "$SPLIT_DIR/out/public.customers.sql" >&2
fi

run_step_no_diff "05 re-split round-trips" "$SPLIT_DIR"/out/*.sql || true

# --- Step 6: --omit-schema drops the schema from the filenames too ---
step "06 split with --omit-schema"
dump_output=$("$PISTA" dump --split "$SPLIT_DIR/bare" --omit-schema 2>&1) || { fail "dump failed: $dump_output"; true; }
got=$(ls "$SPLIT_DIR/bare")
if [ "$got" = "$EXPECTED_FILES" ]; then
  pass
else
  fail "unexpected file list"
  echo "    want: $EXPECTED_FILES" >&2
  echo "    got:  $got" >&2
fi

run_step_no_diff "07 unqualified split dump round-trips" "$SPLIT_DIR"/bare/*.sql || true

summary
