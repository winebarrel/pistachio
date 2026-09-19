#!/usr/bin/env bash
# Scenario test: enum type lifecycle.
# The initial schema has one enum used by a plain column, an array column and
# a column default. PostgreSQL cannot remove an enum value, so the steps walk
# the additions it does allow, the error a removal raises, and the
# -- pista:renamed-from directive that turns a removal into a RENAME VALUE.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/enum"

# --- Setup: load the initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: the initial schema round-trips with no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: append a value ---
run_step "01 add value at the end" \
  "ALTER TYPE public.order_status ADD VALUE 'delivered' AFTER 'shipped';" \
  "$DATA/steps/01_add_value_end.sql" || true

# --- Step 2: insert a value between two existing ones ---
run_step "02 add value in the middle" \
  "ALTER TYPE public.order_status ADD VALUE 'paid' AFTER 'pending';" \
  "$DATA/steps/02_add_value_middle.sql" || true

# --- Step 3: dropping a value is an error PostgreSQL cannot carry out ---
assert_plan_error "03 removing a value fails" \
  "PostgreSQL does not support removing enum values" \
  "$DATA/steps/03_remove_value.sql" || true

# --- Step 4: the same removal with a renamed-from directive is a rename ---
run_step "04 renamed-from renames the value" \
  "ALTER TYPE public.order_status RENAME VALUE 'delivered' TO 'done';" \
  "$DATA/steps/04_rename_value.sql" || true

# --- Step 5: the directive is a no-op once the rename is applied ---
run_step_no_diff "05 applied rename plans clean" \
  "$DATA/steps/04_rename_value.sql" || true

# --- Step 6: a second enum arrives with the column that uses it ---
run_step "06 add enum and a column of it" "$(cat <<'EOS'
CREATE TYPE public.ship_method AS ENUM (
ALTER TABLE public.orders ADD COLUMN method public.ship_method DEFAULT 'ground'::public.ship_method NOT NULL;
EOS
)" "$DATA/steps/05_add_enum_column.sql" || true

# --- Step 7: rename the type out from under the column ---
run_step "07 rename type (ship_method -> shipping_method)" \
  "ALTER TYPE public.ship_method RENAME TO shipping_method;" \
  "$DATA/steps/06_rename_type.sql" || true

# --- Step 8: drop the column, then the type it used ---
run_step "08 drop column and enum" "$(cat <<'EOS'
ALTER TABLE public.orders DROP COLUMN method;
DROP TYPE public.shipping_method;
EOS
)" "$DATA/steps/07_drop_enum.sql" || true

summary
