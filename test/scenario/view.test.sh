#!/usr/bin/env bash
# Scenario test: view lifecycle over a two-level chain.
# eng_staff reads staff and staff reads employees, so a change to staff has to
# leave the view above it working. The steps walk the changes CREATE OR
# REPLACE can carry, the comments and options a view holds, a rename, and the
# drop and re-create a shape change needs once nothing reads the view.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/view"

# --- Setup: load the initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: the initial schema round-trips with no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: appending a column is a CREATE OR REPLACE ---
run_step "01 add a view column" \
  "CREATE OR REPLACE VIEW public.staff AS" \
  "$DATA/steps/01_add_view_column.sql" || true

# --- Step 2: a changed expression under the same name replaces in place ---
run_step "02 change a column expression" \
  "SELECT employees.id, upper(employees.name) AS name, employees.dept, employees.salary FROM public.employees;" \
  "$DATA/steps/02_change_view_column.sql" || true

# --- Step 3: comments on the view and on one of its columns ---
run_step "03 comment on view and view column" "$(cat <<'EOS'
COMMENT ON VIEW public.staff IS 'Employees without the private columns';
COMMENT ON COLUMN public.staff.name IS 'Upper-cased display name';
EOS
)" "$DATA/steps/03_comments.sql" || true

# --- Step 4: security_invoker is managed with or without --manage-storage-param ---
run_step "04 set security_invoker" \
  "ALTER VIEW public.staff SET (security_invoker='true');" \
  "$DATA/steps/04_options.sql" || true

# --- Step 5: rename the leaf view ---
run_step "05 rename view (eng_staff -> engineering)" \
  "ALTER VIEW public.eng_staff RENAME TO engineering;" \
  "$DATA/steps/05_rename_view.sql" || true

# --- Step 6: a view drop needs --allow-drop ---
assert_commented_drop "06 view drop is suppressed by default" view \
  "$DATA/steps/06_drop_dependent_view.sql" || true

# --- Step 7: with the drop allowed, the leaf view goes ---
run_step "07 drop the dependent view" \
  "DROP VIEW public.engineering;" \
  "$DATA/steps/06_drop_dependent_view.sql" || true

# --- Step 8: losing a column re-creates the view, comments and all ---
run_step "08 drop a view column (drop and re-create)" "$(cat <<'EOS'
DROP VIEW public.staff;
CREATE OR REPLACE VIEW public.staff WITH (security_invoker='true') AS
COMMENT ON VIEW public.staff IS 'Employees without the private columns';
COMMENT ON COLUMN public.staff.name IS 'Upper-cased display name';
EOS
)" "$DATA/steps/07_drop_view_column.sql" || true

# --- Step 9: the last view goes and the table is left alone ---
run_step "09 drop the last view" \
  "DROP VIEW public.staff;" \
  "$DATA/steps/08_drop_view.sql" || true

summary
