#!/usr/bin/env bash
# Scenario test: functions and procedures.
#
# Routines are opt-in, so every pista invocation here runs with
# PISTA_MANAGE_ROUTINE set. The steps walk an overload set through a body
# change, a CHECK constraint that calls a routine, a trigger function, a
# procedure, a return-type change that has to run as a drop and a create, and
# a drop. Each step verifies the schema is drift-free after apply, which is
# what pins the round trip between what dump writes and what plan reads.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/routine"

export PISTA_MANAGE_ROUTINE=1

# --- Setup ---
setup_db "$DATA/init.sql"

# --- Step 0: the initial schema round-trips with no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: add an overload ---
run_step "01 add overload (normalize_email/2)" \
  "CREATE OR REPLACE FUNCTION public.normalize_email(e text, keep_case boolean)" \
  "$DATA/steps/01_add_overload.sql" || true

# --- Step 2: a body change replaces in place ---
run_step "02 replace body" \
  "CREATE OR REPLACE FUNCTION public.normalize_email(e text)" \
  "$DATA/steps/02_replace_body.sql" || true

# --- Step 3: a CHECK constraint that calls the routine ---
run_step "03 check constraint calls routine" \
  "users_email_normalized" \
  "$DATA/steps/03_check_constraint_calls_routine.sql" || true

# --- Step 4: a trigger function and a procedure ---
run_step "04 add trigger function and procedure" \
  "CREATE OR REPLACE PROCEDURE public.purge_users()" \
  "$DATA/steps/04_add_procedure_and_trigger.sql" || true

# --- Step 5: a return-type change runs as a drop and a create ---
run_step "05 change return type (drop and create)" \
  "DROP FUNCTION public.normalize_email(text, boolean)" \
  "$DATA/steps/05_change_return_type.sql" || true

# --- Step 6: dropping is gated by --allow-drop routine ---
assert_commented_drop "06 drop is suppressed without --allow-drop" \
  routine "$DATA/steps/06_drop_overload.sql" || true

assert_no_drop_type "06b other --allow-drop types do not drop a routine" \
  routine table "$DATA/steps/06_drop_overload.sql" || true

assert_drop_type_present "06c --allow-drop routine drops it" \
  routine routine "$DATA/steps/06_drop_overload.sql" || true

run_step "07 drop overload and procedure" \
  "DROP FUNCTION public.normalize_email(text, boolean)" \
  "$DATA/steps/06_drop_overload.sql" || true

summary
