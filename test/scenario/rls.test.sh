#!/usr/bin/env bash
# Scenario test: row-level security (RLS) and policies
# Walks the desired schema through enable → policies → modify → drop → disable,
# verifying drift-free state at every step.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/rls"

setup_db "$DATA/init.sql"

run_step "step 1: enable RLS" \
  "ENABLE ROW LEVEL SECURITY" \
  "$DATA/step1_enable.sql"

run_step "step 2: add policies" \
  "CREATE POLICY owner_select" \
  "$DATA/step2_add_policies.sql"

run_step "step 3: alter USING in place" \
  "ALTER POLICY owner_select" \
  "$DATA/step3_alter_using.sql"

run_step "step 4: change command (DROP+CREATE)" \
  "DROP POLICY owner_select" \
  "$DATA/step4_change_command.sql"

run_step "step 5: drop one policy" \
  "DROP POLICY owner_modify" \
  "$DATA/step5_drop_policy.sql"

run_step "step 6: force RLS" \
  "FORCE ROW LEVEL SECURITY" \
  "$DATA/step6_force.sql"

run_step "step 7: disable RLS" \
  "DISABLE ROW LEVEL SECURITY" \
  "$DATA/step7_disable.sql"

summary
