#!/usr/bin/env bash
# Scenario test: triggers.
# The trigger functions are managed alongside the triggers, so every pista run
# here sets PISTA_MANAGE_ROUTINE. The steps walk the parts of a trigger that
# a replace can carry (WHEN clause, UPDATE OF columns), a statement-level
# trigger, the enable state, a rename, a constraint trigger, an INSTEAD OF
# trigger on a view, and the drop at the end.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/trigger"

export PISTA_MANAGE_ROUTINE=1

# --- Setup: load the initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: the initial schema round-trips with no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: a WHEN clause replaces the trigger in place ---
run_step "01 add a WHEN clause" \
  "CREATE OR REPLACE TRIGGER accounts_touch BEFORE UPDATE ON public.accounts FOR EACH ROW WHEN (old.balance IS DISTINCT FROM new.balance) EXECUTE FUNCTION touch_updated_at();" \
  "$DATA/steps/01_when_clause.sql" || true

# --- Step 2: narrow the trigger to one column ---
run_step "02 narrow to UPDATE OF balance" \
  "CREATE OR REPLACE TRIGGER accounts_touch BEFORE UPDATE OF balance ON public.accounts" \
  "$DATA/steps/02_update_of.sql" || true

# --- Step 3: a statement-level trigger on three events ---
run_step "03 add a statement-level trigger" \
  "CREATE TRIGGER accounts_audit AFTER INSERT OR DELETE OR UPDATE ON public.accounts EXECUTE FUNCTION audit_change();" \
  "$DATA/steps/03_statement_trigger.sql" || true

# --- Step 4: CREATE TRIGGER cannot say disabled, so the state is its own
#     ALTER TABLE ---
run_step "04 disable a trigger" \
  "ALTER TABLE public.accounts DISABLE TRIGGER accounts_audit;" \
  "$DATA/steps/04_disable_trigger.sql" || true

# --- Step 5: the replica-safe state ---
run_step "05 enable always" \
  "ALTER TABLE public.accounts ENABLE ALWAYS TRIGGER accounts_audit;" \
  "$DATA/steps/05_enable_always.sql" || true

# --- Step 6: a rename keeps the state the trigger already has ---
run_step "06 rename a trigger" \
  "ALTER TRIGGER accounts_audit ON public.accounts RENAME TO accounts_audit_stmt;" \
  "$DATA/steps/06_rename_trigger.sql" || true

# --- Step 7: a deferrable constraint trigger ---
run_step "07 add a constraint trigger" \
  "CREATE CONSTRAINT TRIGGER accounts_balance_check AFTER UPDATE ON public.accounts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION audit_change();" \
  "$DATA/steps/07_constraint_trigger.sql" || true

# --- Step 8: an INSTEAD OF trigger arrives with the view it sits on ---
run_step "08 add an INSTEAD OF trigger on a view" "$(cat <<'EOS'
CREATE OR REPLACE VIEW public.rich_accounts AS
CREATE TRIGGER rich_accounts_ins INSTEAD OF INSERT ON public.rich_accounts FOR EACH ROW EXECUTE FUNCTION audit_change();
EOS
)" "$DATA/steps/08_instead_of_trigger.sql" || true

# --- Step 9: a trigger drop needs --allow-drop trigger ---
assert_commented_drop_with_allowed "09 trigger drop is suppressed by default" \
  trigger:accounts_audit_stmt view "$DATA/steps/09_drop_triggers.sql" || true
assert_no_drop_type "10 no trigger drop without the type" \
  trigger:accounts_audit_stmt view "$DATA/steps/09_drop_triggers.sql" || true

# --- Step 11: with the drop allowed, the view and its triggers go ---
run_step "11 drop the triggers" "$(cat <<'EOS'
DROP VIEW public.rich_accounts;
DROP TRIGGER accounts_audit_stmt ON public.accounts;
DROP TRIGGER accounts_balance_check ON public.accounts;
EOS
)" "$DATA/steps/09_drop_triggers.sql" || true

summary
