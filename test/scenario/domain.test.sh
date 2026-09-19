#!/usr/bin/env bash
# Scenario test: domain lifecycle.
# The initial schema has a domain with a check constraint and a table column
# of it. The steps walk the metadata a domain carries (default, NOT NULL,
# constraints), the VALIDATE a constraint added NOT VALID out of band needs,
# and the rename and drop at the end. The column of the domain stays in place
# throughout, so every apply has to keep the dependency working.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/domain"

# --- Setup: load the initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: the initial schema round-trips with no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: the domain grows a default and NOT NULL ---
run_step "01 set default and NOT NULL" "$(cat <<'EOS'
ALTER DOMAIN public.email SET DEFAULT 'nobody@example.com'::text;
ALTER DOMAIN public.email SET NOT NULL;
EOS
)" "$DATA/steps/01_default_not_null.sql" || true

# --- Step 2: add a second constraint ---
run_step "02 add constraint (email_length)" \
  "ALTER DOMAIN public.email ADD CONSTRAINT email_length CHECK (length(value) <= 320);" \
  "$DATA/steps/02_add_constraint.sql" || true

# --- Step 3: a changed expression is a drop and an add ---
run_step "03 change constraint expression" "$(cat <<'EOS'
ALTER DOMAIN public.email DROP CONSTRAINT email_format;
ALTER DOMAIN public.email ADD CONSTRAINT email_format CHECK (value ~ '^[^@]+@[^@]+$'::text);
EOS
)" "$DATA/steps/03_change_constraint.sql" || true

# --- Step 4: a constraint added NOT VALID out of band is validated ---
run_sql "ALTER DOMAIN public.email ADD CONSTRAINT email_local CHECK (length(split_part(VALUE, '@', 1)) > 0) NOT VALID"
run_step "04 validate out-of-band NOT VALID constraint" \
  "ALTER DOMAIN public.email VALIDATE CONSTRAINT email_local;" \
  "$DATA/steps/04_validate_out_of_band.sql" || true

# --- Step 5: take the default and NOT NULL back off ---
run_step "05 drop default and NOT NULL" "$(cat <<'EOS'
ALTER DOMAIN public.email DROP DEFAULT;
ALTER DOMAIN public.email DROP NOT NULL;
EOS
)" "$DATA/steps/05_drop_default_not_null.sql" || true

# --- Step 6: rename the domain the column depends on ---
run_step "06 rename domain (email -> email_address)" \
  "ALTER DOMAIN public.email RENAME TO email_address;" \
  "$DATA/steps/06_rename_domain.sql" || true

# --- Step 7: drop two of the three constraints ---
run_step "07 drop constraints" "$(cat <<'EOS'
ALTER DOMAIN public.email_address DROP CONSTRAINT email_length;
ALTER DOMAIN public.email_address DROP CONSTRAINT email_local;
EOS
)" "$DATA/steps/07_drop_constraints.sql" || true

# --- Step 8: the column moves to the base type and the domain goes ---
run_step "08 column back to base type, drop domain" "$(cat <<'EOS'
ALTER TABLE public.users ALTER COLUMN addr SET DATA TYPE text;
DROP DOMAIN public.email_address;
EOS
)" "$DATA/steps/08_drop_domain.sql" || true

# --- Step 9: the domain drop is suppressed unless --allow-drop covers it ---
setup_db "$DATA/init.sql"
assert_commented_drop_with_allowed "09 domain drop is suppressed by default" \
  domain column "$DATA/steps/08_drop_domain.sql" || true
assert_no_drop_type "10 no executable domain drop without the type" \
  domain column "$DATA/steps/08_drop_domain.sql" || true

summary
