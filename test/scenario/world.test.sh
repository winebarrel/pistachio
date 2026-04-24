#!/usr/bin/env bash
# Scenario test: world schema
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/world"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: initial schema has no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: ADD COLUMN (city.timezone) ---
run_step "01 add column (city.timezone)" \
  "ADD COLUMN timezone text" \
  "$DATA/steps/01_add_column.sql" || true

# --- Step 2: ALTER COLUMN TYPE (city.population int→bigint) ---
run_step "02 alter column type (city.population)" \
  "SET DATA TYPE bigint" \
  "$DATA/steps/02_alter_column_type.sql" || true

# --- Step 3: ADD INDEX (city.countrycode) ---
run_step "03 add index (city.countrycode)" \
  "CREATE INDEX idx_city_countrycode" \
  "$DATA/steps/03_add_index.sql" || true

# --- Step 4: SET DEFAULT (country.headofstate) ---
run_step "04 set default (country.headofstate)" \
  "SET DEFAULT" \
  "$DATA/steps/04_set_default.sql" || true

# --- Step 5: DROP COLUMN (country.gnpold) ---
run_step "05 drop column (country.gnpold)" \
  "DROP COLUMN gnpold" \
  "$DATA/steps/05_drop_column.sql" || true

# --- Step 6: ADD CHECK CONSTRAINT (city.population >= 0) ---
run_step "06 add check constraint" \
  "ADD CONSTRAINT city_population_check" \
  "$DATA/steps/06_add_check_constraint.sql" || true

# --- Step 7: ADD UNIQUE CONSTRAINT (country.code2) ---
run_step "07 add unique constraint (country.code2)" \
  "ADD CONSTRAINT country_code2_key" \
  "$DATA/steps/07_add_unique_constraint.sql" || true

# --- Step 8: ADD VIEW (large_cities) ---
run_step "08 add view (large_cities)" \
  "CREATE OR REPLACE VIEW public.large_cities" \
  "$DATA/steps/08_add_view.sql" || true

# --- Step 9: ADD FK (city.countrycode → country.code) ---
run_step "09 add fk (city → country)" \
  "ADD CONSTRAINT city_countrycode_fkey" \
  "$DATA/steps/09_add_fk.sql" || true

# --- Step 10: RENAME COLUMN (countrylanguage.language → lang) ---
run_step "10 rename column (language → lang)" \
  "RENAME COLUMN language TO lang" \
  "$DATA/steps/10_rename_column.sql" || true

summary
