#!/usr/bin/env bash
# Scenario test: usda schema
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/usda"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: initial schema has no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: ADD COLUMN (data_src.url) ---
run_step "01 add column (data_src.url)" \
  "ADD COLUMN url text" \
  "$DATA/steps/01_add_column.sql" || true

# --- Step 2: ADD INDEX (nutr_def.tagname) ---
run_step "02 add index (nutr_def.tagname)" \
  "CREATE INDEX nutr_def_tagname_idx" \
  "$DATA/steps/02_add_index.sql" || true

# --- Step 3: ADD CHECK CONSTRAINT (nut_data.nutr_val >= 0) ---
run_step "03 add check constraint (nut_data.nutr_val)" \
  "ADD CONSTRAINT nut_data_nutr_val_check" \
  "$DATA/steps/03_add_check_constraint.sql" || true

summary
