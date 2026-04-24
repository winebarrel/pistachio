#!/usr/bin/env bash
# Scenario test: lego schema
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/lego"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: initial schema has no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: ADD COLUMN (lego_sets.img_url) ---
run_step "01 add column (lego_sets.img_url)" \
  "ADD COLUMN img_url text" \
  "$DATA/steps/01_add_column.sql" || true

# --- Step 2: ADD CHECK CONSTRAINT (lego_sets.year >= 1900) ---
run_step "02 add check (lego_sets.year >= 1900)" \
  "ADD CONSTRAINT lego_sets_year_check" \
  "$DATA/steps/02_add_constraint.sql" || true

summary
