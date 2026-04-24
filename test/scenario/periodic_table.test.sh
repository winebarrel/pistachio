#!/usr/bin/env bash
# Scenario test: periodic_table schema
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/periodic_table"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: initial schema has no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: ADD INDEX (periodic_table."Symbol") ---
run_step "01 add index (Symbol)" \
  "CREATE INDEX idx_periodic_table_symbol" \
  "$DATA/steps/01_add_index.sql" || true

summary
