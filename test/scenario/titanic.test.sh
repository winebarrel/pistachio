#!/usr/bin/env bash
# Scenario test: titanic schema
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/titanic"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: initial schema has no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: ADD CHECK CONSTRAINT (passenger.class 1-3) ---
run_step "01 add check (passenger.class 1-3)" \
  "ADD CONSTRAINT passenger_class_check" \
  "$DATA/steps/01_add_check.sql" || true

summary
