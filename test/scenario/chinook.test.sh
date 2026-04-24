#!/usr/bin/env bash
# Scenario test: chinook schema (quoted identifiers)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/chinook"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: initial schema has no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: ADD COLUMN (Artist."Country") ---
run_step "01 add column (Artist.Country)" \
  'ADD COLUMN "Country"' \
  "$DATA/steps/01_add_column.sql" || true

# --- Step 2: ADD INDEX (Invoice."InvoiceDate") ---
run_step "02 add index (Invoice.InvoiceDate)" \
  'CREATE INDEX "IX_InvoiceDate"' \
  "$DATA/steps/02_add_index.sql" || true

# --- Step 3: DROP COLUMN (Employee."Fax") ---
run_step "03 drop column (Employee.Fax)" \
  'DROP COLUMN "Fax"' \
  "$DATA/steps/03_drop_column.sql" || true

summary
