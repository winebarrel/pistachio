#!/usr/bin/env bash
# Scenario test: materialized view support
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/matview"

# --- Setup: load initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: initial schema has no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: ADD MATERIALIZED VIEW ---
run_step "01 add materialized view" \
  'CREATE MATERIALIZED VIEW public.order_stats' \
  "$DATA/steps/01_add_matview.sql" || true

# --- Step 2: ADD INDEX ON MATERIALIZED VIEW ---
run_step "02 add index on matview" \
  'CREATE INDEX idx_order_stats_user' \
  "$DATA/steps/02_add_matview_index.sql" || true

# --- Step 3: DROP MATERIALIZED VIEW ---
run_step "03 drop materialized view" \
  'DROP MATERIALIZED VIEW public.order_stats' \
  "$DATA/steps/03_drop_matview.sql" || true

summary
