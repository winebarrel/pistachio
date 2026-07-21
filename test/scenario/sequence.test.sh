#!/usr/bin/env bash
# Scenario test: standalone sequence management mixed with serial/identity.
# The initial schema has a standalone sequence (code_seq, referenced by a
# column default) alongside a serial column and an identity column. Those two
# own auto-created sequences that must stay unmanaged: every step verifies the
# schema is drift-free after apply, which fails if the owned sequences leak
# into the diff.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/sequence"

# --- Setup: load the mixed initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: mixed schema round-trips with no diff (owned sequences ignored) ---
run_step_no_diff "00 init: no diff (serial/identity not managed)" "$DATA/init.sql" || true

# --- Step 1: add a standalone sequence ---
run_step "01 add sequence (order_seq)" \
  "CREATE SEQUENCE public.order_seq" \
  "$DATA/steps/01_add_sequence.sql" || true

# --- Step 2: alter sequence options ---
run_step "02 alter sequence (increment/maxvalue/cycle)" \
  "ALTER SEQUENCE public.order_seq" \
  "$DATA/steps/02_alter_sequence.sql" || true

# --- Step 3: add a comment ---
run_step "03 add comment" \
  "COMMENT ON SEQUENCE public.order_seq IS 'Order id generator'" \
  "$DATA/steps/03_add_comment.sql" || true

# --- Step 4: rename the sequence ---
run_step "04 rename sequence (order_seq -> seq_orders)" \
  "ALTER SEQUENCE public.order_seq RENAME TO seq_orders" \
  "$DATA/steps/04_rename_sequence.sql" || true

# --- Step 5: drop the sequence ---
run_step "05 drop sequence (seq_orders)" \
  "DROP SEQUENCE public.seq_orders" \
  "$DATA/steps/05_drop_sequence.sql" || true

summary
