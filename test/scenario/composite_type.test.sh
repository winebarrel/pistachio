#!/usr/bin/env bash
# Scenario test: composite type lifecycle (create, alter attributes, rename,
# comment), verifying drift-free state at each step.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

STEPS="$SCRIPT_DIR/testdata/composite_type/steps"

setup_db ""

run_step "01 create composite type" \
  "CREATE TYPE public.addr AS (" \
  "$STEPS/01_create.sql"

run_step "02 add / drop / alter attribute" \
  "ALTER TYPE public.addr ADD ATTRIBUTE city text;" \
  "$STEPS/02_alter_attrs.sql"

run_step "03 rename type and attribute" \
  "ALTER TYPE public.addr RENAME TO address;" \
  "$STEPS/03_rename.sql"

run_step "04 type and attribute comments" \
  "COMMENT ON TYPE public.address IS 'an address';" \
  "$STEPS/04_comment.sql"

summary
