#!/usr/bin/env bash
# Scenario test: --allow-drop policy
# Verifies that drops are suppressed when --allow-drop is not specified,
# and that individual --allow-drop types only allow their specific drops.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/drop_policy"

# ============================================================
# Part 1: No --allow-drop (default) suppresses ALL drops
# ============================================================

# --- all types removed: no drops in plan ---
setup_db "$DATA/init.sql"
assert_no_drop "no-drop: all types removed" "$DATA/desired_drop_all.sql" || true

# --- table removed: no drop ---
setup_db "$DATA/init.sql"
assert_no_drop "no-drop: table removed" "$DATA/desired_drop_table.sql" || true

# --- view removed: no drop ---
setup_db "$DATA/init.sql"
assert_no_drop "no-drop: view removed" "$DATA/desired_drop_view.sql" || true

# --- column removed: no drop ---
setup_db "$DATA/init.sql"
assert_no_drop "no-drop: column removed" "$DATA/desired_drop_column.sql" || true

# --- enum removed: no drop ---
setup_db "$DATA/init.sql"
assert_no_drop "no-drop: enum removed" "$DATA/desired_drop_enum.sql" || true

# --- domain removed: no drop ---
setup_db "$DATA/init.sql"
assert_no_drop "no-drop: domain removed" "$DATA/desired_drop_domain.sql" || true

# --- matview removed: no drop ---
setup_db "$DATA/init.sql"
assert_no_drop "no-drop: matview removed" "$DATA/desired_drop_matview.sql" || true

# ============================================================
# Part 1b: Suppressed drops are surfaced as commented DROPs,
# while -- No changes is still reported (no executable DDL).
# ============================================================

setup_db "$DATA/init.sql"
assert_commented_drop "no-drop: table commented" "table" "$DATA/desired_drop_table.sql" || true

setup_db "$DATA/init.sql"
assert_commented_drop "no-drop: view commented" "view" "$DATA/desired_drop_view.sql" || true

setup_db "$DATA/init.sql"
assert_commented_drop "no-drop: column commented" "column" "$DATA/desired_drop_column.sql" || true

setup_db "$DATA/init.sql"
assert_commented_drop "no-drop: enum commented" "enum" "$DATA/desired_drop_enum.sql" || true

setup_db "$DATA/init.sql"
assert_commented_drop "no-drop: domain commented" "domain" "$DATA/desired_drop_domain.sql" || true

setup_db "$DATA/init.sql"
assert_commented_drop "no-drop: matview commented" "view" "$DATA/desired_drop_matview.sql" || true

# ============================================================
# Part 1c: With --allow-drop=table, the table DROP executes while
# DROPs of other types still appear as skipped comments.
# ============================================================

setup_db "$DATA/init.sql"
assert_drop_type_present "allow-drop table: table drop executable" "table" "table" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_commented_drop_with_allowed "allow-drop table: view drop skipped" "view" "table" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_commented_drop_with_allowed "allow-drop table: column drop skipped" "column" "table" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_commented_drop_with_allowed "allow-drop table: enum drop skipped" "enum" "table" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_commented_drop_with_allowed "allow-drop table: domain drop skipped" "domain" "table" "$DATA/desired_drop_all.sql" || true

# ============================================================
# Part 2: Individual --allow-drop types
# Each type allows only its own drop; others are suppressed.
# ============================================================

# --allow-drop table: table dropped, but not view/matview/column/enum/domain
setup_db "$DATA/init.sql"
assert_drop_type_present "allow-drop table: table drop present" "table" "table" "$DATA/desired_drop_table.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop table: no view drop" "view" "table" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop table: no column drop" "column" "table" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop table: no enum drop" "enum" "table" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop table: no domain drop" "domain" "table" "$DATA/desired_drop_all.sql" || true

# --allow-drop view: view dropped (incl. matview), but not table/column/enum/domain
setup_db "$DATA/init.sql"
assert_drop_type_present "allow-drop view: view drop present" "view" "view" "$DATA/desired_drop_view.sql" || true
setup_db "$DATA/init.sql"
assert_drop_type_present "allow-drop view: matview drop present" "view" "view" "$DATA/desired_drop_matview.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop view: no table drop" "table" "view" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop view: no column drop" "column" "view" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop view: no enum drop" "enum" "view" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop view: no domain drop" "domain" "view" "$DATA/desired_drop_all.sql" || true

# --allow-drop column: column dropped, but not table/view/enum/domain
setup_db "$DATA/init.sql"
assert_drop_type_present "allow-drop column: column drop present" "column" "column" "$DATA/desired_drop_column.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop column: no table drop" "table" "column" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop column: no view drop" "view" "column" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop column: no enum drop" "enum" "column" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop column: no domain drop" "domain" "column" "$DATA/desired_drop_all.sql" || true

# --allow-drop enum: enum dropped, but not table/view/column/domain
setup_db "$DATA/init.sql"
assert_drop_type_present "allow-drop enum: enum drop present" "enum" "enum" "$DATA/desired_drop_enum.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop enum: no table drop" "table" "enum" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop enum: no view drop" "view" "enum" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop enum: no column drop" "column" "enum" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop enum: no domain drop" "domain" "enum" "$DATA/desired_drop_all.sql" || true

# --allow-drop domain: domain dropped, but not table/view/column/enum
setup_db "$DATA/init.sql"
assert_drop_type_present "allow-drop domain: domain drop present" "domain" "domain" "$DATA/desired_drop_domain.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop domain: no table drop" "table" "domain" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop domain: no view drop" "view" "domain" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop domain: no column drop" "column" "domain" "$DATA/desired_drop_all.sql" || true
setup_db "$DATA/init.sql"
assert_no_drop_type "allow-drop domain: no enum drop" "enum" "domain" "$DATA/desired_drop_all.sql" || true

# ============================================================
# Part 3: --allow-drop all allows everything
# ============================================================
setup_db "$DATA/init.sql"

step "allow-drop all: drops present"
if ! plan_output=$(pist_plan "$DATA/desired_drop_all.sql"); then
  fail "plan failed: $plan_output"
else
  has_all=true
  echo "$plan_output" | grep -qi 'DROP TABLE' || has_all=false
  echo "$plan_output" | grep -qi 'DROP VIEW' || has_all=false
  echo "$plan_output" | grep -qi 'DROP MATERIALIZED VIEW' || has_all=false
  echo "$plan_output" | grep -qi 'DROP COLUMN' || has_all=false
  echo "$plan_output" | grep -qi 'DROP TYPE' || has_all=false
  echo "$plan_output" | grep -qi 'DROP DOMAIN' || has_all=false
  if $has_all; then
    pass
  else
    fail "expected all drop types in plan with --allow-drop all"
    echo "    $plan_output" >&2
  fi
fi

summary
