#!/usr/bin/env bash
# Common helpers for CLI scenario tests.

set -euo pipefail

: "${PIST:=./pist}"
export PIST_CONN_STR="${PIST_CONN_STR:-${TEST_PIST_CONN_STR:-postgres://postgres@localhost/postgres}}"

_pass=0
_fail=0
_current_step=""

step() {
  _current_step="$1"
  printf "  %-50s " "$1"
}

pass() {
  _pass=$((_pass + 1))
  echo "PASS"
}

fail() {
  _fail=$((_fail + 1))
  echo "FAIL"
  if [ $# -gt 0 ]; then
    echo "    $1" >&2
  fi
}

summary() {
  echo ""
  echo "  ${_pass} passed, ${_fail} failed"
  [ "$_fail" -eq 0 ]
}

# Reset the database and optionally run init SQL from a file.
setup_db() {
  psql -X "$PIST_CONN_STR" -q -v ON_ERROR_STOP=1 -c 'SET client_min_messages TO warning; DROP SCHEMA public CASCADE; CREATE SCHEMA public'
  if [ $# -gt 0 ] && [ -n "$1" ]; then
    psql -X "$PIST_CONN_STR" -q -v ON_ERROR_STOP=1 -f "$1"
  fi
}

# Run pist plan and capture output.
pist_plan() {
  "$PIST" plan --allow-drop all "$@" 2>&1
}

# Run pist apply.
pist_apply() {
  "$PIST" apply --allow-drop all "$@" 2>&1
}

# Run pist plan without --allow-drop (default: no drops allowed).
pist_plan_no_drop() {
  "$PIST" plan "$@" 2>&1
}

# Run pist plan with specific --allow-drop types.
pist_plan_allow_drop() {
  local drop_types="$1"
  shift
  "$PIST" plan --allow-drop "$drop_types" "$@" 2>&1
}

# Assert that plan output does NOT contain any DROP/drop statements.
assert_no_drop() {
  local step_name="$1"
  shift
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pist_plan_no_drop "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  if echo "$plan_output" | grep -qiE '^[[:space:]]*(DROP TABLE |DROP VIEW |DROP MATERIALIZED VIEW |DROP TYPE |DROP DOMAIN |ALTER TABLE .* DROP COLUMN)'; then
    fail "unexpected DROP in plan without --allow-drop"
    echo "    $plan_output" >&2
    return 1
  fi

  pass
}

# Assert that plan output does NOT contain DROP for a specific type,
# even when other drop types are allowed.
# Usage: assert_no_drop_type "step name" "protected_type" "allowed_types" files...
assert_no_drop_type() {
  local step_name="$1"
  local protected_type="$2"
  local allowed_types="$3"
  shift 3
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pist_plan_allow_drop "$allowed_types" "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  local drop_pattern
  case "$protected_type" in
    table)  drop_pattern='DROP TABLE' ;;
    view)   drop_pattern='DROP VIEW' ;;
    column) drop_pattern='DROP COLUMN' ;;
    enum)   drop_pattern='DROP TYPE' ;;
    domain) drop_pattern='DROP DOMAIN' ;;
    *) fail "unknown protected_type: $protected_type"; return 1 ;;
  esac

  if echo "$plan_output" | grep -qi "$drop_pattern"; then
    fail "unexpected $protected_type drop in plan with --allow-drop $allowed_types"
    echo "    $plan_output" >&2
    return 1
  fi

  pass
}

# Assert that plan output DOES contain DROP for a specific type.
# Usage: assert_drop_type_present "step name" "expected_type" "allowed_types" files...
assert_drop_type_present() {
  local step_name="$1"
  local expected_type="$2"
  local allowed_types="$3"
  shift 3
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pist_plan_allow_drop "$allowed_types" "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  local drop_pattern
  case "$expected_type" in
    table)  drop_pattern='DROP TABLE' ;;
    view)   drop_pattern='DROP VIEW' ;;
    column) drop_pattern='DROP COLUMN' ;;
    enum)   drop_pattern='DROP TYPE' ;;
    domain) drop_pattern='DROP DOMAIN' ;;
    *) fail "unknown expected_type: $expected_type"; return 1 ;;
  esac

  if echo "$plan_output" | grep -qi "$drop_pattern"; then
    pass
  else
    fail "expected $expected_type drop in plan with --allow-drop $allowed_types"
    echo "    $plan_output" >&2
    return 1
  fi
}

# Run a step: plan, check expected output, apply, verify no drift.
run_step() {
  local step_name="$1"
  local expected="$2"
  shift 2
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pist_plan "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  if ! echo "$plan_output" | grep -qF "$expected"; then
    fail "unexpected plan output"
    echo "    expected to contain: $expected" >&2
    echo "    actual: $plan_output" >&2
    return 1
  fi

  local apply_output
  apply_output=$(pist_apply "${files[@]}") || { fail "apply failed: $apply_output"; return 1; }

  local drift
  drift=$(pist_plan "${files[@]}") || { fail "post-apply plan failed: $drift"; return 1; }
  if ! echo "$drift" | grep -q 'No changes'; then
    fail "drift after apply"
    echo "    $drift" >&2
    return 1
  fi

  pass
}

# Run a step that expects no changes.
run_step_no_diff() {
  local step_name="$1"
  shift
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pist_plan "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  if echo "$plan_output" | grep -q 'No changes'; then
    pass
  else
    fail "expected no changes"
    echo "    actual: $plan_output" >&2
    return 1
  fi
}
