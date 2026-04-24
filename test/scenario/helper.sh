#!/usr/bin/env bash
# Common helpers for CLI scenario tests.

set -euo pipefail

: "${PIST:=./pist}"
export PIST_CONN_STR="${PIST_CONN_STR:-postgres://postgres@localhost/postgres}"
export PGHOST="${PGHOST:-localhost}"
export PGUSER="${PGUSER:-postgres}"

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
  psql -q -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public' 2>/dev/null
  if [ $# -gt 0 ] && [ -n "$1" ]; then
    psql -q -f "$1" 2>/dev/null
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

# Run pist dump.
pist_dump() {
  "$PIST" dump "$@" 2>&1
}

# Assert that plan output contains the expected string.
assert_plan_contains() {
  local plan_output="$1"
  local expected="$2"
  if echo "$plan_output" | grep -qF "$expected"; then
    return 0
  else
    fail "expected plan to contain: $expected"
    echo "    actual: $plan_output" >&2
    return 1
  fi
}

# Assert that plan shows no changes (drift-free).
assert_no_drift() {
  local plan_output
  plan_output=$(pist_plan "$@")
  if echo "$plan_output" | grep -q 'No changes'; then
    return 0
  else
    fail "expected no drift, but got:"
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

  pist_apply "${files[@]}" >/dev/null 2>&1 || { fail "apply failed"; return 1; }

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
