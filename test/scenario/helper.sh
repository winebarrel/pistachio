#!/usr/bin/env bash
# Common helpers for CLI scenario tests.

set -euo pipefail

: "${PISTA:=./pista}"
export PISTA_CONN_STR="${PISTA_CONN_STR:-${TEST_PISTA_CONN_STR:-postgres://postgres@localhost:5415/postgres}}"

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
  psql -X "$PISTA_CONN_STR" -q -v ON_ERROR_STOP=1 -c 'SET client_min_messages TO warning; DROP SCHEMA public CASCADE; CREATE SCHEMA public'
  if [ $# -gt 0 ] && [ -n "$1" ]; then
    psql -X "$PISTA_CONN_STR" -q -v ON_ERROR_STOP=1 -f "$1"
  fi
}

# Run SQL against the test database, for a change pista is not making.
run_sql() {
  psql -X "$PISTA_CONN_STR" -q -v ON_ERROR_STOP=1 -c "$1"
}

# Run pista plan and capture output.
pista_plan() {
  "$PISTA" plan --allow-drop all "$@" 2>&1
}

# Run pista apply.
pista_apply() {
  "$PISTA" apply --allow-drop all "$@" 2>&1
}

# Run pista plan without --allow-drop (default: no drops allowed).
pista_plan_no_drop() {
  "$PISTA" plan "$@" 2>&1
}

# Run pista plan with specific --allow-drop types.
pista_plan_allow_drop() {
  local drop_types="$1"
  shift
  "$PISTA" plan --allow-drop "$drop_types" "$@" 2>&1
}

# Assert that plan output does NOT contain any executable (uncommented) DROP
# statements. Commented "-- skipped: ..." DROPs may still be present.
assert_no_drop() {
  local step_name="$1"
  shift
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pista_plan_no_drop "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  if echo "$plan_output" | grep -qiE '^[[:space:]]*(DROP TABLE |DROP VIEW |DROP MATERIALIZED VIEW |DROP TYPE |DROP DOMAIN |DROP FUNCTION |DROP PROCEDURE |ALTER TABLE .* DROP COLUMN)'; then
    fail "unexpected DROP in plan without --allow-drop"
    echo "    $plan_output" >&2
    return 1
  fi

  pass
}

# _drop_pattern prints the grep -E pattern that matches a drop of the given
# kind. The kind may carry the object's name, "foreign_key:orders_parent_fk",
# so the pattern matches that object rather than any drop of the kind: every
# constraint drop is one ALTER TABLE ... DROP CONSTRAINT, and a plan that drops
# a primary key would otherwise pass a check written for a foreign key. prefix
# is what the statement starts with, the skipped-drop comment or leading space.
_drop_pattern() {
  local spec="$1"
  local prefix="$2"

  local kind="${spec%%:*}"
  local name=""
  if [ "$spec" != "$kind" ]; then
    name="${spec#*:}"
  fi

  case "$kind" in
    table)       printf '%sDROP TABLE %s' "$prefix" "$name" ;;
    view)        printf '%sDROP (MATERIALIZED )?VIEW %s' "$prefix" "$name" ;;
    column)      printf '%sALTER TABLE .* DROP COLUMN %s' "$prefix" "$name" ;;
    enum)        printf '%sDROP TYPE %s' "$prefix" "$name" ;;
    domain)      printf '%sDROP DOMAIN %s' "$prefix" "$name" ;;
    routine)     printf '%sDROP (FUNCTION|PROCEDURE) %s' "$prefix" "$name" ;;
    foreign_key) printf '%sALTER TABLE .* DROP CONSTRAINT %s' "$prefix" "$name" ;;
    trigger)     printf '%sDROP TRIGGER %s' "$prefix" "$name" ;;
    *) return 1 ;;
  esac
}

# The two prefixes a drop is looked for under: the comment a suppressed drop
# leaves, and the start of a statement that runs.
_SKIPPED_PREFIX='^-- skipped: '
_RUNS_PREFIX='^[[:space:]]*'

# Assert that plan output contains a commented DROP for a specific type.
# Used to verify that suppressed drops are still surfaced as comments.
# Usage: assert_commented_drop "step name" "expected_type" files...
assert_commented_drop() {
  local step_name="$1"
  local expected_type="$2"
  shift 2
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pista_plan_no_drop "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  local drop_pattern
  drop_pattern=$(_drop_pattern "$expected_type" "$_SKIPPED_PREFIX") || {
    fail "unknown expected_type: $expected_type"
    return 1
  }

  if ! echo "$plan_output" | grep -qE "$drop_pattern"; then
    fail "expected skipped $expected_type drop in plan"
    echo "    $plan_output" >&2
    return 1
  fi

  if ! echo "$plan_output" | grep -qF -e '-- No changes'; then
    fail "expected -- No changes when only commented drops were emitted"
    echo "    $plan_output" >&2
    return 1
  fi

  pass
}

# Assert that plan output contains a commented DROP for a specific type
# even when --allow-drop is set for other types.
# Usage: assert_commented_drop_with_allowed "step name" "expected_type" "allowed_types" files...
assert_commented_drop_with_allowed() {
  local step_name="$1"
  local expected_type="$2"
  local allowed_types="$3"
  shift 3
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pista_plan_allow_drop "$allowed_types" "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  local drop_pattern
  drop_pattern=$(_drop_pattern "$expected_type" "$_SKIPPED_PREFIX") || {
    fail "unknown expected_type: $expected_type"
    return 1
  }

  if ! echo "$plan_output" | grep -qE "$drop_pattern"; then
    fail "expected skipped $expected_type drop in plan with --allow-drop $allowed_types"
    echo "    $plan_output" >&2
    return 1
  fi

  pass
}

# Assert that plan output does NOT contain an executable (uncommented) DROP
# for a specific type, even when other drop types are allowed. Commented
# "-- skipped: ..." DROPs are not flagged by this check.
# Usage: assert_no_drop_type "step name" "protected_type" "allowed_types" files...
assert_no_drop_type() {
  local step_name="$1"
  local protected_type="$2"
  local allowed_types="$3"
  shift 3
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pista_plan_allow_drop "$allowed_types" "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  local drop_pattern
  drop_pattern=$(_drop_pattern "$protected_type" "$_RUNS_PREFIX") || {
    fail "unknown protected_type: $protected_type"
    return 1
  }

  if echo "$plan_output" | grep -qiE "$drop_pattern"; then
    fail "unexpected $protected_type drop in plan with --allow-drop $allowed_types"
    echo "    $plan_output" >&2
    return 1
  fi

  pass
}

# Assert that plan output DOES contain an executable (uncommented) DROP
# for a specific type when --allow-drop covers it.
# Usage: assert_drop_type_present "step name" "expected_type" "allowed_types" files...
assert_drop_type_present() {
  local step_name="$1"
  local expected_type="$2"
  local allowed_types="$3"
  shift 3
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pista_plan_allow_drop "$allowed_types" "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  local drop_pattern
  drop_pattern=$(_drop_pattern "$expected_type" "$_RUNS_PREFIX") || {
    fail "unknown expected_type: $expected_type"
    return 1
  }

  if echo "$plan_output" | grep -qiE "$drop_pattern"; then
    pass
  else
    fail "expected $expected_type drop in plan with --allow-drop $allowed_types"
    echo "    $plan_output" >&2
    return 1
  fi
}

# Print the first expectation missing from the given output, if any. The
# expectations are one per line; blank lines are skipped.
# Usage: _missing_expectation "$expected" "$actual"
_missing_expectation() {
  local expected="$1"
  local actual="$2"
  local line

  while IFS= read -r line; do
    [ -n "$line" ] || continue
    if ! printf '%s\n' "$actual" | grep -qF -- "$line"; then
      printf '%s' "$line"
      return 0
    fi
  done <<< "$expected"

  return 1
}

# Run a step: plan, check expected output, apply, verify no drift. The
# expected output is one string per line, so a step that produces several
# statements can name each of them.
run_step() {
  local step_name="$1"
  local expected="$2"
  shift 2
  local files=("$@")

  step "$step_name"

  local plan_output
  plan_output=$(pista_plan "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  local missing
  if missing=$(_missing_expectation "$expected" "$plan_output"); then
    fail "unexpected plan output"
    echo "    expected to contain: $missing" >&2
    echo "    actual: $plan_output" >&2
    return 1
  fi

  local apply_output
  apply_output=$(pista_apply "${files[@]}") || { fail "apply failed: $apply_output"; return 1; }

  local drift
  drift=$(pista_plan "${files[@]}") || { fail "post-apply plan failed: $drift"; return 1; }
  if ! echo "$drift" | grep -q 'No changes'; then
    fail "drift after apply"
    echo "    $drift" >&2
    return 1
  fi

  pass
}

# Assert that the dump output plans clean when it is fed back as the desired
# schema. Any flags are passed to both dump and plan.
# Usage: assert_dump_round_trip "step name" [flags...]
assert_dump_round_trip() {
  local step_name="$1"
  shift
  local flags=("$@")

  step "$step_name"

  local tmp_sql
  tmp_sql=$(mktemp)

  if ! "$PISTA" dump "${flags[@]}" > "$tmp_sql" 2>/dev/null; then
    rm -f "$tmp_sql"
    fail "dump failed"
    return 1
  fi

  local plan_output
  plan_output=$(pista_plan "${flags[@]}" "$tmp_sql") || {
    rm -f "$tmp_sql"
    fail "plan failed: $plan_output"
    return 1
  }
  rm -f "$tmp_sql"

  if ! echo "$plan_output" | grep -qF -e '-- No changes'; then
    fail "expected no changes when the dump is fed back"
    echo "    $plan_output" >&2
    return 1
  fi

  pass
}

# Assert that plan fails and that its message contains the given text.
# Usage: assert_plan_error "step name" "expected message" files...
assert_plan_error() {
  local step_name="$1"
  local expected="$2"
  shift 2
  local files=("$@")

  step "$step_name"

  local plan_output
  local rc=0
  plan_output=$(pista_plan "${files[@]}") || rc=$?

  if [ "$rc" -eq 0 ]; then
    fail "expected plan to fail"
    echo "    $plan_output" >&2
    return 1
  fi

  if ! echo "$plan_output" | grep -qF "$expected"; then
    fail "unexpected error message"
    echo "    expected to contain: $expected" >&2
    echo "    actual: $plan_output" >&2
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
  plan_output=$(pista_plan "${files[@]}") || { fail "plan failed: $plan_output"; return 1; }

  if echo "$plan_output" | grep -q 'No changes'; then
    pass
  else
    fail "expected no changes"
    echo "    actual: $plan_output" >&2
    return 1
  fi
}
