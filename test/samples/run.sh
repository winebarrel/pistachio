#!/usr/bin/env bash
# Check that pista handles real-world sample schemas correctly.
#
# The sample list, downloads, and DB setup all live in the Makefile. This
# script only drives the check: for each sample it asks make to reset the
# database and load the sample, runs `pista dump` to capture pista's model of
# the schema, then runs `pista plan` against the dump. A faithful round-trip
# must plan to "No changes"; any diff means pista's catalog reader and parser
# disagree.
set -euo pipefail

cd "$(dirname "$0")/../.."

export PGHOST="${PGHOST:-localhost}"
export PGUSER="${PGUSER:-postgres}"
export PGPORT="${PGPORT:-5415}"

: "${PISTA:=./pista}"

_pass=0
_fail=0

# Indent stdin by four spaces for readable failure output.
indent() {
  # shellcheck disable=SC2001  # per-line prefix; parameter expansion can't do it
  sed 's/^/    /'
}

# check <name> <schemas> <flags>
# <schemas> is passed to pista -n; empty means default (public).
# <flags> are extra pista plan flags; empty for almost every sample.
check() {
  local name="$1" schemas="$2" flags="$3"
  local dump="${TMPDIR:-/tmp}/pista-sample-${name}.sql"
  printf "  %-20s " "$name"

  local ns=()
  [ -n "$schemas" ] && ns=(-n "$schemas")

  # shellcheck disable=SC2206  # $flags must word-split into separate flags
  local extra=($flags)

  if ! "$PISTA" dump "${ns[@]}" >"$dump" 2>"${dump}.err"; then
    echo "FAIL (dump)"
    indent <"${dump}.err" >&2
    _fail=$((_fail + 1))
    return
  fi

  local out
  out=$("$PISTA" plan "${ns[@]}" "${extra[@]}" "$dump" 2>&1) || {
    echo "FAIL (plan)"
    echo "$out" | indent >&2
    _fail=$((_fail + 1))
    return
  }

  if echo "$out" | grep -q 'No changes'; then
    echo "PASS"
    _pass=$((_pass + 1))
  else
    echo "DRIFT"
    echo "$out" | grep -v '^--' | indent >&2
    _fail=$((_fail + 1))
  fi
}

echo "Building pista..."
go build -o pista ./cmd/pista
PISTA="./pista"

echo ""
echo "Sample schema check (dump then plan; expect No changes):"

# Start from an empty database, so a schema left by an earlier run cannot make
# a load fail on objects that already exist. Between samples only `public` is
# reset; see the reset-db comment in the Makefile.
make -s clean-schema >/dev/null

# Iterate the sample manifest from the Makefile. For each sample let make
# reset the database and load the schema, then run the drift check here.
while IFS='|' read -r name target args schemas flags; do
  [ -n "$name" ] || continue

  make -s reset-db >/dev/null
  # shellcheck disable=SC2086  # $args must word-split into make VAR=value pairs
  if ! make -s "$target" $args >/dev/null; then
    printf "  %-20s FAIL (load)\n" "$name"
    _fail=$((_fail + 1))
    continue
  fi

  check "$name" "$schemas" "$flags"
done < <(make -s print-samples)

echo ""
echo "  ${_pass} passed, ${_fail} failed"
[ "$_fail" -eq 0 ]
