#!/usr/bin/env bash
# Check that `pista dump` reproduces the schema it was taken from.
#
# The other two suites ask pistachio about itself. test/samples feeds a dump
# back to `pista plan` and requires "No changes", which catches the catalog
# reader and the parser disagreeing but passes whenever both sides overlook
# the same thing. The Go fixtures compare against an expected output someone
# wrote, so they only cover what someone thought to write down.
#
# Here PostgreSQL decides. For each schema the check loads it, records
# `pg_dump -s`, loads `pista dump` into an empty database, and records
# `pg_dump -s` again. The two have to be identical. Nothing pistachio drops
# can hide, whether or not anyone knew to look for it.
#
# A schema file therefore states what pistachio manages. Adding a construct it
# does not manage fails the check, which is the point: it is a decision to
# make, not a diff to silence.
#
# pg_dump has to be at least as new as the server, which is its own rule, not
# one this check adds.
set -uo pipefail

cd "$(dirname "$0")/../.."

export PGHOST="${PGHOST:-localhost}"
export PGUSER="${PGUSER:-postgres}"
export PGPORT="${PGPORT:-5415}"

# Routines are opt-in, and the schemas here dump and reload them like anything
# else. Set as an environment variable so it reaches every pista call.
export PISTA_MANAGE_ROUTINE=1

: "${PISTA:=./pista}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

_pass=0
_fail=0

# Drop what pg_dump prints around the schema itself. The two dumps come off the
# same server, so their SET and comment lines agree, except for the \restrict
# and \unrestrict tokens PostgreSQL 18's pg_dump randomizes per run.
schema_only() {
  pg_dump -s -n public \
    | grep -v -e '^--' -e '^$' -e '^SET ' -e "^SELECT pg_catalog.set_config" -e '^\\restrict ' -e '^\\unrestrict '
}

reset_db() {
  psql -X -q -v ON_ERROR_STOP=1 \
    -c 'SET client_min_messages TO warning; DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public'
}

indent() {
  sed 's/^/    /'
}

check() {
  local file="$1"
  local name
  name="$(basename "$file" .sql)"
  printf "  %-24s " "$name"

  reset_db >/dev/null 2>&1
  if ! psql -X -q -v ON_ERROR_STOP=1 -f "$file" >"$WORK/load.log" 2>&1; then
    echo "FAIL (load)"
    indent <"$WORK/load.log" >&2
    _fail=$((_fail + 1))
    return
  fi
  if ! schema_only >"$WORK/expected.sql" 2>"$WORK/pg_dump.err"; then
    echo "FAIL (pg_dump)"
    indent <"$WORK/pg_dump.err" >&2
    _fail=$((_fail + 1))
    return
  fi

  # --sort-by-deps because the reload below goes through psql, which needs a
  # table to exist before the one referencing it. `dump` is otherwise sorted
  # by name, which is what the other suites read.
  if ! "$PISTA" dump --sort-by-deps >"$WORK/dumped.sql" 2>"$WORK/dump.err"; then
    echo "FAIL (dump)"
    indent <"$WORK/dump.err" >&2
    _fail=$((_fail + 1))
    return
  fi

  reset_db >/dev/null 2>&1
  if ! psql -X -q -v ON_ERROR_STOP=1 -f "$WORK/dumped.sql" >"$WORK/reload.log" 2>&1; then
    echo "FAIL (reload)"
    indent <"$WORK/reload.log" >&2
    _fail=$((_fail + 1))
    return
  fi
  if ! schema_only >"$WORK/actual.sql" 2>"$WORK/pg_dump.err"; then
    echo "FAIL (pg_dump)"
    indent <"$WORK/pg_dump.err" >&2
    _fail=$((_fail + 1))
    return
  fi

  if diff -u "$WORK/expected.sql" "$WORK/actual.sql" >"$WORK/diff.txt"; then
    echo "PASS"
    _pass=$((_pass + 1))
  else
    # Read the diff as: - is what the schema had, + is what the dump restored.
    echo "DIFF"
    tail -n +3 "$WORK/diff.txt" | indent >&2
    _fail=$((_fail + 1))
  fi
}

echo "=== restore fidelity ==="
for f in test/fidelity/schemas/*.sql; do
  check "$f"
done

echo ""
echo "  ${_pass} passed, ${_fail} failed"
[ "$_fail" -eq 0 ]
