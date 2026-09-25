#!/usr/bin/env bash
# Check that pista handles real-world sample schemas correctly.
#
# The sample list, downloads, and DB setup all live in sample-db.mk. This
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

# Routines are opt-in, and the round trip this script checks is exactly what
# they have to hold up: dump writes them, plan reads them back. Set as an
# environment variable rather than as a per-sample flag because it has to reach
# both the dump and the plan, and the manifest's flags column only reaches the
# plan.
export PISTA_MANAGE_ROUTINE=1

# A table's storage parameters are opt-in as well, and the samples carry the
# WITH clauses real schemas write, so the round trip has to hold up for them.
export PISTA_MANAGE_STORAGE_PARAM=1

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

# require_extension <extension> <package> <samples>
# SAMPLE names the samples to check, separated by commas, and unset or empty
# means all of them. A name the manifest does not hold is an error rather than
# a sample quietly skipped. make hands SAMPLE over in the environment rather
# than on the command line, so the value never passes through a shell.
_manifest=$(make -s print-samples)
_selected=()
IFS=',' read -r -a _parts <<<"${SAMPLE:-}"
for _name in "${_parts[@]}"; do
  [ -n "$_name" ] && _selected+=("$_name")
done
for _name in "${_selected[@]}"; do
  if ! cut -d'|' -f1 <<<"$_manifest" | grep -qxF -- "$_name"; then
    echo "No sample named $_name. make print-samples lists them." >&2
    exit 1
  fi
done

# selected <name>
# True when <name> is to be checked.
selected() {
  [ ${#_selected[@]} -eq 0 ] && return 0
  local s
  for s in "${_selected[@]}"; do
    [ "$s" = "$1" ] && return 0
  done
  return 1
}

# The discourse, citizenlab, affine, lobehub, and formbricks samples need
# pgvector, and the osm, inaturalist, and citizenlab samples and the dhis2
# loader need PostGIS, neither of which the official postgres image ships. Say
# so up front: without them the sample fails at load time and the reason is
# buried in psql's output. The check is skipped when no selected sample needs
# the extension.
# require_extension <extension> <package> <sample>...
require_extension() {
  local ext="$1" pkg="$2" s needed=()
  shift 2
  for s in "$@"; do
    selected "$s" && needed+=("$s")
  done
  [ ${#needed[@]} -gt 0 ] || return 0
  if [ -z "$(psql -X -q -At -c "SELECT 1 FROM pg_available_extensions WHERE name = '$ext'")" ]; then
    echo "$pkg is not installed on this server. It is needed by ${needed[*]}." >&2
    echo "compose.yaml installs it at container start; recreate the container with" >&2
    echo "  docker compose down && docker compose up -d" >&2
    exit 1
  fi
}

require_extension vector pgvector discourse citizenlab affine lobehub formbricks
require_extension postgis PostGIS osm inaturalist dhis2 citizenlab

echo "Building pista..."
go build -o pista ./cmd/pista
PISTA="./pista"

echo ""
echo "Sample schema check (dump then plan; expect No changes):"

# Start from an empty database, so a schema left by an earlier run cannot make
# a load fail on objects that already exist. Between samples only `public` is
# reset; see the reset-db comment in sample-db.mk.
make -s clean-schema >/dev/null

# Iterate the sample manifest from sample-db.mk. For each sample let make
# reset the database and load the schema, then run the drift check here.
while IFS='|' read -r name target args schemas flags; do
  [ -n "$name" ] || continue
  selected "$name" || continue

  make -s reset-db >/dev/null
  # shellcheck disable=SC2086  # $args must word-split into make VAR=value pairs
  if ! make -s "$target" $args >/dev/null; then
    printf "  %-20s FAIL (load)\n" "$name"
    _fail=$((_fail + 1))
    continue
  fi

  check "$name" "$schemas" "$flags"
done <<<"$_manifest"

echo ""
echo "  ${_pass} passed, ${_fail} failed"
[ "$_fail" -eq 0 ]
