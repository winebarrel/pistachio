#!/usr/bin/env bash
# Tests scripts/merge-changelog.sh, the CHANGELOG.md merge driver that
# changelog-merge.yml uses. Each case runs the driver on a base, ours and
# theirs, and compares the result and exit status with the expected ones.

set -uo pipefail

driver=$(cd "$(dirname "$0")/../.." && pwd)/scripts/merge-changelog.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
failed=0

# check NAME STATUS BASE OURS THEIRS EXPECTED
check() {
    printf '%s' "$3" > "$tmp/base"
    printf '%s' "$4" > "$tmp/ours"
    printf '%s' "$5" > "$tmp/theirs"
    bash "$driver" "$tmp/base" "$tmp/ours" "$tmp/theirs" > /dev/null 2>&1
    status=$?
    if [ "$status" -ne "$2" ]; then
        echo "FAIL $1: exit status $status, want $2"
        failed=1
    elif [ "$2" -eq 0 ] && ! diff -u <(printf '%s' "$6") "$tmp/ours"; then
        echo "FAIL $1"
        failed=1
    elif [ "$2" -ne 0 ] && ! grep -q '^<<<<<<<' "$tmp/ours"; then
        echo "FAIL $1: no conflict markers"
        failed=1
    else
        echo "ok   $1"
    fi
}

released='## [1.0.0] - 2026-01-01

* Old.
'

check 'both sides add an entry' 0 \
"# Changelog

## [Unreleased]

* B.

$released" \
"# Changelog

## [Unreleased]

* Ours.

* B.

$released" \
"# Changelog

## [Unreleased]

* Theirs.

* B.

$released" \
"# Changelog

## [Unreleased]

* Ours.

* Theirs.

* B.

$released"

check 'both sides create the section' 0 \
"# Changelog

$released" \
"# Changelog

## [Unreleased]

* Ours.

$released" \
"# Changelog

## [Unreleased]

* Theirs.

$released" \
"# Changelog

## [Unreleased]

* Ours.

* Theirs.

$released"

check 'theirs releases the section' 0 \
"# Changelog

## [Unreleased]

* B.

$released" \
"# Changelog

## [Unreleased]

* Ours.

* B.

$released" \
"# Changelog

## [1.1.0] - 2026-02-01

* B.

$released" \
"# Changelog

## [Unreleased]

* Ours.

## [1.1.0] - 2026-02-01

* B.

$released"

check 'ours releases the section' 0 \
"# Changelog

## [Unreleased]

* B.

$released" \
"# Changelog

## [1.1.0] - 2026-02-01

* B.

$released" \
"# Changelog

## [Unreleased]

* Theirs.

* B.

$released" \
"# Changelog

## [Unreleased]

* Theirs.

## [1.1.0] - 2026-02-01

* B.

$released"

check 'an entry one side removes stays removed' 0 \
"# Changelog

## [Unreleased]

* Gone.

* Kept.

$released" \
"# Changelog

## [Unreleased]

* Kept.

$released" \
"# Changelog

## [Unreleased]

* Theirs.

* Gone.

* Kept.

$released" \
"# Changelog

## [Unreleased]

* Theirs.

* Kept.

$released"

check 'an indented paragraph belongs to the entry above' 0 \
"# Changelog

$released" \
"# Changelog

## [Unreleased]

* Ours.
  * Nested.

  More about ours.

$released" \
"# Changelog

## [Unreleased]

* Theirs.

$released" \
"# Changelog

## [Unreleased]

* Ours.
  * Nested.

  More about ours.

* Theirs.

$released"

check 'a conflict outside the section fails' 1 \
"# Changelog

$released" \
"# Changelog

## [1.0.0] - 2026-01-01

* Old, edited by ours.
" \
"# Changelog

## [1.0.0] - 2026-01-01

* Old, edited by theirs.
" \
''

exit "$failed"
