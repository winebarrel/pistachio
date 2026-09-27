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

# via_git NAME BASE PR MAIN EXPECTED
#
# Runs a case through git, the way changelog-merge.yml does: the attribute
# alone leaves merge-tree a text merge, and git merge with the driver
# configured gives EXPECTED. The workflow pushes only when the two differ,
# so a case whose text merge already gives EXPECTED fails here.
via_git() {
    local repo=$tmp/$1 tree text
    repo=${repo// /-}
    git init -q -b main "$repo"
    g() { git -C "$repo" -c user.name=test -c user.email=test@example.com "$@"; }
    printf '%s' "$2" > "$repo/CHANGELOG.md"
    g add CHANGELOG.md
    g commit -q -m base
    g checkout -q -b pr
    printf '%s' "$3" > "$repo/CHANGELOG.md"
    g commit -q -am pr
    g checkout -q main
    printf '%s' "$4" > "$repo/CHANGELOG.md"
    g commit -q -am main
    echo 'CHANGELOG.md merge=changelog' >> "$repo/.git/info/attributes"
    if tree=$(g merge-tree --write-tree --name-only --no-messages main pr); then
        text=$(g cat-file -p "$tree:CHANGELOG.md")
    elif [ "$(sed 1d <<<"$tree")" = CHANGELOG.md ]; then
        text=
    else
        echo "FAIL $1: merge-tree reports '$(sed 1d <<<"$tree")'"
        failed=1
        return
    fi
    g checkout -q pr
    if [ "$text" = "${5%$'\n'}" ]; then
        echo "FAIL $1: the text merge already gives the expected file"
        failed=1
    elif ! g -c merge.changelog.driver="bash $driver %O %A %B" merge -q --no-edit main > /dev/null 2>&1; then
        echo "FAIL $1: merge failed"
        failed=1
    elif ! diff -u <(printf '%s' "$5") "$repo/CHANGELOG.md"; then
        echo "FAIL $1"
        failed=1
    else
        echo "ok   $1"
    fi
}

via_git 'git: both sides add an entry' \
"# Changelog

$released" \
"# Changelog

## [Unreleased]

* PR.

$released" \
"# Changelog

## [Unreleased]

* Main.

$released" \
"# Changelog

## [Unreleased]

* PR.

* Main.

$released"

via_git 'git: a release after the branch' \
"# Changelog

## [Unreleased]

* A.

$released" \
"# Changelog

## [Unreleased]

* PR.

* A.

$released" \
"# Changelog

## [1.1.0] - 2026-02-01

* A.

$released" \
"# Changelog

## [Unreleased]

* PR.

## [1.1.0] - 2026-02-01

* A.

$released"

exit "$failed"
