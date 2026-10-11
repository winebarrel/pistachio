#!/usr/bin/env bash
# Fail when non-test code imports a package that only the tests may use.
#
# `go list` reports the imports of a package's non-test files in .Imports;
# the imports of its tests are kept apart, so the tests themselves are free
# to import these packages.

set -euo pipefail

# The packages only the tests may import. A pattern ending in /... takes the
# packages under it too.
test_only=$(go list \
    ./internal/testutil/...)

status=0
while read -r pkg imports; do
    # The test-only packages may import each other.
    if grep -qxF "$pkg" <<<"$test_only"; then
        continue
    fi
    for imp in $imports; do
        if grep -qxF "$imp" <<<"$test_only"; then
            echo "$pkg imports $imp, which only the tests may use" >&2
            status=1
        fi
    done
done < <(go list -f '{{.ImportPath}} {{join .Imports " "}}' ./...)

exit "$status"
