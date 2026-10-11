#!/usr/bin/env bash
# Fail when non-test code imports internal/testutil or a package under it.
#
# `go list` reports the imports of a package's non-test files in .Imports and
# those of its tests in .TestImports and .XTestImports, so reading .Imports
# alone finds the non-test code that reaches for a test helper. The helpers
# themselves import each other and are left out.

set -euo pipefail

found=$(go list -f '{{.ImportPath}}{{range .Imports}} {{.}}{{end}}' ./... |
    awk '$1 !~ /\/internal\/testutil(\/|$)/ {
        for (i = 2; i <= NF; i++)
            if ($i ~ /\/internal\/testutil(\/|$)/)
                print $1 " -> " $i
    }')

if [ -n "$found" ]; then
    printf 'Non-test code imports a test helper:\n%s\n' "$found" >&2
    exit 1
fi
