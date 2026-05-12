#!/usr/bin/env bash
# Run `go tool deadcode` and filter out findings marked with `//deadcode:keep`.
#
# The marker must appear (with optional leading whitespace) on a line within
# the contiguous comment-or-blank block immediately above the declaration.
# Exits non-zero with the unmarked findings if any remain.

set -euo pipefail

output=$(go tool deadcode -test=false ./cmd/pista)
[ -z "$output" ] && exit 0

unmarked=""
while IFS= read -r line; do
    [ -z "$line" ] && continue

    file=${line%%:*}
    rest=${line#*:}
    lineno=${rest%%:*}

    found=0
    cur=$((lineno - 1))
    limit=30
    while [ "$cur" -gt 0 ] && [ "$limit" -gt 0 ]; do
        text=$(sed -n "${cur}p" "$file")
        stripped=$(printf '%s' "$text" | sed 's/^[[:space:]]*//')
        case "$stripped" in
            "//deadcode:keep"*) found=1; break ;;
            "//"*|"") ;;
            *) break ;;
        esac
        cur=$((cur - 1))
        limit=$((limit - 1))
    done

    if [ "$found" -eq 0 ]; then
        unmarked+="$line"$'\n'
    fi
done <<< "$output"

if [ -n "$unmarked" ]; then
    printf 'Unmarked dead code (add //deadcode:keep above the declaration to suppress):\n' >&2
    printf '%s' "$unmarked" >&2
    exit 1
fi
