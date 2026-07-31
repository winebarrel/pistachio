#!/usr/bin/env bash
# Regenerate model/testdata/keywords.txt from the kwlist.h bundled with the
# pg_query_go version in go.mod. Run it after upgrading pg_query_go;
# TestIdent_allKeywords fails on a stale corpus.

set -euo pipefail

out=model/testdata/keywords.txt
mod=github.com/pganalyze/pg_query_go/v6
dir=$(go list -m -f '{{.Dir}}' "$mod")
kwlist="$dir/parser/include/postgres/parser/kwlist.h"

if [ ! -f "$kwlist" ]; then
    echo "kwlist.h not found at $kwlist" >&2
    exit 1
fi

version=$(go list -m -f '{{.Version}}' "$mod")

{
    echo "# PostgreSQL keywords, one per line, from the kwlist.h bundled with"
    echo "# pg_query_go $version. Categories are not recorded here: util_test.go"
    echo "# derives each one through pg_query.Scan at run time."
    echo "#"
    echo "# Regenerate with: make keywords"
    sed -n 's/^PG_KEYWORD("\([a-z_0-9]*\)".*/\1/p' "$kwlist"
} > "$out"

echo "wrote $out ($(grep -cv '^#' "$out") keywords)"
