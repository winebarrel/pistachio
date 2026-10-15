#!/usr/bin/env bash
# Merge the coverage profiles the CI test jobs upload and write the report
# winebarrel/pistachio-coverage serves on GitHub Pages:
#
#   coverage.txt   the merged profile
#   coverage.html  go tool cover -html of it
#   index.html     a list of the files with their coverage
#   badge.svg      a badge with the total
#
# Usage: scripts/coverage-report.sh OUTDIR PROFILE...
#
# Prints the total, in percent, on stdout. Each test job runs only part of
# the code (a PostgreSQL version, an OS, DSQL), so a block counts as covered
# when any profile covers it.

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "usage: $0 OUTDIR PROFILE..." >&2
    exit 2
fi

outdir=$1
shift
mkdir -p "$outdir"

# Test helpers and tools that are not part of pista.
module=$(go list -m)
ignore="^$module/(internal/testutil|cmd/gen-json-schema|test)/"

# A block is keyed on its position and statement count. In set mode its
# count is 0 or 1, so a block covered by any profile counts as covered; in
# count and atomic mode the counts add up.
awk -v ignore="$ignore" '
FNR == 1 {
    if (mode == "") {
        mode = $2
    } else if ($2 != mode) {
        printf "%s: mode %s, others %s\n", FILENAME, $2, mode > "/dev/stderr"
        failed = 1
        exit 1
    }
    next
}
$1 ~ ignore { next }
{
    key = $1 " " $2
    if (!(key in count)) {
        order[n++] = key
        count[key] = 0
    }
    count[key] = mode == "set" ? (count[key] || $3) : count[key] + $3
}
END {
    if (failed) {
        exit 1
    }
    print "mode: " mode
    for (i = 0; i < n; i++) {
        print order[i], count[order[i]]
    }
}' "$@" >"$outdir/coverage.txt"

go tool cover -html="$outdir/coverage.txt" -o "$outdir/coverage.html"
total=$(go tool cover -func="$outdir/coverage.txt" | awk '$1 == "total:" { sub("%", "", $NF); print $NF }')

# index.html lists the files, least covered first, each linked to its page
# in coverage.html. That page is #fileN, where N counts the files in the
# order go tool cover sorts them, which is by name, byte by byte.
{
    cat <<EOF
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pistachio coverage</title>
<style>
:root { color-scheme: light dark; --fg: #1f2328; --bg: #fff; --line: #d1d9e0; --bar: #2da44e; --track: #eaeef2; }
@media (prefers-color-scheme: dark) {
  :root { --fg: #e6edf3; --bg: #0d1117; --line: #3d444d; --bar: #3fb950; --track: #262c36; }
}
body { margin: 0 auto; max-width: 960px; padding: 16px; font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: var(--fg); background: var(--bg); }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 4px 8px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; }
th:first-child, td:first-child { text-align: left; white-space: normal; word-break: break-all; }
a { color: inherit; }
.bar { display: inline-block; width: 80px; height: 8px; margin-left: 8px; background: var(--track); vertical-align: middle; }
.bar span { display: block; height: 100%; background: var(--bar); }
</style>
</head>
<body>
<h1>Coverage: $total%</h1>
<table>
<thead><tr><th>File</th><th>Statements</th><th>Missed</th><th>Coverage</th></tr></thead>
<tbody>
EOF
    awk 'NR > 1 {
        file = substr($1, 1, index($1, ":") - 1)
        stmts[file] += $2
        if ($3 > 0) {
            covered[file] += $2
        }
    }
    END {
        for (file in stmts) {
            print file, stmts[file], covered[file] + 0
        }
    }' "$outdir/coverage.txt" |
        LC_ALL=C sort |
        awk -v prefix="$module/" '{
            pct = $2 > 0 ? 100 * $3 / $2 : 0
            name = substr($1, 1, length(prefix)) == prefix ? substr($1, length(prefix) + 1) : $1
            printf "%.3f\t%s\t<tr><td><a href=\"coverage.html#file%d\">%s</a></td><td>%d</td><td>%d</td><td>%.1f%%<span class=\"bar\"><span style=\"width: %.1f%%\"></span></span></td></tr>\n",
                pct, name, NR - 1, name, $2, $2 - $3, pct, pct
        }' |
        LC_ALL=C sort -t "$(printf '\t')" -k1,1n -k2,2 |
        cut -f3
    cat <<EOF
</tbody>
</table>
</body>
</html>
EOF
} >"$outdir/index.html"

if awk -v t="$total" 'BEGIN { exit !(t >= 90) }'; then
    color='#4c1'
elif awk -v t="$total" 'BEGIN { exit !(t >= 80) }'; then
    color='#97ca00'
elif awk -v t="$total" 'BEGIN { exit !(t >= 70) }'; then
    color='#dfb317'
else
    color='#e05d44'
fi

# The widths fit "coverage" and "100.0%" in 11px Verdana.
cat >"$outdir/badge.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" width="115" height="20" role="img" aria-label="coverage: $total%">
  <title>coverage: $total%</title>
  <linearGradient id="s" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <clipPath id="r"><rect width="115" height="20" rx="3" fill="#fff"/></clipPath>
  <g clip-path="url(#r)">
    <rect width="61" height="20" fill="#555"/>
    <rect x="61" width="54" height="20" fill="$color"/>
    <rect width="115" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="11">
    <text x="30.5" y="15" fill="#010101" fill-opacity=".3">coverage</text>
    <text x="30.5" y="14">coverage</text>
    <text x="88" y="15" fill="#010101" fill-opacity=".3">$total%</text>
    <text x="88" y="14">$total%</text>
  </g>
</svg>
EOF

# Pages would otherwise run the files through Jekyll.
touch "$outdir/.nojekyll"

echo "$total"
