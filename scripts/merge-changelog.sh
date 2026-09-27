#!/usr/bin/env bash
# Git merge driver for CHANGELOG.md: merge-changelog.sh %O %A %B
#
# Two branches that each add an entry under ## [Unreleased] conflict in a
# plain text merge, since both insert at the same place. This driver merges
# the file with that section taken out, then puts the section back holding
# the entries of both sides: the ones %A added first, then those of %B.
# An entry that was in the base and is gone from either side stays gone,
# so an entry a release moved under its version is not added back.
#
# A conflict outside the section falls back to git's own merge, conflict
# markers included, and the driver fails. changelog-merge.yml runs it on
# the pull requests into main that change CHANGELOG.md.

set -euo pipefail

base=$1 ours=$2 theirs=$3
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Writes the file without the section to $2.stripped and the entries of
# the section to $2.entries, each followed by a line holding only \036.
# An entry is a paragraph; an indented one continues the entry above it.
split() {
    awk -v stripped="$2.stripped" -v entries="$2.entries" '
        function flush() {
            if (entry != "") printf "%s\n\036\n", entry > entries
            entry = ""
        }
        /^## / { in_section = ($0 == "## [Unreleased]"); if (in_section) next }
        !in_section { print > stripped; next }
        /^$/ { blank = 1; next }
        {
            if (entry != "" && blank && $0 !~ /^[ \t]/) flush()
            if (entry == "") entry = $0
            else entry = entry (blank ? "\n\n" : "\n") $0
            blank = 0
        }
        END { flush(); printf "" > stripped; printf "" > entries }
    ' "$1"
}

split "$base" "$tmp/base"
split "$ours" "$tmp/ours"
split "$theirs" "$tmp/theirs"

if ! git merge-file -p "$tmp/ours.stripped" "$tmp/base.stripped" "$tmp/theirs.stripped" > "$tmp/merged"; then
    git merge-file "$ours" "$base" "$theirs"
    exit 1
fi

section=$tmp/section
awk -v base="$tmp/base.entries" -v ours="$tmp/ours.entries" -v theirs="$tmp/theirs.entries" -v section="$section" '
    function load(file, list, set,    n, entry, line) {
        n = 0
        entry = ""
        while ((getline line < file) > 0) {
            if (line == "\036") { list[++n] = entry; set[entry] = 1; entry = "" }
            else entry = entry == "" ? line : entry "\n" line
        }
        return n
    }
    function add(entry) {
        if (entry in seen) return
        seen[entry] = 1
        out = out == "" ? entry : out "\n\n" entry
    }
    BEGIN {
        load(base, b, in_base)
        n_ours = load(ours, o, in_ours)
        n_theirs = load(theirs, t, in_theirs)
        for (i = 1; i <= n_ours; i++) if (!(o[i] in in_base)) add(o[i])
        for (i = 1; i <= n_theirs; i++) if (!(t[i] in in_base) || t[i] in in_ours) add(t[i])
        if (out != "") printf "%s", out > section
    }
'

awk -v section="$section" '
    BEGIN {
        if ((getline line < section) > 0) {
            text = line
            while ((getline line < section) > 0) text = text "\n" line
        }
    }
    text != "" && !done && /^## / { printf "## [Unreleased]\n\n%s\n\n", text; done = 1 }
    { print }
    END { if (text != "" && !done) printf "\n## [Unreleased]\n\n%s\n", text }
' "$tmp/merged" > "$ours"
