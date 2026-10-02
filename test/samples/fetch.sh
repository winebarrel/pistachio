#!/usr/bin/env bash
# Write the body at a URL to stdout, as `curl -sSfL` does. The sample loaders in
# sample-db.mk fetch every source through this script.
#
# When SAMPLE_CACHE names a directory, the body is kept there under the SHA-256
# of its URL and read from there the next time. Every source is pinned to a
# commit or a release file, so a kept body does not go stale. CI keeps the
# directory with actions/cache, keyed on sample-db.mk.
set -euo pipefail

url="$1"

if [ -z "${SAMPLE_CACHE:-}" ]; then
  exec curl -sSfL --retry 3 --retry-delay 2 "$url"
fi

mkdir -p "$SAMPLE_CACHE"
file="$SAMPLE_CACHE/$(printf '%s' "$url" | sha256sum | cut -d' ' -f1)"

# Download to a temporary file first, so a failed download leaves nothing that
# a later run would take for the whole body.
if [ ! -f "$file" ]; then
  tmp=$(mktemp "$file.XXXXXX")
  trap 'rm -f "$tmp"' EXIT
  curl -sSfL --retry 3 --retry-delay 2 -o "$tmp" "$url"
  mv "$tmp" "$file"
else
  # CI removes the bodies no run read, by modification time.
  touch "$file"
fi

exec cat "$file"
