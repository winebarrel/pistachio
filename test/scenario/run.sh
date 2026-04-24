#!/usr/bin/env bash
# Run all scenario tests.
set -euo pipefail

cd "$(dirname "$0")/../.."

echo "Building pist..."
go build -o pist ./cmd/pist
export PIST="./pist"

rc=0
for script in test/scenario/*.test.sh; do
  echo ""
  echo "=== $(basename "$script" .test.sh) ==="
  if bash "$script"; then
    :
  else
    rc=1
  fi
done

echo ""
if [ "$rc" -eq 0 ]; then
  echo "All scenarios passed."
else
  echo "Some scenarios failed."
fi

exit "$rc"
