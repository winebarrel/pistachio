#!/usr/bin/env bash
# Scenario test: pist fmt (formatting SQL files)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/fmt"

# --- Step 1: stdout output matches expected ---
step "01 fmt stdout output"
actual=$("$PIST" fmt "$DATA/unformatted.sql" 2>&1)
expected=$(cat "$DATA/formatted.sql")
if [ "$actual" = "$expected" ]; then
  pass
else
  fail "stdout output mismatch"
  diff <(echo "$actual") <(echo "$expected") >&2 || true
fi

# --- Step 2: --check detects unformatted file ---
step "02 --check detects unformatted"
if "$PIST" fmt --check "$DATA/unformatted.sql" >/dev/null 2>&1; then
  fail "expected non-zero exit for unformatted file"
else
  pass
fi

# --- Step 3: --check passes for formatted file ---
step "03 --check passes for formatted"
if "$PIST" fmt --check "$DATA/formatted.sql" 2>&1; then
  pass
else
  fail "expected zero exit for formatted file"
fi

# --- Step 4: -w writes formatted output to file ---
step "04 -w writes formatted file"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
cp "$DATA/unformatted.sql" "$tmp_dir/input.sql"
"$PIST" fmt -w "$tmp_dir/input.sql" 2>&1
actual=$(cat "$tmp_dir/input.sql")
expected=$(cat "$DATA/formatted.sql")
if [ "$actual" = "$expected" ]; then
  pass
else
  fail "written file mismatch"
  diff <(echo "$actual") <(echo "$expected") >&2 || true
fi

# --- Step 5: -w then --check is idempotent ---
step "05 -w then --check idempotent"
if "$PIST" fmt --check "$tmp_dir/input.sql" 2>&1; then
  pass
else
  fail "formatted file should pass --check"
fi

# --- Step 6: fmt multiple files ---
step "06 fmt multiple files"
cp "$DATA/unformatted.sql" "$tmp_dir/a.sql"
cp "$DATA/unformatted2.sql" "$tmp_dir/b.sql"
"$PIST" fmt -w "$tmp_dir/a.sql" "$tmp_dir/b.sql" 2>&1
if "$PIST" fmt --check "$tmp_dir/a.sql" "$tmp_dir/b.sql" 2>&1; then
  pass
else
  fail "multiple files should pass --check after -w"
fi

# --- Step 7: --check lists unformatted files ---
step "07 --check lists unformatted files"
cp "$DATA/unformatted.sql" "$tmp_dir/c.sql"
check_output=$("$PIST" fmt --check "$tmp_dir/a.sql" "$tmp_dir/c.sql" 2>&1 || true)
if echo "$check_output" | grep -qF "$tmp_dir/c.sql"; then
  pass
else
  fail "expected unformatted file path in --check output"
  echo "    actual: $check_output" >&2
fi

summary
