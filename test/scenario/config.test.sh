#!/usr/bin/env bash
# Scenario test: --config loads options from a YAML file.
# Exercises the real CLI wiring (kong.Configuration + the --config flag) and
# the flag > env > config precedence end to end.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/config"

setup_db "$DATA/init.sql"

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

# --- Step 1: options in the config file are applied ---
step "01 --config applies exclude"
printf 'exclude:\n  - tmp_*\n' > "$tmp_dir/pista.yml"
out=$("$PISTA" --config "$tmp_dir/pista.yml" dump 2>&1) || out="DUMP FAILED: $out"
if echo "$out" | grep -q 'CREATE TABLE public.users' && ! echo "$out" | grep -q 'tmp_cache'; then
  pass
else
  fail "exclude from config not applied"
  echo "    $out" >&2
fi

# --- Step 2: an unknown key is rejected ---
step "02 unknown config key is rejected"
printf 'bogus: 1\n' > "$tmp_dir/bad.yml"
if err=$("$PISTA" --config "$tmp_dir/bad.yml" dump 2>&1); then
  fail "expected an error for an unknown key"
  echo "    $err" >&2
elif echo "$err" | grep -q 'unknown config key'; then
  pass
else
  fail "unexpected error: $err"
fi

# --- Step 3: an environment variable overrides the config file ---
step "03 env overrides config"
printf 'exclude:\n  - users\n' > "$tmp_dir/pista.yml"
out=$(PISTA_EXCLUDE='tmp_*' "$PISTA" --config "$tmp_dir/pista.yml" dump 2>&1) || out="DUMP FAILED: $out"
if echo "$out" | grep -q 'CREATE TABLE public.users' && ! echo "$out" | grep -q 'tmp_cache'; then
  pass
else
  fail "env did not override config exclude"
  echo "    $out" >&2
fi

# --- Step 4: a command-line flag overrides both env and config ---
step "04 flag overrides env and config"
out=$(PISTA_EXCLUDE='tmp_*' "$PISTA" --config "$tmp_dir/pista.yml" dump --exclude 'nomatch_*' 2>&1) || out="DUMP FAILED: $out"
if echo "$out" | grep -q 'CREATE TABLE public.users' && echo "$out" | grep -q 'CREATE TABLE public.tmp_cache'; then
  pass
else
  fail "flag did not override env and config exclude"
  echo "    $out" >&2
fi

summary
