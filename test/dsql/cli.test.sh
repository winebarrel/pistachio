#!/usr/bin/env bash
# DSQL scenario: the CLI paths the sample scenarios do not take. A change DSQL
# cannot make is refused at plan, a plan file is applied with apply-from,
# dump --split plans clean, and apply --dsql-no-wait-job leaves no drift.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

cat > "$WORK/items.sql" <<'EOS'
CREATE TABLE items (
    id    bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),
    name  text NOT NULL,
    price integer,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
CREATE INDEX items_name_idx ON items (name);
EOS

reset_db

# --- Step 0: build the table ---
run_step "00 build a table" "$(cat <<'EOS'
CREATE TABLE public.items (
CREATE INDEX ASYNC items_name_idx ON public.items (name);
EOS
)" "$WORK/items.sql" || true

# --- Steps 1 and 2: a change DSQL cannot make is refused at plan ---
derive "$WORK/type.sql" "$WORK/items.sql" 's/price integer/price bigint/'
assert_plan_error "01 refuse a column type change" \
  "DSQL does not support a column type change" "$WORK/type.sql" || true

derive "$WORK/not_null.sql" "$WORK/items.sql" 's/price integer/price integer NOT NULL/'
assert_plan_error "02 refuse SET NOT NULL" \
  "DSQL does not support SET NOT NULL" "$WORK/not_null.sql" || true

# --- Step 3: apply-from runs a plan file under the engine it records, and
#     waits for the validation job ---
derive "$WORK/check.sql" "$WORK/items.sql" '
  s/(    price integer)/$1,\n    note  text DEFAULT \x27\x27,\n    CONSTRAINT items_price_check CHECK (price >= 0)/;
'
step "03 apply a plan file with apply-from"
plan_file="$WORK/plan.json"
if ! out=$("$PISTA" plan --allow-drop all --out "$plan_file" "$WORK/check.sql" 2>&1); then
  fail "plan failed: $out"
elif ! out=$("$PISTA" apply-from "$plan_file" 2>&1); then
  fail "apply-from failed: $out"
elif ! printf '%s\n' "$out" | grep -qF 'ALTER TABLE ASYNC public.items VALIDATE CONSTRAINT items_price_check;'; then
  fail "apply-from did not validate in a job: $out"
elif ! out=$(pista_plan "$WORK/check.sql") || ! printf '%s\n' "$out" | grep -q 'No changes'; then
  fail "drift after apply-from: $out"
else
  pass
fi

# --- Step 4: dump --split plans clean ---
step "04 dump --split plans clean"
if ! out=$("$PISTA" dump --split "$WORK/split" 2>&1); then
  fail "dump failed: $out"
elif ! out=$(pista_plan "$WORK"/split/*.sql) || ! printf '%s\n' "$out" | grep -qF -e '-- No changes'; then
  fail "expected no changes when the split dump is fed back: $out"
else
  pass
fi

# --- Step 5: an index built without waiting for its job leaves no drift ---
derive "$WORK/index.sql" "$WORK/check.sql" 's/ON items \(name\)/ON items (name, price)/'
step "05 apply --dsql-no-wait-job"
if ! out=$(pista_apply --dsql-no-wait-job "$WORK/index.sql"); then
  fail "apply failed: $out"
elif ! printf '%s\n' "$out" | grep -qF 'CREATE INDEX ASYNC items_name_idx ON public.items (name, price);'; then
  fail "unexpected apply output: $out"
elif ! out=$(pista_plan "$WORK/index.sql") || ! printf '%s\n' "$out" | grep -q 'No changes'; then
  fail "drift after apply: $out"
else
  pass
fi

# --- Step 6: drop everything ---
run_step "06 drop everything" "DROP TABLE public.items;" "$WORK/empty.sql" || true

summary
