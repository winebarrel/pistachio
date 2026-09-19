#!/usr/bin/env bash
# Scenario test: a range-partitioned table with its partitions managed.
# The companion scenario, skip_partition_child, covers the flag that leaves
# the partitions to another tool. Here pista owns them: the steps add and
# drop partitions, push a column and an index down from the parent, index one
# partition on its own, widen the primary key so a partition can be
# partitioned again, and finally drop the whole tree.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/partition"

# --- Setup: load the initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: the initial schema round-trips with no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: add a partition ---
run_step "01 add partition (events_2025)" \
  "CREATE TABLE public.events_2025 PARTITION OF public.events FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');" \
  "$DATA/steps/01_add_partition.sql" || true

# --- Step 2: add a default partition ---
run_step "02 add default partition" \
  "CREATE TABLE public.events_default PARTITION OF public.events DEFAULT;" \
  "$DATA/steps/02_add_default_partition.sql" || true

# --- Step 3: a column on the parent reaches every partition ---
run_step "03 add column on the parent" \
  "ALTER TABLE public.events ADD COLUMN payload jsonb;" \
  "$DATA/steps/03_parent_column.sql" || true

# --- Step 4: an index on the parent is pushed down, and the copies it
#     creates on the partitions must not read as drift ---
run_step "04 add index on the parent" \
  "CREATE INDEX events_kind_idx ON public.events USING btree (kind);" \
  "$DATA/steps/04_parent_index.sql" || true

# --- Step 5: an index on one partition alone ---
run_step "05 add index on one partition" \
  "CREATE INDEX events_2024_payload_idx ON public.events_2024 USING gin (payload);" \
  "$DATA/steps/05_partition_index.sql" || true

# --- Step 6: widen the primary key, which recurses into every partition ---
run_step "06 widen the primary key" "$(cat <<'EOS'
ALTER TABLE public.events DROP CONSTRAINT events_pkey;
ALTER TABLE public.events ADD CONSTRAINT events_pkey PRIMARY KEY (id, at, kind);
EOS
)" "$DATA/steps/06_extend_pkey.sql" || true

# --- Step 7: a partition that is partitioned again, with its own children ---
run_step "07 add a sub-partitioned partition" "$(cat <<'EOS'
CREATE TABLE public.events_2026 PARTITION OF public.events FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')
PARTITION BY LIST (kind);
CREATE TABLE public.events_2026_click PARTITION OF public.events_2026 FOR VALUES IN ('click');
CREATE TABLE public.events_2026_other PARTITION OF public.events_2026 DEFAULT;
EOS
)" "$DATA/steps/07_subpartition.sql" || true

# --- Step 8: the whole tree round-trips through dump ---
assert_dump_round_trip "08 dump output round-trips" || true

# --- Step 9: a partition drop needs --allow-drop ---
assert_commented_drop "09 partition drop is suppressed by default" table \
  "$DATA/steps/08_drop_partition.sql" || true

# --- Step 10: with the drop allowed, the partition and its index go ---
run_step "10 drop a partition" \
  "DROP TABLE public.events_2024;" \
  "$DATA/steps/08_drop_partition.sql" || true

# --- Step 11: the parent goes after its partitions, deepest child first ---
run_step "11 drop the whole partitioned table" "$(cat <<'EOS'
DROP TABLE public.events_2026_other;
DROP TABLE public.events_2026_click;
DROP TABLE public.events_2026;
DROP TABLE public.events;
EOS
)" "$DATA/steps/09_drop_all.sql" || true

summary
