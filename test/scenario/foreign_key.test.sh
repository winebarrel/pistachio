#!/usr/bin/env bash
# Scenario test: foreign key lifecycle and the ordering a key forces.
# The steps add a key NOT VALID and validate it, change its actions, add a
# composite and a deferrable self-referencing key, create a pair of tables
# that reference each other in one plan, and drop keys and tables in an order
# PostgreSQL accepts.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

DATA="$SCRIPT_DIR/testdata/foreign_key"

# --- Setup: load the initial schema ---
setup_db "$DATA/init.sql"

# --- Step 0: the initial schema round-trips with no diff ---
run_step_no_diff "00 init: no diff" "$DATA/init.sql" || true

# --- Step 1: add the key NOT VALID, which skips the scan ---
run_step "01 add foreign key NOT VALID" \
  "ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES public.customers (id) NOT VALID;" \
  "$DATA/steps/01_add_fk_not_valid.sql" || true

# --- Step 2: taking NOT VALID off validates rather than re-adds ---
run_step "02 validate the foreign key" \
  "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_customer_fk;" \
  "$DATA/steps/02_validate_fk.sql" || true

# --- Step 3: a changed referential action is a drop and an add ---
run_step "03 change ON UPDATE / ON DELETE" "$(cat <<'EOS'
ALTER TABLE public.orders DROP CONSTRAINT orders_customer_fk;
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES public.customers (id) ON UPDATE CASCADE ON DELETE RESTRICT;
EOS
)" "$DATA/steps/03_change_action.sql" || true

# --- Step 4: a composite key and a deferrable self-referencing one ---
run_step "04 add composite and self-referencing keys" "$(cat <<'EOS'
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_cust_code_fk FOREIGN KEY (cust_code, cust_region) REFERENCES public.customers (code, region) ON DELETE SET NULL;
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_parent_fk FOREIGN KEY (parent_id) REFERENCES public.orders (id) DEFERRABLE INITIALLY DEFERRED;
EOS
)" "$DATA/steps/04_composite_and_self_fk.sql" || true

# --- Step 5: two tables that reference each other arrive in one plan, so
#     both tables are created before either key ---
run_step "05 create tables that reference each other" "$(cat <<'EOS'
CREATE TABLE public.invoices (
CREATE TABLE public.shipments (
ALTER TABLE ONLY public.invoices ADD CONSTRAINT invoices_shipment_fk FOREIGN KEY (shipment_id) REFERENCES public.shipments (id);
ALTER TABLE ONLY public.shipments ADD CONSTRAINT shipments_invoice_fk FOREIGN KEY (invoice_id) REFERENCES public.invoices (id);
EOS
)" "$DATA/steps/05_new_tables_circular_fk.sql" || true

# --- Step 6: --assume-validated leaves a key added NOT VALID out of band
#     alone, where a plain plan validates it ---
run_sql "ALTER TABLE public.orders ADD CONSTRAINT orders_customer_fk2 FOREIGN KEY (customer_id) REFERENCES public.customers(id) NOT VALID"
run_step_no_diff "06 assume-validated ignores NOT VALID" \
  --assume-validated "$DATA/steps/06_assume_validated.sql" || true

# --- Step 7: without the flag the same file validates the key ---
run_step "07 validate the out-of-band key" \
  "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_customer_fk2;" \
  "$DATA/steps/06_assume_validated.sql" || true
run_sql "ALTER TABLE public.orders DROP CONSTRAINT orders_customer_fk2"

# --- Step 8: dropping the pair drops both keys first ---
run_step "08 drop the tables that reference each other" "$(cat <<'EOS'
ALTER TABLE public.invoices DROP CONSTRAINT invoices_shipment_fk;
ALTER TABLE public.shipments DROP CONSTRAINT shipments_invoice_fk;
DROP TABLE public.invoices;
DROP TABLE public.shipments;
EOS
)" "$DATA/steps/07_drop_circular_tables.sql" || true

# --- Step 9: dropping the referenced table drops the keys that name it ---
run_step "09 drop the referenced table" "$(cat <<'EOS'
ALTER TABLE public.orders DROP CONSTRAINT orders_cust_code_fk;
ALTER TABLE public.orders DROP CONSTRAINT orders_customer_fk;
DROP TABLE public.customers;
EOS
)" "$DATA/steps/08_drop_referenced_table.sql" || true

# --- Step 10: a key drop of its own needs --allow-drop foreign_key ---
assert_commented_drop "10 fk drop is suppressed by default" foreign_key:orders_parent_fk \
  "$DATA/steps/09_drop_fk.sql" || true
assert_drop_type_present "11 fk drop runs with --allow-drop foreign_key" \
  foreign_key:orders_parent_fk foreign_key "$DATA/steps/09_drop_fk.sql" || true

# --- Step 12: with the drop allowed, the key goes ---
run_step "12 drop the self-referencing key" \
  "ALTER TABLE public.orders DROP CONSTRAINT orders_parent_fk;" \
  "$DATA/steps/09_drop_fk.sql" || true

summary
