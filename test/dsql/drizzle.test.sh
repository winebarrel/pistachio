#!/usr/bin/env bash
# DSQL scenario: the pet clinic schema of the aws-samples Drizzle ORM sample.
# The sample adds its foreign keys NOT VALID and validates them with ALTER
# TABLE ASYNC, which pistachio cannot read, so the steps build the schema with
# the keys NOT VALID, validate them by taking NOT VALID off, change a key and
# a table, drop some of them, and drop everything.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

TABLES="$DSQL_SAMPLES/drizzle_0000.sql"
derive "$WORK/fk_not_valid.sql" "$DSQL_SAMPLES/drizzle_0001.sql" '
  s/ALTER TABLE ASYNC [^;]*;\n(--> statement-breakpoint\n)?//g;
'

reset_db

# --- Step 0: build the sample with its keys NOT VALID ---
run_step "00 build with keys NOT VALID" "$(cat <<'EOS'
CREATE TABLE public.owner (
CREATE TABLE public.specialty_to_vet (
CREATE INDEX ASYNC pet_owner_id_idx ON public.pet (owner_id);
ALTER TABLE ONLY public.pet ADD CONSTRAINT pet_owner_id_owner_id_fk FOREIGN KEY (owner_id) REFERENCES owner (id) ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID;
EOS
)" "$TABLES" "$WORK/fk_not_valid.sql" || true

# --- Step 1: taking NOT VALID off validates each key in a job ---
derive "$WORK/fk.sql" "$WORK/fk_not_valid.sql" 's/\n\tNOT VALID;/;/g'
run_step "01 validate the keys" "$(cat <<'EOS'
ALTER TABLE ASYNC public.pet VALIDATE CONSTRAINT pet_owner_id_owner_id_fk;
ALTER TABLE ASYNC public.specialty_to_vet VALIDATE CONSTRAINT specialty_to_vet_specialty_name_specialty_name_fk;
ALTER TABLE ASYNC public.specialty_to_vet VALIDATE CONSTRAINT specialty_to_vet_vet_id_vet_id_fk;
EOS
)" "$TABLES" "$WORK/fk.sql" || true

assert_dump_round_trip "02 dump plans clean" || true

# --- Step 3: change a key's action, add a column with a default and a
#     unique constraint, and add a deferrable key ---
derive "$WORK/tables_edit.sql" "$TABLES" '
  s/(\t"telephone" varchar\(20\))/$1,\n\t"email" varchar(120) DEFAULT \x27\x27,\n\tCONSTRAINT "owner_email_key" UNIQUE ("email")/;
'
derive "$WORK/fk_edit.sql" "$WORK/fk.sql" '
  s/(REFERENCES "owner"\("id"\)\n\tON DELETE) RESTRICT/$1 CASCADE/;
  $_ .= "ALTER TABLE \"pet\" ADD CONSTRAINT \"pet_owner_id_deferred_fk\" FOREIGN KEY (\"owner_id\") REFERENCES \"owner\"(\"id\") DEFERRABLE INITIALLY DEFERRED;\n";
'
run_step "03 edit a key and a table" "$(cat <<'EOS'
ALTER TABLE public.owner ADD COLUMN email character varying(120);
ALTER TABLE public.owner ALTER COLUMN email SET DEFAULT '';
CREATE UNIQUE INDEX ASYNC owner_email_key ON public.owner (email);
ALTER TABLE public.owner ADD CONSTRAINT owner_email_key UNIQUE USING INDEX owner_email_key;
ALTER TABLE public.pet DROP CONSTRAINT pet_owner_id_owner_id_fk;
ALTER TABLE ONLY public.pet ADD CONSTRAINT pet_owner_id_owner_id_fk FOREIGN KEY (owner_id) REFERENCES owner (id) ON UPDATE RESTRICT ON DELETE CASCADE NOT VALID;
ALTER TABLE ASYNC public.pet VALIDATE CONSTRAINT pet_owner_id_owner_id_fk;
ALTER TABLE ONLY public.pet ADD CONSTRAINT pet_owner_id_deferred_fk FOREIGN KEY (owner_id) REFERENCES owner (id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE ASYNC public.pet VALIDATE CONSTRAINT pet_owner_id_deferred_fk;
EOS
)" "$WORK/tables_edit.sql" "$WORK/fk_edit.sql" || true

# --- Step 4: make the deferrable key immediate in place ---
derive "$WORK/fk_immediate.sql" "$WORK/fk_edit.sql" 's/DEFERRABLE INITIALLY DEFERRED/DEFERRABLE INITIALLY IMMEDIATE/'
run_step "04 change a key's deferral" \
  "ALTER TABLE public.pet ALTER CONSTRAINT pet_owner_id_deferred_fk DEFERRABLE;" \
  "$WORK/tables_edit.sql" "$WORK/fk_immediate.sql" || true

# --- Step 5: drop a column with its unique constraint, a key, and a table
#     that holds keys ---
derive "$WORK/tables_drop.sql" "$WORK/tables_edit.sql" '
  s/,\n\t"email" varchar\(120\) DEFAULT \x27\x27,\n\tCONSTRAINT "owner_email_key" UNIQUE \("email"\)//;
  s/CREATE TABLE "specialty_to_vet" \(.*?\n\);\n//s;
'
derive "$WORK/fk_drop.sql" "$WORK/fk_immediate.sql" '
  s/ALTER TABLE "specialty_to_vet"[^;]*;\n(--> statement-breakpoint\n)?//g;
  s/ALTER TABLE "pet" ADD CONSTRAINT "pet_owner_id_deferred_fk"[^;]*;\n//;
'
run_step "05 drop objects" "$(cat <<'EOS'
ALTER TABLE public.pet DROP CONSTRAINT pet_owner_id_deferred_fk;
ALTER TABLE public.owner DROP CONSTRAINT owner_email_key;
ALTER TABLE public.owner DROP COLUMN email;
DROP TABLE public.specialty_to_vet;
EOS
)" "$WORK/tables_drop.sql" "$WORK/fk_drop.sql" || true

# --- Step 6: drop everything ---
run_step "06 drop everything" "$(cat <<'EOS'
DROP TABLE public.pet;
DROP TABLE public.owner;
EOS
)" "$WORK/empty.sql" || true

summary
