# Loading the real-world sample schemas and checking that pista round-trips
# them. Included by the Makefile, which owns PGHOST/PGUSER/PGPORT and SHELL.
# See sample-db-test.md.

# Single source of the real-world sample schemas, consumed by both `schema`
# (loads them all into one database) and `test-samples` (loads each into an
# isolated database and checks pista round-trips it). One record per line:
#   name | loader-target | VAR=value ... | schemas for pista -n (blank = public)
#     | extra pista plan flags (blank for almost every sample)
#
# Every GitHub source is pinned to a commit, not a branch, so that an upstream
# schema change cannot turn this repository's CI red on its own. To move a
# sample to a newer upstream schema, resolve the branch and replace the SHA:
#   git ls-remote https://github.com/<owner>/<repo> <branch>
# then run `make test-samples` to confirm the new schema still round-trips.
# Each SHA is the tip of the upstream default branch as of the pin, except
# synapse (develop), rt (stable), and znuny (dev), which ship their schema
# elsewhere.
define SAMPLES
chinook|sample-db|SQL_FILE=chinook.sql|
dvdrental|sample-db|SQL_FILE=dvdrental.sql|
happiness_index|sample-db|SQL_FILE=happiness_index.sql|
lego|sample-db|SQL_FILE=lego.sql|
netflix|sample-db|SQL_FILE=netflix.sql|
pagila|sample-db|SQL_FILE=pagila.sql|
periodic_table|sample-db|SQL_FILE=periodic_table.sql|
titanic|sample-db|SQL_FILE=titanic.sql|
world|sample-db-tar|TAR_URL=https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/world/world-1.0/world-1.0.tar.gz TAR_SQL_PATH=dbsamples-0.1/world/world.sql|
usda|sample-db-tar|TAR_URL=https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/usda/usda-r18-1.0/usda-r18-1.0.tar.gz TAR_SQL_PATH=usda-r18-1.0/usda.sql|
dellstore2|sample-db-tar|TAR_URL=https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/dellstore2/dellstore2-normal-1.0/dellstore2-normal-1.0.tar.gz TAR_SQL_PATH=dellstore2-normal-1.0/dellstore2-normal-1.0.sql|
french_towns|sample-db-tar|TAR_URL=https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/french-towns-communes-francais/french-towns-communes-francaises-1.0/french-towns-communes-francaises-1.0.tar.gz TAR_SQL_PATH=french-towns-communes-francaises.sql|
iso_3166|sample-db-tar|TAR_URL=https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/iso-3166/iso-3166-1.0/iso-3166-1.0.tar.gz TAR_SQL_PATH=iso-3166/iso-3166.sql|
northwind|sample-db-url|URL=https://raw.githubusercontent.com/pthom/northwind_psql/cd0ef28d66369fbe177778e604e4be0f153c9e5c/northwind.sql|
employees|sample-db-url|URL=https://raw.githubusercontent.com/h8/employees-database/2d6007094f93bd887d6e2b8a409967efd75fc587/employees_schema.sql|employees
mimiciv|sample-db-mimiciv||mimiciv_hosp,mimiciv_icu
mediawiki|sample-db-url-schema|URL=https://raw.githubusercontent.com/wikimedia/mediawiki/2f1ddc8e7fc4e3b1678178f63c6c06895390d218/sql/postgres/tables-generated.sql SCHEMA=mediawiki|mediawiki
synapse|sample-db-url-schema|URL=https://raw.githubusercontent.com/element-hq/synapse/415a869f1f53bd4bf22a69e74081875dc9c17735/synapse/storage/schema/main/full_schemas/72/full.sql.postgres SCHEMA=synapse|synapse
temporal|sample-db-url-schema|URL=https://raw.githubusercontent.com/temporalio/temporal/a669256c743238702f29900100ce441f52a1d49f/schema/postgresql/v12/temporal/schema.sql SCHEMA=temporal|temporal
icingadb|sample-db-url-schema|URL=https://raw.githubusercontent.com/Icinga/icingadb/1f63fe3db7070b718a761050a316bfde60013401/schema/pgsql/schema.sql SCHEMA=icingadb|icingadb
rt|sample-db-url-schema|URL=https://raw.githubusercontent.com/bestpractical/rt/9ffb1ed910b19eefd459ef5480369bf6a2a9038a/etc/schema.Pg SCHEMA=rt|rt
sourcegraph|sample-db-url-schema|URL=https://raw.githubusercontent.com/sourcegraph/sourcegraph-public-snapshot/c864f15af264f0f456a6d8a83290b5c940715349/migrations/frontend/squashed.sql SCHEMA=sourcegraph|sourcegraph
imdb|sample-db-imdb||
adventureworks|sample-db-adventureworks||person,humanresources,production,purchasing,sales
clubdata|sample-db-clubdata|URL=https://pgexercises.com/dbfiles/clubdata.sql|cd
demodb|sample-db-demodb|URL=https://raw.githubusercontent.com/postgrespro/demodb/bf7a1c1972d2f89dc9de21f19d7dd3aa650e8647/tables.sql|bookings
musicbrainz|sample-db-musicbrainz||musicbrainz
znuny|sample-db-znuny||znuny
hive|sample-db-hive|URL=https://raw.githubusercontent.com/apache/hive/d98bfeda81c23007866bd5bf7ee970fa017689ed/standalone-metastore/metastore-server/src/main/sql/postgres/hive-schema-4.2.0.postgres.sql SCHEMA=hive|hive
ranger|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/ranger/0249cc1d8255c0370322508c7e1d30250e152340/security-admin/db/postgres/optimized/current/ranger_core_db_postgres.sql SCHEMA=ranger CLIENT_MIN_MESSAGES=error|ranger
ambari|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/ambari/0347dc4503c09b0593d9cb12815e2f9784b6a86a/ambari-server/src/main/resources/Ambari-DDL-Postgres-CREATE.sql SCHEMA=ambari|ambari
ovirt|sample-db-url-schema|URL=https://raw.githubusercontent.com/oVirt/ovirt-engine/be1f6647db1ebbf39186bbe4dd6b1a777376815b/packaging/dbscripts/create_tables.sql SCHEMA=ovirt|ovirt
gitlab|sample-db-url-schema|URL=https://raw.githubusercontent.com/gitlabhq/gitlabhq/35e789d8f1173a11a7724ae360a80d1f19ec92dc/db/structure.sql SCHEMA=gitlab|gitlab,gitlab_partitions_static,gitlab_partitions_dynamic|--assume-validated
ledgersmb|sample-db-url-schema|URL=https://raw.githubusercontent.com/ledgersmb/LedgerSMB/34a05e7b58c161e008561e36a74dbede860fe4d9/sql/Pg-database.sql SCHEMA=ledgersmb|ledgersmb
koji|sample-db-url-schema|URL=https://raw.githubusercontent.com/koji-project/koji/c3239d46c1abdcbda509739eb0182031b71d9f01/schemas/schema.sql SCHEMA=koji|koji
kea|sample-db-url-schema|URL=https://raw.githubusercontent.com/isc-projects/kea/82440eddc70089a55892a42ca96b78a92eb3e484/src/share/database/scripts/pgsql/dhcpdb_create.pgsql SCHEMA=kea CLIENT_MIN_MESSAGES=warning|kea
dolphinscheduler|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/dolphinscheduler/51057477b815eef59c68f1b76563567814c72a93/dolphinscheduler-dao/src/main/resources/sql/dolphinscheduler_postgresql.sql SCHEMA=dolphinscheduler CLIENT_MIN_MESSAGES=warning|dolphinscheduler
camunda|sample-db-camunda||camunda
wso2apim|sample-db-url-schema|URL=https://raw.githubusercontent.com/wso2/carbon-apimgt/5dd6e3084a35228b7542c4ff6a9071af1c7476fa/features/apimgt/org.wso2.carbon.apimgt.core.feature/src/main/resources/sql/postgresql.sql SCHEMA=wso2apim CLIENT_MIN_MESSAGES=warning|wso2apim
discourse|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/discourse/discourse/f3c568cfd26a427e9cae32063732a56bc7d334b9/db/structure.sql SCHEMA=discourse|discourse
icinga_director|sample-db-url-schema|URL=https://raw.githubusercontent.com/Icinga/icingaweb2-module-director/b2e4a4e4180b0a160461a773ca8b8b5874e0fba7/schema/pgsql.sql SCHEMA=icinga_director|icinga_director
flowable|sample-db-url-schema|URL=https://raw.githubusercontent.com/flowable/flowable-engine/53e93b6681e86dccea720efaa3c0fc2a96f57366/distro/sql/create/all/flowable.postgres.all.create.sql SCHEMA=flowable|flowable
ejabberd|sample-db-url-schema|URL=https://raw.githubusercontent.com/processone/ejabberd/f42a49c1e83ad2399743dd46c6cf1e43d39d303b/sql/pg.new.sql SCHEMA=ejabberd|ejabberd
guacamole|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/guacamole-client/5be18be1eeadc4cc544c737c54bd761261d2ad65/extensions/guacamole-auth-jdbc/modules/guacamole-auth-jdbc-postgresql/schema/001-create-schema.sql SCHEMA=guacamole|guacamole
dotcms|sample-db-url-schema|URL=https://raw.githubusercontent.com/dotCMS/core/b0095c0c3920e236efcc16ceb136cd5dd88804b7/dotCMS/src/main/resources/postgres.sql SCHEMA=dotcms|dotcms
osm|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/openstreetmap/openstreetmap-website/9da0fa5ecbff8adc5e6e91c7cf22546755a91f77/db/structure.sql SCHEMA=osm|osm
chado|sample-db-chado|URL=https://raw.githubusercontent.com/GMOD/Chado/31c2407716e3d0fe4e837b6effad0a510af22238/chado/schemas/1.31/default_schema.sql|chado,genetic_code,so,frange
endef

# Every loader pipes its schema into this psql. ON_ERROR_STOP makes a failing
# statement stop the load and fail the target, which the runner reports as
# FAIL (load). Without it psql prints the error, carries on, and exits 0, and
# the check runs against a schema quietly missing whatever the statement was
# going to create -- which dump and plan then agree about, so the sample passes.
PSQL = psql -v ON_ERROR_STOP=1

# Print the sample manifest, one record per line, for shell consumers.
.PHONY: print-samples
print-samples:
	$(info $(SAMPLES))@:

.PHONY: schema
schema: clean-schema
	@$(MAKE) -s print-samples | while IFS='|' read -r name target args schemas; do \
	  [ -n "$$name" ] || continue; \
	  echo "==> $$name"; \
	  $(MAKE) $$target $$args; \
	done

# The Neon sample collection. dvdrental was dumped by a pg_dump new enough to
# set transaction_timeout in its preamble, a parameter 15 and 16 do not have,
# so the line is dropped rather than let it stop the load on those versions. It
# sets nothing the schema depends on.
.PHONY: sample-db
sample-db:
	curl -sSf --retry 3 --retry-delay 2 https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/b54cb67534bf20775803b181b7a1c6f573422161/$(SQL_FILE) \
	  | sed '/^SET transaction_timeout = /d' \
	  | $(PSQL)

.PHONY: sample-db-tar
sample-db-tar:
	curl -sSfL --retry 3 --retry-delay 2 $(TAR_URL) | tar xzO $(TAR_SQL_PATH) | $(PSQL)

.PHONY: sample-db-url
sample-db-url:
	curl -sSfL --retry 3 --retry-delay 2 $(URL) | $(PSQL)

# MIMIC-IV (MIT-LCP/mimic-code, MIT). The schema ships as three files, so
# concatenate them in dependency order: create.sql (which creates the mimiciv_*
# schemas and the tables), then the primary and foreign keys, then the indexes.
# constraint.sql qualifies every table it touches and index.sql sets its own
# search_path, so no search_path is needed here. Both files drop what they are
# about to create with IF EXISTS, which floods a fresh database with NOTICEs, so
# quiet those.
MIMICIV_SQL_FILES = create.sql constraint.sql index.sql

.PHONY: sample-db-mimiciv
sample-db-mimiciv:
	for f in $(MIMICIV_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/MIT-LCP/mimic-code/3a914fce11e05888a4b659c7788e207bc34d1728/mimic-iv/buildmimic/postgres/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c client_min_messages=warning' $(PSQL)

# A plain SQL URL loaded into a schema of its own. These dumps name no schema at
# all, so search_path decides where they land, and upstream expects them in the
# search path's first schema. Loading them into `public` would collide with the
# other public samples when `make schema` puts everything in one database
# (mediawiki and pagila both define `actor` and `category`, for one), so each
# gets its own schema instead.
#
# CLIENT_MIN_MESSAGES defaults to notice, the server default; a sample whose
# dump is noisy on a fresh database can raise it from its SAMPLES record. kea,
# dolphinscheduler, and wso2apim raise it to warning, since each drops what it
# is about to create with IF EXISTS, and ranger to error, since it does the same
# and commits outside a transaction, which adds a warning per statement.
CLIENT_MIN_MESSAGES ?= notice

.PHONY: sample-db-url-schema
sample-db-url-schema:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) | PGOPTIONS='-c search_path=$(SCHEMA) -c client_min_messages=$(CLIENT_MIN_MESSAGES)' $(PSQL)

# Hive metastore (apache/hive, Apache-2.0). Like the sample-db-url-schema
# dumps this one belongs in a schema of its own, but it is a pg_dump-style
# file that sets `search_path` to public itself, which overrides anything
# PGOPTIONS passes in. So rewrite that one line instead of setting the
# search_path from outside.
.PHONY: sample-db-hive
sample-db-hive:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^SET search_path = / { print "SET search_path = $(SCHEMA), pg_catalog;"; next } { print }' \
	  | $(PSQL)

# GMOD Chado (GMOD/Chado), the schema behind FlyBase and the other model
# organism databases. It names no schema for the objects it creates, so
# search_path decides where they land, but four times partway through the file
# it sets search_path itself with `public` in it, which overrides anything
# PGOPTIONS passes in. So rewrite `public` in those four lines to `chado`, as
# sample-db-hive rewrites its one line. The lines that name genetic_code, so,
# and frange keep them: the file creates those three schemas itself and fills
# them, so the sample is checked with all four. `so` is where its 1,832
# Sequence Ontology views land, more views than every other sample together.
#
# The same sed qualifies the create_point calls in the bodies of boxrange and
# boxquery. PostgreSQL 17 runs CREATE INDEX with search_path set to
# pg_catalog, pg_temp, and the three gist indexes over boxrange inline it,
# which re-resolves the unqualified create_point under that search_path and
# does not find it. Qualifying the calls keeps all three indexes on 17 and 18
# and changes nothing on 15 and 16. The CREATE line for create_point itself is
# left alone, so search_path still decides where the function lands.
#
# Nine of the file's SQL functions are written against the `@` box operator,
# which PostgreSQL 14 removed, so they error out on every version in the CI
# matrix. The awk drops them: it buffers each CREATE OR REPLACE FUNCTION
# through the LANGUAGE line that ends it, and skips the six whose body uses the
# operator plus the three that call the three-argument groupoverlaps, which is
# one of the six. Nothing else is dropped, so the 94 functions that do load are
# part of the round trip like every other object.
.PHONY: sample-db-chado
sample-db-chado:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS chado'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | sed -E 's/^(SET search_path ?=[^;]*)public/\1chado/; /^CREATE/! s/create_point\(/chado.create_point(/g' \
	  | awk '/^CREATE OR REPLACE FUNCTION/ && !hold { buf = $$0; hold = 1; next } \
	         hold { buf = buf ORS $$0; \
	                if (tolower($$0) ~ /language/) { hold = 0; \
	                  if (buf !~ /@ boxrange|FROM groupoverlaps\(\$$1,\$$2,\$$3\)/) print buf } \
	                next } \
	         { print }' \
	  | PGOPTIONS='-c search_path=chado -c client_min_messages=warning' $(PSQL)

# Join Order Benchmark (gregrahn/join-order-benchmark), the IMDB schema used by
# the JOB query set. The tables and their indexes ship as two separate files, so
# concatenate them: schema.sql first, then the foreign-key indexes.
.PHONY: sample-db-imdb
sample-db-imdb:
	for f in schema.sql fkindexes.sql; do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/gregrahn/join-order-benchmark/a39603662e023e449cb2121997a5034df9e02ebf/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

# AdventureWorks (lorint/AdventureWorks-for-Postgres, MIT). Schema-only load:
# strip \copy lines (data lives in CSVs we don't fetch) and the inline
# Production.ProductReview INSERT (FK target rows aren't loaded).
.PHONY: sample-db-adventureworks
sample-db-adventureworks:
	curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/lorint/AdventureWorks-for-Postgres/b474991f0df1c4bf55ca4735eb0254ca0709eed2/install.sql \
	  | awk '/^\\copy/ { next } /^INSERT INTO Production.ProductReview/ { skip=1 } skip { if (/\);[[:space:]]*$$/) skip=0; next } { print }' \
	  | $(PSQL)

# PostgreSQL Exercises club data (pgexercises.com). The dump creates its own
# database and reconnects to it, which we cannot do mid-pipe; strip those two
# lines and load the rest into the current database. The remaining SQL creates
# the `cd` schema and sets search_path itself.
.PHONY: sample-db-clubdata
sample-db-clubdata:
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^CREATE DATABASE exercises;/ { next } /^\\c exercises/ { next } { print }' \
	  | $(PSQL)

# Postgres Pro demo database (postgrespro/demodb, PostgreSQL License). The repo
# ships a schema-generation script, not a plain dump: tables.sql defines the
# `gen` and `bookings` schemas and \copy-loads reference data from .dat files we
# don't fetch. We only need the schema, so strip the \copy lines (their data is
# irrelevant to a round-trip check) and enable btree_gist first, which the
# bookings.routes exclusion constraint requires.
.PHONY: sample-db-demodb
sample-db-demodb:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS btree_gist'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^[[:space:]]*\\copy/ { next } { print }' \
	  | $(PSQL)

# MusicBrainz (metabrainz/musicbrainz-server, GPL-2.0). The schema ships as one
# file per object kind and none of them create the schema, so create
# `musicbrainz` up front, point search_path at it, and concatenate the files in
# dependency order: extensions and the ICU collation first (tables reference
# both), then the text search configuration and types, the tables, the functions
# (CHECK constraints and views call them), and finally keys, indexes,
# constraints, and views.
MUSICBRAINZ_SQL_FILES = \
	Extensions.sql \
	CreateCollations.sql \
	CreateSearchConfiguration.sql \
	CreateTypes.sql \
	CreateTables.sql \
	CreateFunctions.sql \
	CreatePrimaryKeys.sql \
	CreateFKConstraints.sql \
	CreateIndexes.sql \
	CreateConstraints.sql \
	CreateViews.sql

.PHONY: sample-db-musicbrainz
sample-db-musicbrainz:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS musicbrainz'
	for f in $(MUSICBRAINZ_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/metabrainz/musicbrainz-server/424c5fad44da2b3ad55d08286fe8ad07c11ec471/admin/sql/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c search_path=musicbrainz,public -c client_min_messages=warning' $(PSQL)

# Znuny (znuny/Znuny, GPL-3.0). The schema ships as two files, so concatenate
# them: schema.postgresql.sql (tables and indexes) and then
# schema-post.postgresql.sql (the foreign keys, which need every table to
# exist). Neither names a schema, so search_path decides where they land and
# `public` would collide with the other public samples; create `znuny` up front
# and point search_path at it, as sample-db-url-schema does for the one-file
# dumps.
ZNUNY_SQL_FILES = schema.postgresql.sql schema-post.postgresql.sql

.PHONY: sample-db-znuny
sample-db-znuny:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS znuny'
	for f in $(ZNUNY_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/znuny/Znuny/0b894348ebc458621545ccdab5f3d24d1b396a70/scripts/database/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c search_path=znuny' $(PSQL)

# Camunda 7 (camunda/camunda-bpm-platform, Apache-2.0). The schema ships as one
# file per engine component and none of them create a schema, so create
# `camunda` up front and concatenate the files in dependency order: the process
# engine first, then history, identity, and the case and decision engines with
# their history, each of which keys back into the tables the engine file
# creates.
CAMUNDA_SQL_FILES = \
	activiti.postgres.create.engine.sql \
	activiti.postgres.create.history.sql \
	activiti.postgres.create.identity.sql \
	activiti.postgres.create.case.engine.sql \
	activiti.postgres.create.case.history.sql \
	activiti.postgres.create.decision.engine.sql \
	activiti.postgres.create.decision.history.sql

.PHONY: sample-db-camunda
sample-db-camunda:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS camunda'
	for f in $(CAMUNDA_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/camunda/camunda-bpm-platform/ee4826e5e76c2348a1510ef46a2f4ccd3b080e48/engine/src/main/resources/org/camunda/bpm/engine/db/create/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c search_path=camunda' $(PSQL)

# A pg_dump-style dump loaded into a schema of its own. discourse and osm ship
# their schema as Rails' db/structure.sql, which belongs in a schema of its own
# like the sample-db-url-schema dumps but is pg_dump output: it empties
# search_path and qualifies every object it creates with `public`, so neither
# PGOPTIONS nor the hive-style search_path rewrite reaches it. Drop the line
# that empties search_path and strip the `public.` qualifier instead, which
# leaves every name unqualified and lets search_path place it. The CREATE
# EXTENSION lines say `WITH SCHEMA public` without a dot, so they are untouched
# and the types they own (discourse's halfvec, hstore, and trgm operator
# classes, osm's geometry) still resolve from `public`, which stays second in
# the search path. Column names that merely start with "public" (`public`,
# `public_version`) are not qualifiers and are left alone. The tail of the file
# is Rails' own `SET search_path TO "$user", public` followed by the migration
# versions it inserts into schema_migrations, which is data and would land in
# the wrong schema anyway, so everything from that line on is dropped.
#
# discourse needs pgvector and osm needs PostGIS, neither of which the official
# postgres image ships; compose.yaml and the samples CI job install both. See
# sample-db-test.md.
.PHONY: sample-db-pgdump-schema
sample-db-pgdump-schema:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | sed -E "/^SELECT pg_catalog.set_config\('search_path', '', false\);\$$/d; /^SET search_path TO /,\$$d; s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g" \
	  | PGOPTIONS='-c search_path=$(SCHEMA),public' $(PSQL)

.PHONY: test-samples
test-samples:
	bash test/samples/run.sh

# Wipe every user schema. Used by `schema` and `demo` to start from an empty
# database, by test-samples once before its first sample, and by `test` and
# `test-scenario`, which reset only `public` on their own.
#
# The tables go first, a batch at a time, and only then the schemas. A single
# DROP SCHEMA ... CASCADE takes locks on every object it reaches, and the
# larger samples do not fit: gitlab owns 1,400 tables and their indexes, which
# runs the server out of lock table space at the default
# max_locks_per_transaction. Each batch is a statement of its own, so its locks
# are released before the next one starts, and the schemas are empty by the
# time they are dropped.
DROP_TABLE_BATCH = 50

.PHONY: clean-schema
clean-schema:
	while :; do \
	  batch=$$(psql -X -q -At -v ON_ERROR_STOP=1 -c "SELECT string_agg(format('%I.%I', schemaname, tablename), ', ') FROM (SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT LIKE 'pg_%' AND schemaname <> 'information_schema' LIMIT $(DROP_TABLE_BATCH)) t") || exit 1; \
	  [ -n "$$batch" ] || break; \
	  psql -X -q -v ON_ERROR_STOP=1 -c "SET client_min_messages TO warning; DROP TABLE IF EXISTS $$batch CASCADE" || exit 1; \
	done
	psql -X -q -At -v ON_ERROR_STOP=1 -c "SELECT quote_ident(nspname) FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'" \
	  | while read -r s; do \
	      psql -X -q -v ON_ERROR_STOP=1 -c "SET client_min_messages TO warning; DROP SCHEMA IF EXISTS $$s CASCADE" || exit 1; \
	    done
	psql -X -q -v ON_ERROR_STOP=1 -c 'CREATE SCHEMA public'

# Reset `public` between samples. The samples that load into `public` are the
# only ones that can collide with each other; every other sample owns a schema
# of its own and is checked with `pista -n`, so what it leaves behind is
# invisible to the next sample and not worth dropping. Not dropping those
# schemas is also what keeps the big samples cheap: cascading through gitlab's
# 1,400 tables does not fit in one statement's locks, and musicbrainz's ~1,000
# objects are close behind.
#
# Extensions are the exception, because they are visible to the next sample
# whichever schema they sit in: a dump that says CREATE EXTENSION IF NOT EXISTS
# does nothing when the extension already exists in some other sample's schema,
# and then its types and operator classes do not resolve. icingadb and
# sourcegraph both install citext, and sourcegraph and gitlab both install
# pg_trgm. So drop the extensions too, before the next sample runs.
.PHONY: reset-db
reset-db:
	psql -X -q -v ON_ERROR_STOP=1 -c 'SET client_min_messages TO warning; DROP SCHEMA IF EXISTS public CASCADE'
	psql -X -q -At -v ON_ERROR_STOP=1 -c "SELECT quote_ident(extname) FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE n.nspname <> 'pg_catalog'" \
	  | while read -r e; do \
	      psql -X -q -v ON_ERROR_STOP=1 -c "SET client_min_messages TO warning; DROP EXTENSION IF EXISTS $$e CASCADE" || exit 1; \
	    done
	psql -X -q -v ON_ERROR_STOP=1 -c 'CREATE SCHEMA public'
