# Loading the real-world sample schemas and checking that pista round-trips
# them. Included by the Makefile, which owns PGHOST/PGUSER/PGPORT and SHELL.
# See SAMPLE-DB-TESTS.md.

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
#
# boundary comes first because its migrations assume the database is Boundary's
# alone; sample-db-boundary says why. Every loader runs in list order after one
# clean-schema, in both `schema` and `test-samples`, so first is the one place
# no other sample's schema is there yet.
define SAMPLES
boundary|sample-db-boundary||boundary
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
demodb|sample-db-demodb|URL=https://raw.githubusercontent.com/postgrespro/demodb/bf7a1c1972d2f89dc9de21f19d7dd3aa650e8647/tables.sql CLIENT_MIN_MESSAGES=warning|bookings
musicbrainz|sample-db-musicbrainz||musicbrainz
znuny|sample-db-znuny||znuny
hive|sample-db-hive|URL=https://raw.githubusercontent.com/apache/hive/d98bfeda81c23007866bd5bf7ee970fa017689ed/standalone-metastore/metastore-server/src/main/sql/postgres/hive-schema-4.2.0.postgres.sql SCHEMA=hive|hive
ranger|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/ranger/0249cc1d8255c0370322508c7e1d30250e152340/security-admin/db/postgres/optimized/current/ranger_core_db_postgres.sql SCHEMA=ranger CLIENT_MIN_MESSAGES=error|ranger
ambari|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/ambari/0347dc4503c09b0593d9cb12815e2f9784b6a86a/ambari-server/src/main/resources/Ambari-DDL-Postgres-CREATE.sql SCHEMA=ambari|ambari
ovirt|sample-db-url-schema|URL=https://raw.githubusercontent.com/oVirt/ovirt-engine/be1f6647db1ebbf39186bbe4dd6b1a777376815b/packaging/dbscripts/create_tables.sql SCHEMA=ovirt|ovirt
gitlab|sample-db-url-schema|URL=https://raw.githubusercontent.com/gitlabhq/gitlabhq/35e789d8f1173a11a7724ae360a80d1f19ec92dc/db/structure.sql SCHEMA=gitlab|gitlab,gitlab_partitions_static,gitlab_partitions_dynamic
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
wso2is|sample-db-url-schema|URL=https://raw.githubusercontent.com/wso2/carbon-identity-framework/a956a69ffdd1bcf916132682b12b5f319102f69e/features/identity-core/org.wso2.carbon.identity.core.server.feature/resources/dbscripts/postgresql.sql SCHEMA=wso2is CLIENT_MIN_MESSAGES=warning|wso2is
nightingale|sample-db-url-schema|URL=https://raw.githubusercontent.com/ccfos/nightingale/8362cbe18f98b3c06af176d71b13994673912c46/docker/compose-postgres/initsql_for_postgres/a-n9e-for-Postgres.sql SCHEMA=nightingale|nightingale
danbooru|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/danbooru/danbooru/f8de3ba286db1f9a1f3efbbf495bcbc1001e7b9b/db/structure.sql SCHEMA=danbooru|danbooru
openolat|sample-db-url-schema|URL=https://raw.githubusercontent.com/OpenOLAT/OpenOLAT/8d07a02fe4dafb8c82179f3efa77e1cb985fdc7e/src/main/resources/database/postgresql/setupDatabase.sql SCHEMA=openolat|openolat
inaturalist|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/inaturalist/inaturalist/e52c649d2f0e8e260c2dce6fcf6448d971100415/db/structure.sql SCHEMA=inaturalist|inaturalist
joomla|sample-db-joomla||joomla
harbor|sample-db-harbor||harbor
bigbluebutton|sample-db-bigbluebutton|URL=https://raw.githubusercontent.com/bigbluebutton/bigbluebutton/4c3a477fe3e34a7da6854c491be2c8d02e83c083/bbb-graphql-server/bbb_schema.sql SCHEMA=bigbluebutton|bigbluebutton
listmonk|sample-db-url-schema|URL=https://raw.githubusercontent.com/knadh/listmonk/594b74056dd8a0d3a7621a32898ee38bfbe10e96/schema.sql SCHEMA=listmonk CLIENT_MIN_MESSAGES=warning|listmonk
dhis2|sample-db-dhis2|URL=https://raw.githubusercontent.com/dhis2/dhis2-core/5d2dbdef40e91c1c613fc50a8132158dd5683b7f/dhis-2/dhis-support/dhis-support-db-migration/src/main/resources/org/hisp/dhis/db/base/dhis2_base_schema.sql SCHEMA=dhis2|dhis2
coder|sample-db-url-schema|URL=https://raw.githubusercontent.com/coder/coder/263f2c207eca19c2e42a71f439782c15ebeb8f07/coderd/database/dump.sql SCHEMA=coder CHECK_FUNCTION_BODIES=off|coder
hatchet|sample-db-hatchet||hatchet
thingsboard|sample-db-thingsboard||thingsboard
glific|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/glific/glific/2c8141103b58aa6144240e79c253f119bfbcf98c/priv/repo/structure.sql SCHEMA=glific|glific
lago|sample-db-lago|URL=https://raw.githubusercontent.com/getlago/lago-api/78f709bfb31c43834ab6ea18d5f771e3a94e313d/db/structure.sql|lago
calcom|sample-db-prisma|REPO=calcom/cal.diy SHA=6bc45298226f96ff79e0c070c8b2ce39727e8477 DIR=packages/prisma/migrations SCHEMA=calcom|calcom
triggerdev|sample-db-prisma|REPO=triggerdotdev/trigger.dev SHA=2d03fee2e3ff368128302ed4c783ba4e32d1cb00 DIR=internal-packages/database/prisma/migrations SCHEMA=triggerdev|triggerdev
mattermost|sample-db-mattermost||mattermost
lemmy|sample-db-lemmy||lemmy,r,utils
windmill|sample-db-windmill||windmill
plausible|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/plausible/analytics/30abd272b5114ba1f3c2c8bd146b86a4d2b7b984/priv/repo/structure.sql SCHEMA=plausible|plausible
feedbin|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/feedbin/feedbin/eabfb10cc5975ebd755781ce33c0043feee6af31/db/structure.sql SCHEMA=feedbin|feedbin
citizenlab|sample-db-citizenlab|URL=https://raw.githubusercontent.com/CitizenLabDotCo/citizenlab/0501175990e1ecec84f4a541c1a8b7a2b4755da1/back/db/structure.sql|citizenlab
dokploy|sample-db-dokploy||dokploy
hyperswitch|sample-db-hyperswitch||hyperswitch
documenso|sample-db-prisma|REPO=documenso/documenso SHA=e658cc581878f52c03b3e6a8f7ffd613aead4e69 DIR=packages/prisma/migrations SCHEMA=documenso|documenso
langfuse|sample-db-prisma|REPO=langfuse/langfuse SHA=330bb86bcc332a76d4b24569fa4e1c1086d651d1 DIR=packages/shared/prisma/migrations SCHEMA=langfuse CLIENT_MIN_MESSAGES=warning|langfuse
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
# dump is noisy on a fresh database can raise it from its SAMPLES record.
# demodb, kea, dolphinscheduler, wso2apim, wso2is, and listmonk raise it to
# warning, since each drops what it is about to create with IF EXISTS, wso2is
# also because five of its index names are over 63 characters and the server
# says so as it truncates them, and ranger to error, since it drops the same way
# and commits outside a transaction, which adds a warning per statement. It
# reaches sample-db-prisma as well, where langfuse raises it for the same two
# reasons: one of its index names runs past 63 characters, and one migration
# turns a unique index into a primary key with ADD CONSTRAINT ... USING INDEX,
# which the server says it is renaming the index for.
CLIENT_MIN_MESSAGES ?= notice

# CHECK_FUNCTION_BODIES defaults to on, the server default. coder turns it off:
# its dump is pg_dump output with the preamble stripped, and that preamble is
# where pg_dump turns it off itself. Without it a plpgsql function declaring a
# variable of a table's row type stops the load, since the table comes later in
# the file.
CHECK_FUNCTION_BODIES ?= on

.PHONY: sample-db-url-schema
sample-db-url-schema:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) | PGOPTIONS='-c search_path=$(SCHEMA) -c client_min_messages=$(CLIENT_MIN_MESSAGES) -c check_function_bodies=$(CHECK_FUNCTION_BODIES)' $(PSQL)

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
# bookings.routes exclusion constraint requires. It drops both schemas with
# IF EXISTS before creating them, which says so on a fresh database, so
# client_min_messages is raised to warning for the load.
.PHONY: sample-db-demodb
sample-db-demodb:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS btree_gist'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^[[:space:]]*\\copy/ { next } { print }' \
	  | PGOPTIONS='-c client_min_messages=$(CLIENT_MIN_MESSAGES)' $(PSQL)

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

# A pg_dump-style dump loaded into a schema of its own. discourse, osm,
# danbooru, inaturalist, and feedbin ship their schema as Rails'
# db/structure.sql, which
# belongs in a schema of its own like the sample-db-url-schema dumps but is
# pg_dump output: it empties search_path and qualifies every object it creates
# with `public`, so neither PGOPTIONS nor the hive-style search_path rewrite
# reaches it. Drop the line that empties search_path and strip the `public.`
# qualifier instead, which leaves every name unqualified and lets search_path
# place it. The CREATE
# EXTENSION lines say `WITH SCHEMA public` without a dot, so they are untouched
# and the types and operator classes they own (discourse's halfvec, hstore, and
# trgm operator classes, osm's and inaturalist's geometry, danbooru's trgm and
# btree_gin operator classes) still resolve from `public`, which stays second in
# the search path.
# Column names that merely start with "public" (`public`, `public_version`) are
# not qualifiers and are left alone. The tail of the file is Rails' own
# `SET search_path TO "$user", public` followed by the migration versions it
# inserts into schema_migrations, which is data and would land in the wrong
# schema anyway, so everything from that line on is dropped.
#
# glific's and plausible's structure.sql is Ecto's rather than Rails', the same
# pg_dump output without that SET line, so their migration versions stay and,
# with the qualifier stripped, go into their own schema_migrations. They are
# rows, not schema.
#
# discourse needs pgvector and osm and inaturalist need PostGIS, neither of
# which the official postgres image ships; compose.yaml and the samples CI job
# install both. See SAMPLE-DB-TESTS.md. danbooru installs five extensions of
# its own, btree_gin, fuzzystrmatch, pg_trgm, pgcrypto, and pgstattuple,
# inaturalist one, uuid-ossp, which 16 of its columns default through, feedbin
# three, hstore, pg_stat_statements, and uuid-ossp, and plausible one, citext,
# which three of its columns are typed by; those are all contrib and the
# official image already has them.
.PHONY: sample-db-pgdump-schema
sample-db-pgdump-schema:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | sed -E "/^SELECT pg_catalog.set_config\('search_path', '', false\);\$$/d; /^SET search_path TO /,\$$d; s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g" \
	  | PGOPTIONS='-c search_path=$(SCHEMA),public' $(PSQL)

# Joomla CMS (joomla/joomla-cms, GPL-2.0-or-later). The PostgreSQL installer
# ships as three files that must load in that order -- base.sql (the core
# tables), extensions.sql (the bundled extensions and plugins), and
# supports.sql (the smart search tables, which reference extensions.sql's
# content types) -- none of which create a schema or set search_path
# themselves. Every table name carries the literal `#__` prefix Joomla
# replaces at install time; left as `#__` and quoted, it is just an ordinary
# identifier, so nothing needs rewriting.
JOOMLA_SQL_FILES = base.sql extensions.sql supports.sql

.PHONY: sample-db-joomla
sample-db-joomla:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS joomla'
	for f in $(JOOMLA_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/joomla/joomla-cms/b2648c39655a1dc9ceb2791cf9131acc4f98d22e/installation/sql/postgresql/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c search_path=joomla' $(PSQL)

# Harbor (goharbor/harbor, Apache-2.0), the CNCF container registry. Its
# schema ships as one file per release, each an incremental delta meant to be
# replayed by golang-migrate, which tracks what it has already applied in a
# schema_migrations table of its own. One delta ALTERs that table directly to
# add a column a later delta drops again, so a stand-in schema_migrations
# table is created before the deltas run and dropped once they have, since
# golang-migrate's own bookkeeping is not part of Harbor's schema. A few
# deltas also omit their file's closing semicolon, which merges the next
# file's opening statement into it once concatenated, so every file gets one
# appended regardless of whether it already ends in one.
HARBOR_SQL_FILES = 0001_initial_schema.up.sql 0002_1.7.0_schema.up.sql \
	0003_add_replication_op_uuid.up.sql 0004_1.8.0_schema.up.sql \
	0005_1.8.2_schema.up.sql 0010_1.9.0_schema.up.sql 0011_1.9.1_schema.up.sql \
	0012_1.9.4_schema.up.sql 0015_1.10.0_schema.up.sql 0030_2.0.0_schema.up.sql \
	0031_2.0.3_schema.up.sql 0040_2.1.0_schema.up.sql 0041_2.1.4_schema.up.sql \
	0050_2.2.0_schema.up.sql 0051_2.2.1_schema.up.sql 0052_2.2.2_schema.up.sql \
	0053_2.2.3_schema.up.sql 0060_2.3.0_schema.up.sql 0061_2.3.4_schema.up.sql \
	0070_2.4.0_schema.up.sql 0071_2.4.2_schema.up.sql 0080_2.5.0_schema.up.sql \
	0081_2.5.2_schema.up.sql 0082_2.5.3_schema.up.sql 0090_2.6.0_schema.up.sql \
	0091_2.6.2_schema.up.sql 0100_2.7.0_schema.up.sql 0110_2.8.0_schema.up.sql \
	0111_2.8.1_schema.up.sql 0120_2.9.0_schema.up.sql 0130_2.10.0_schema.up.sql \
	0140_2.11.0_schema.up.sql 0150_2.12.0_schema.up.sql 0160_2.13.0_schema.up.sql \
	0170_2.14.0_schema.up.sql 0171_2.14.1_schema.up.sql 0180_2.15.0_schema.up.sql \
	0181_2.15.3_schema.up.sql 0190_2.16.0_schema.up.sql

.PHONY: sample-db-harbor
sample-db-harbor:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS harbor'
	$(PSQL) -c 'SET search_path = harbor; CREATE TABLE schema_migrations (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL DEFAULT false)'
	for f in $(HARBOR_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/goharbor/harbor/fb4e2406747df0b01dec994bf593f49261f6a874/make/migrations/postgresql/$$f || exit 1; \
	  echo ';'; \
	done | PGOPTIONS='-c search_path=harbor -c client_min_messages=warning' $(PSQL)
	$(PSQL) -c 'SET search_path = harbor; DROP TABLE schema_migrations'

# BigBlueButton (bigbluebutton/bigbluebutton, LGPL-3.0). The schema belongs in
# a schema of its own like the sample-db-url-schema dumps, and names none for
# almost everything it creates, but three of its views are qualified with
# `public`. Those three would land outside the sample's schema, and the views
# that select from them would then not resolve, so the qualifier is stripped
# and search_path places them with the rest. The file also installs unaccent,
# which a function behind three of its generated columns calls; it is contrib,
# so the official image already has it.
.PHONY: sample-db-bigbluebutton
sample-db-bigbluebutton:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | sed -E 's/^(CREATE OR REPLACE VIEW )public\./\1/' \
	  | PGOPTIONS='-c search_path=$(SCHEMA)' $(PSQL)

# DHIS2 (dhis2/dhis2-core, BSD-3-Clause). A pg_dump of the base schema Flyway
# starts from, naming no schema and no owner, so it loads like the
# sample-db-url-schema dumps. One of its columns is a PostGIS geometry, though,
# and the dump does not install the extension, so install it first and leave
# `public` in the search path for the type to resolve from. PostGIS is not in
# the official image; compose.yaml and the samples CI job install it for the
# osm and inaturalist samples already.
.PHONY: sample-db-dhis2
sample-db-dhis2:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS postgis'
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | PGOPTIONS='-c search_path=$(SCHEMA),public' $(PSQL)

# HashiCorp Boundary (hashicorp/boundary, BUSL-1.1). The schema ships as
# migrations only, 290 files that Boundary replays in order: the two base files
# first, then one directory per schema version, in numeric order, with the files
# in each in name order. Fetching them one at a time is slow, so the repository
# tarball is fetched once and only the migrations directory is extracted. None
# of the files names a schema, so `boundary` is created up front and search_path
# places everything, the extensions citext, pgcrypto, and btree_gist included;
# all three are contrib.
#
# The migrations assume the database holds Boundary and nothing else. One runs a
# bare `analyze;` and another renames every unique constraint and foreign key it
# finds in pg_constraint, whichever schema the table is in. With another
# sample's schema already loaded, both reach it and stop the load, the ANALYZE on
# musicbrainz's expression indexes, whose unaccent dictionary reset-db has
# dropped, and the rename on a table search_path cannot see. So boundary is the
# first sample in SAMPLES.
BOUNDARY_SHA = 01cd5c86e8602aa9babc54b07e51ef7b8445e0b6

.PHONY: sample-db-boundary
sample-db-boundary:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS boundary'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/hashicorp/boundary/tar.gz/$(BOUNDARY_SHA) \
	  | tar xz -C "$$dir" --strip-components=5 boundary-$(BOUNDARY_SHA)/internal/db/schema/migrations && \
	cd "$$dir" && \
	{ ls base/postgres/*.up.sql; ls oss/postgres/*/*.up.sql | sort -t/ -k3,3n -k4,4; } \
	  | while read -r f; do cat "$$f"; echo; done \
	  | PGOPTIONS='-c search_path=boundary -c client_min_messages=warning' $(PSQL)

# Hatchet (hatchet-dev/hatchet, MIT). The schema ships as three files, so they
# are concatenated in the order Hatchet's sqlc.yaml lists them: v0.sql,
# v1-core.sql, and v1-olap.sql. The pg-stubs.sql it lists after them stands in
# for catalog views sqlc cannot see and is not schema. None of the three names a
# schema, so `hatchet` is created up front and search_path places everything,
# btree_gist included, which is contrib. The partitions are created at run time
# by functions the files define, so only the partitioned parents load.
HATCHET_SQL_FILES = v0.sql v1-core.sql v1-olap.sql

.PHONY: sample-db-hatchet
sample-db-hatchet:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS hatchet'
	for f in $(HATCHET_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/hatchet-dev/hatchet/1b410eb672448014bb1c6a29df92c63554f46034/sql/schema/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c search_path=hatchet -c client_min_messages=warning' $(PSQL)

# ThingsBoard (thingsboard/thingsboard, Apache-2.0). The schema ships as one
# file per part, loaded in the order ThingsBoard's installer runs them: the
# entity tables and their indexes, the PostgreSQL-only indexes, the views, and
# then the functions, several of which declare a variable of a view's row type,
# followed by the time series tables. schema-ts-latest-psql.sql is left out: only
# the migration from Cassandra reads it, and schema-entities.sql already creates
# the table it holds. None of them names a schema, so
# `thingsboard` is created up front and search_path places everything. The
# time series tables are partitioned, and their partitions are created at run
# time, so only the partitioned parents load.
THINGSBOARD_SQL_FILES = \
	schema-entities.sql \
	schema-entities-idx.sql \
	schema-entities-idx-psql-addon.sql \
	schema-views.sql \
	schema-functions.sql \
	schema-ts-psql.sql

.PHONY: sample-db-thingsboard
sample-db-thingsboard:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS thingsboard'
	for f in $(THINGSBOARD_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/thingsboard/thingsboard/562b19aa90f92c97b96c255b14816c32da7f4958/dao/src/main/resources/sql/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c search_path=thingsboard -c client_min_messages=warning' $(PSQL)

# Lago (getlago/lago-api, AGPL-3.0). Its db/structure.sql is Rails' pg_dump
# output like discourse's, and loads the same way as sample-db-pgdump-schema
# with two things taken out first. It was dumped with --clean, so it opens with
# about 1,400 lines of DROP ... IF EXISTS and placeholder views; with public
# second in search_path those could reach another sample's objects in `make
# schema`, so everything before the first `-- Name:` header is skipped. And it
# installs pg_partman, which is not contrib, into a schema of its own and keeps
# one template table there. pg_dump separates statements with a blank line, so
# every paragraph that names partman is dropped; the partitioned table Lago
# creates itself stays.
.PHONY: sample-db-lago
sample-db-lago:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS lago'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^-- Name: /{body=1} !body && /^(DROP |ALTER TABLE IF EXISTS |CREATE OR REPLACE VIEW )/{skip=1} body{skip=0} !skip' \
	  | awk -v RS= -v ORS='\n\n' '!/partman/' \
	  | sed -E "/^SELECT pg_catalog.set_config\('search_path', '', false\);\$$/d; /^SET search_path TO /,\$$d; s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g" \
	  | PGOPTIONS='-c search_path=lago,public' $(PSQL)

# A Prisma migration history replayed into a schema of its own. Prisma keeps
# one directory per migration under DIR, each holding a migration.sql, and
# applies them in name order, which starts with a timestamp. The repository
# tarball is fetched once and only DIR is extracted, since there are hundreds
# of files; the archive's top directory is named after the repository, so
# REPO is the name GitHub uses now. A few files end without a semicolon, or on
# a comment, so every file is followed by a newline and one. Prisma qualifies
# some statements with `public`, which is stripped so search_path places them.
# CLIENT_MIN_MESSAGES is passed in the way sample-db-url-schema passes it, for
# a migration history whose replay is noisy on a fresh database.
.PHONY: sample-db-prisma
sample-db-prisma:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/$(REPO)/tar.gz/$(SHA) \
	  | tar xz -C "$$dir" --strip-components=$$((1 + $(words $(subst /, ,$(DIR))))) $(notdir $(REPO))-$(SHA)/$(DIR) && \
	cd "$$dir" && LC_ALL=C && \
	for f in */migration.sql; do cat "$$f"; printf '\n;\n'; done \
	  | sed -E 's/"public"\.//g; s/([^A-Za-z0-9_])public\./\1/g' \
	  | PGOPTIONS='-c search_path=$(SCHEMA) -c client_min_messages=$(CLIENT_MIN_MESSAGES)' $(PSQL)

# Mattermost (mattermost/mattermost, AGPL-3.0 and Apache-2.0). The schema ships
# as golang-migrate migrations, 227 .up.sql files replayed in name order, so the
# repository tarball is fetched once and only the migrations directory is
# extracted, as sample-db-boundary and sample-db-prisma do; fetching 227 files
# one at a time is slow. None of the files names a schema, and the guards they
# write against information_schema all say current_schema(), so `mattermost` is
# created up front and search_path places everything. Eight of the files end
# without a semicolon, which would merge the next file's opening statement into
# the last one, so each is followed by a newline and one regardless. Most of
# them also add and drop with IF NOT EXISTS and IF EXISTS, which floods a fresh
# database with NOTICEs, so client_min_messages is raised to warning.
MATTERMOST_SHA = 3db1a9adf80fd91d911de1b02632bd0bc3e9fe9d

.PHONY: sample-db-mattermost
sample-db-mattermost:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS mattermost'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/mattermost/mattermost/tar.gz/$(MATTERMOST_SHA) \
	  | tar xz -C "$$dir" --strip-components=6 mattermost-$(MATTERMOST_SHA)/server/channels/db/migrations/postgres && \
	cd "$$dir" && LC_ALL=C && \
	for f in *.up.sql; do cat "$$f"; printf '\n;\n'; done \
	  | PGOPTIONS='-c search_path=mattermost -c client_min_messages=warning' $(PSQL)

# Lemmy (LemmyNet/lemmy, AGPL-3.0). The schema ships as Diesel migrations, 342
# directories each holding an up.sql and replayed in name order, and that is
# only half of it: every trigger function lives in a schema named `r` that
# Lemmy's own runner builds afterwards out of two files, dropping and recreating
# the schema whenever they change. So the migrations are followed by the
# CREATE SCHEMA r and the two files that schema_setup/mod.rs lists, which leaves
# the 66 triggers on lemmy's tables calling functions in `r`. The repository
# tarball is fetched once and both paths are extracted from it, since fetching
# 342 files one at a time is slow.
#
# None of the files names a schema, so `lemmy` is created up front and
# search_path places everything, the contrib extensions ltree, pg_trgm, and
# pgcrypto included; the migrations also create a `utils` schema of their own,
# so the sample is checked with all three. Some of them qualify a table or a
# function with `public`, which is stripped the way sample-db-pgdump-schema
# strips it. The `r` and `utils` files qualify nothing, so the same sed is
# harmless there.
#
# One migration puts a trigger on __diesel_schema_migrations, Diesel's own
# bookkeeping table, which the CLI creates rather than a migration. A stand-in
# is created before the migrations run and dropped once they have, as
# sample-db-harbor does with golang-migrate's; it is not part of Lemmy's schema.
# The migrations drop what they are about to create with IF EXISTS throughout,
# so client_min_messages is raised to warning.
#
# Twenty-two of them turn a table's indexes off around a bulk update with
# `UPDATE pg_index ... WHERE indrelid = (SELECT oid FROM pg_class WHERE relname
# = '<table>')`, which names no schema. Lemmy owns its database, so upstream
# that subquery returns one row; here every other sample's schema is still
# there, and `comment` alone matches several, which fails the load with "more
# than one row returned by a subquery". The sed scopes those lookups to the
# lemmy schema, which is what the migration means. It matches only the bare
# `relname =` predicates, so the `relname LIKE '%ccnew%'` inside
# drop_ccnew_indexes, the one that sits in a function body, is left as upstream
# wrote it.
LEMMY_SHA = 646f5a01558d5a38859558426ce54b06185b92e2
LEMMY_REPLACEABLE = crates/diesel_utils/replaceable_schema

.PHONY: sample-db-lemmy
sample-db-lemmy:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS lemmy'
	$(PSQL) -c 'SET search_path = lemmy; CREATE TABLE __diesel_schema_migrations (version varchar(50) NOT NULL PRIMARY KEY, run_on timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP)'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/LemmyNet/lemmy/tar.gz/$(LEMMY_SHA) \
	  | tar xz -C "$$dir" --strip-components=1 \
	      lemmy-$(LEMMY_SHA)/migrations lemmy-$(LEMMY_SHA)/$(LEMMY_REPLACEABLE) && \
	cd "$$dir" && LC_ALL=C && \
	{ for f in migrations/*/up.sql; do cat "$$f"; printf '\n;\n'; done; \
	  echo 'CREATE SCHEMA r;'; \
	  cat $(LEMMY_REPLACEABLE)/utils.sql $(LEMMY_REPLACEABLE)/triggers.sql; } \
	  | sed -E "s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g; \
	            s/^([[:space:]]*)relname = /\1relnamespace = 'lemmy'::regnamespace AND relname = /" \
	  | PGOPTIONS='-c search_path=lemmy -c client_min_messages=warning' $(PSQL)
	$(PSQL) -c 'SET search_path = lemmy; DROP TABLE __diesel_schema_migrations'

# Windmill (windmill-labs/windmill, AGPL-3.0). The schema ships as sqlx
# migrations, 661 .up.sql files replayed in name order, so the repository
# tarball is fetched once and only the migrations directory is extracted. It
# names no schema, so `windmill` is created up front and search_path places
# everything; the five files that qualify something with `public` have the
# qualifier stripped the way sample-db-pgdump-schema strips it. The migrations
# create an `extensions` schema of their own and install uuid-ossp there, which
# is contrib, so `extensions` stays second in the search path for the column
# defaults that call it and is not part of the check.
#
# They also create the windmill_user and windmill_admin roles, each in a DO
# block that swallows the error when the role is already there. pistachio does
# not manage roles, but 366 of Windmill's row-level security policies name one,
# so they have to exist for the policies to load.
#
# One migration reads _sqlx_migrations, sqlx's own bookkeeping table, which the
# migrator creates rather than a migration, so a stand-in is created before the
# migrations run and dropped once they have, as sample-db-harbor does with
# golang-migrate's. Several functions are declared before the tables they read,
# so check_function_bodies is turned off as it is for coder, and the migrations
# drop what they are about to create with IF EXISTS throughout, so
# client_min_messages is raised to warning.
#
# Four of the catalog lookups assume Windmill owns the database, and the sed
# scopes all four to the schema the sample loads into. Two read
# information_schema.columns with no schema filter, one of them to build
# queue_view out of whichever columns it finds, which picks up another sample's
# `queue` and fails the load; they get table_schema = current_schema(). Two more
# name schemaname = 'public' when they look through pg_policies, which finds
# nothing here and silently skips what they do: one creates admin_policy where
# it is missing, the other rewrites the policies that read a session GUC. That
# second one is why the rewrite matters rather than just being tidy -- left
# alone, the sample would carry 366 policies with the wrong expressions in them.
# All four sites sit in DO blocks or in a pg_temp function, so no definition the
# round trip reads is touched.
WINDMILL_SHA = 5371519f0f5ce7750982dcdb374dca72115902e7

.PHONY: sample-db-windmill
sample-db-windmill:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS windmill'
	$(PSQL) -c 'SET search_path = windmill; CREATE TABLE _sqlx_migrations (version bigint NOT NULL PRIMARY KEY, description text NOT NULL, installed_on timestamptz NOT NULL DEFAULT now(), success boolean NOT NULL, checksum bytea NOT NULL, execution_time bigint NOT NULL)'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/windmill-labs/windmill/tar.gz/$(WINDMILL_SHA) \
	  | tar xz -C "$$dir" --strip-components=3 windmill-$(WINDMILL_SHA)/backend/migrations && \
	cd "$$dir" && LC_ALL=C && \
	for f in *.up.sql; do cat "$$f"; printf '\n;\n'; done \
	  | sed -E "s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g; \
	            s/WHERE table_name = /WHERE table_schema = current_schema() AND table_name = /; \
	            s/schemaname = 'public'/schemaname = current_schema()/" \
	  | PGOPTIONS='-c search_path=windmill,extensions -c client_min_messages=warning -c check_function_bodies=off' $(PSQL)
	$(PSQL) -c 'SET search_path = windmill; DROP TABLE _sqlx_migrations'

# CitizenLab (CitizenLabDotCo/citizenlab, AGPL-3.0). Its db/structure.sql is
# Rails' pg_dump output like discourse's, but it is a multi-tenant schema: the
# extensions live in a schema of its own named shared_extensions, which the file
# creates, and every use of them is qualified with it. So only the `public.`
# qualifier is stripped and search_path places the rest, the way
# sample-db-pgdump-schema does it.
#
# It was dumped with --clean, so like lago it opens with several hundred DROP
# statements; everything before the first `-- Name:` header is skipped. Here
# that is not only tidiness: the last two lines of the preamble are
# `DROP SCHEMA IF EXISTS shared_extensions` and `DROP SCHEMA IF EXISTS public`,
# and the second would take every public sample with it in `make schema`.
# Skipping the preamble also drops the SET lines pg_dump writes at the top, so
# check_function_bodies and client_min_messages are passed in instead. The
# `CREATE SCHEMA public` that opens the body is dropped as well, since reset-db
# has just created it.
#
# It needs PostGIS for three geography columns and pgvector for one vector
# column and the hnsw index over it; compose.yaml and the samples CI job install
# both for discourse, osm, inaturalist, and dhis2 already. pgcrypto, pg_trgm,
# and uuid-ossp are contrib.
.PHONY: sample-db-citizenlab
sample-db-citizenlab:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS citizenlab'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^-- Name: /{body=1} body' \
	  | sed -E "/^CREATE SCHEMA public;\$$/d; /^SET search_path TO /,\$$d; s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g" \
	  | PGOPTIONS='-c search_path=citizenlab -c client_min_messages=warning -c check_function_bodies=off' $(PSQL)

# Dokploy (Dokploy/dokploy, Apache-2.0). The schema ships as Drizzle
# migrations, and Drizzle is the fifth migration tool in this list after
# Diesel, sqlx, golang-migrate, and Prisma. The repository tarball is fetched
# once and only the drizzle directory is extracted.
#
# Which files to replay comes from meta/_journal.json rather than from the
# directory listing, because those two do not agree: 0130_abandoned_dagger.sql
# is on disk but not in the journal, so Drizzle never applies it, and replaying
# it adds a `customEntrypoint` column that 0158 adds again, which stops the
# load. The journal lists its tags in the order Drizzle applies them, which is
# also name order, so the tags are read out of it and each file catted in turn.
# A few end without a semicolon, so each is followed by a newline and one.
#
# None of the files names a schema, so `dokploy` is created up front and
# search_path places everything, but the foreign keys Drizzle writes qualify
# their target with "public", which is stripped the way sample-db-prisma strips
# it. client_min_messages is raised to warning for the two foreign key names
# that run past the 63 character identifier limit, which the server says so
# about as it truncates them, as it does for wso2is, and for the one DROP that
# cascades.
DOKPLOY_SHA = 853ca33659853093ee6a91af3719abdc4e1bae72

.PHONY: sample-db-dokploy
sample-db-dokploy:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS dokploy'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/Dokploy/dokploy/tar.gz/$(DOKPLOY_SHA) \
	  | tar xz -C "$$dir" --strip-components=4 dokploy-$(DOKPLOY_SHA)/apps/dokploy/drizzle && \
	cd "$$dir" && \
	grep -oE '"tag": "[^"]+"' meta/_journal.json | sed 's/.*: "//; s/"$$//' \
	  | while read -r t; do cat "$$t.sql"; printf '\n;\n'; done \
	  | sed -E 's/"public"\.//g; s/([^A-Za-z0-9_])public\./\1/g' \
	  | PGOPTIONS='-c search_path=dokploy -c client_min_messages=warning' $(PSQL)

# Hyperswitch (juspay/hyperswitch, Apache-2.0), the payments orchestrator. The
# schema ships as Diesel migrations, 530 directories each holding an up.sql and
# replayed in name order, so the repository tarball is fetched once and only the
# migrations directory is extracted, as sample-db-lemmy does. Every directory
# but Diesel's own 00000000000000_diesel_initial_setup is named for a date, so
# plain name order is the order Diesel applies them in. Four of the files end
# without a semicolon, so each is followed by a newline and one.
#
# Nothing in them names a schema, qualifies anything with public, or installs an
# extension, so `hyperswitch` is created up front and search_path places
# everything. The migrations drop what they are about to create with IF EXISTS
# throughout, so client_min_messages is raised to warning.
HYPERSWITCH_SHA = 80d426974242eecedefed015b617202fbdcbdac1

.PHONY: sample-db-hyperswitch
sample-db-hyperswitch:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS hyperswitch'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/juspay/hyperswitch/tar.gz/$(HYPERSWITCH_SHA) \
	  | tar xz -C "$$dir" --strip-components=2 hyperswitch-$(HYPERSWITCH_SHA)/migrations && \
	cd "$$dir" && LC_ALL=C && \
	for f in */up.sql; do cat "$$f"; printf '\n;\n'; done \
	  | PGOPTIONS='-c search_path=hyperswitch -c client_min_messages=warning' $(PSQL)

.PHONY: test-samples
test-samples:
	bash test/samples/run.sh

# Drop every extension outside pg_catalog. An extension owns tables of its own,
# PostGIS's spatial_ref_sys among them, and neither DROP TABLE nor DROP SCHEMA
# will touch one while the extension is there, so a wipe has to drop the
# extensions before it starts.
#
# Dropping them is also what makes a sample loadable after the one before it:
# an extension is visible to the next sample whichever schema it sits in, so a
# schema that says CREATE EXTENSION IF NOT EXISTS gets nothing when the
# extension already exists in some other sample's schema, and then its types
# and operator classes do not resolve. icingadb and sourcegraph both install
# citext, and sourcegraph and gitlab both install pg_trgm.
.PHONY: drop-extensions
drop-extensions:
	psql -X -q -At -v ON_ERROR_STOP=1 -c "SELECT quote_ident(extname) FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE n.nspname <> 'pg_catalog'" \
	  | while read -r e; do \
	      psql -X -q -v ON_ERROR_STOP=1 -c "SET client_min_messages TO warning; DROP EXTENSION IF EXISTS $$e CASCADE" || exit 1; \
	    done

# Wipe every user schema. Used by `schema` and `demo` to start from an empty
# database, by test-samples once before its first sample, and by `test` and
# `test-scenario`, which reset only `public` on their own.
#
# The extensions go first, since a table one of them owns stops both of the
# statements below. Then the tables, a batch at a time, and only then the
# schemas. A single DROP SCHEMA ... CASCADE takes locks on every object it
# reaches, and the larger samples do not fit: gitlab owns 1,400 tables and
# their indexes, which runs the server out of lock table space at the default
# max_locks_per_transaction. Each batch is a statement of its own, so its locks
# are released before the next one starts, and the schemas are empty by the
# time they are dropped.
DROP_TABLE_BATCH = 50

.PHONY: clean-schema
clean-schema: drop-extensions
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
# whichever schema they sit in; drop-extensions says why. They go first, so a
# table one of them owns cannot stop the DROP SCHEMA below either.
.PHONY: reset-db
reset-db: drop-extensions
	psql -X -q -v ON_ERROR_STOP=1 -c 'SET client_min_messages TO warning; DROP SCHEMA IF EXISTS public CASCADE'
	psql -X -q -v ON_ERROR_STOP=1 -c 'CREATE SCHEMA public'
