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
demodb|sample-db-demodb|URL=https://raw.githubusercontent.com/postgrespro/demodb/bf7a1c1972d2f89dc9de21f19d7dd3aa650e8647/tables.sql|bookings
musicbrainz|sample-db-musicbrainz||musicbrainz
znuny|sample-db-znuny||znuny
hive|sample-db-hive|URL=https://raw.githubusercontent.com/apache/hive/d98bfeda81c23007866bd5bf7ee970fa017689ed/standalone-metastore/metastore-server/src/main/sql/postgres/hive-schema-4.2.0.postgres.sql SCHEMA=hive|hive
ranger|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/ranger/0249cc1d8255c0370322508c7e1d30250e152340/security-admin/db/postgres/optimized/current/ranger_core_db_postgres.sql SCHEMA=ranger CLIENT_MIN_MESSAGES=error|ranger
ambari|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/ambari/0347dc4503c09b0593d9cb12815e2f9784b6a86a/ambari-server/src/main/resources/Ambari-DDL-Postgres-CREATE.sql SCHEMA=ambari|ambari
ovirt|sample-db-url-schema|URL=https://raw.githubusercontent.com/oVirt/ovirt-engine/be1f6647db1ebbf39186bbe4dd6b1a777376815b/packaging/dbscripts/create_tables.sql SCHEMA=ovirt|ovirt
gitlab|sample-db-url-schema|URL=https://raw.githubusercontent.com/gitlabhq/gitlabhq/35e789d8f1173a11a7724ae360a80d1f19ec92dc/db/structure.sql SCHEMA=gitlab|gitlab,gitlab_partitions_static,gitlab_partitions_dynamic
ledgersmb|sample-db-url-schema|URL=https://raw.githubusercontent.com/ledgersmb/LedgerSMB/34a05e7b58c161e008561e36a74dbede860fe4d9/sql/Pg-database.sql SCHEMA=ledgersmb|ledgersmb
koji|sample-db-url-schema|URL=https://raw.githubusercontent.com/koji-project/koji/c3239d46c1abdcbda509739eb0182031b71d9f01/schemas/schema.sql SCHEMA=koji|koji
kea|sample-db-url-schema|URL=https://raw.githubusercontent.com/isc-projects/kea/82440eddc70089a55892a42ca96b78a92eb3e484/src/share/database/scripts/pgsql/dhcpdb_create.pgsql SCHEMA=kea|kea
dolphinscheduler|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/dolphinscheduler/51057477b815eef59c68f1b76563567814c72a93/dolphinscheduler-dao/src/main/resources/sql/dolphinscheduler_postgresql.sql SCHEMA=dolphinscheduler|dolphinscheduler
camunda|sample-db-camunda||camunda
wso2apim|sample-db-url-schema|URL=https://raw.githubusercontent.com/wso2/carbon-apimgt/5dd6e3084a35228b7542c4ff6a9071af1c7476fa/features/apimgt/org.wso2.carbon.apimgt.core.feature/src/main/resources/sql/postgresql.sql SCHEMA=wso2apim|wso2apim
discourse|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/discourse/discourse/f3c568cfd26a427e9cae32063732a56bc7d334b9/db/structure.sql SCHEMA=discourse|discourse
icinga_director|sample-db-url-schema|URL=https://raw.githubusercontent.com/Icinga/icingaweb2-module-director/b2e4a4e4180b0a160461a773ca8b8b5874e0fba7/schema/pgsql.sql SCHEMA=icinga_director|icinga_director
flowable|sample-db-url-schema|URL=https://raw.githubusercontent.com/flowable/flowable-engine/53e93b6681e86dccea720efaa3c0fc2a96f57366/distro/sql/create/all/flowable.postgres.all.create.sql SCHEMA=flowable|flowable
ejabberd|sample-db-url-schema|URL=https://raw.githubusercontent.com/processone/ejabberd/f42a49c1e83ad2399743dd46c6cf1e43d39d303b/sql/pg.new.sql SCHEMA=ejabberd|ejabberd
guacamole|sample-db-url-schema|URL=https://raw.githubusercontent.com/apache/guacamole-client/5be18be1eeadc4cc544c737c54bd761261d2ad65/extensions/guacamole-auth-jdbc/modules/guacamole-auth-jdbc-postgresql/schema/001-create-schema.sql SCHEMA=guacamole|guacamole
dotcms|sample-db-url-schema|URL=https://raw.githubusercontent.com/dotCMS/core/b0095c0c3920e236efcc16ceb136cd5dd88804b7/dotCMS/src/main/resources/postgres.sql SCHEMA=dotcms|dotcms
osm|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/openstreetmap/openstreetmap-website/9da0fa5ecbff8adc5e6e91c7cf22546755a91f77/db/structure.sql SCHEMA=osm|osm
chado|sample-db-chado|URL=https://raw.githubusercontent.com/GMOD/Chado/31c2407716e3d0fe4e837b6effad0a510af22238/chado/schemas/1.31/default_schema.sql|chado,genetic_code,so,frange
wso2is|sample-db-url-schema|URL=https://raw.githubusercontent.com/wso2/carbon-identity-framework/a956a69ffdd1bcf916132682b12b5f319102f69e/features/identity-core/org.wso2.carbon.identity.core.server.feature/resources/dbscripts/postgresql.sql SCHEMA=wso2is|wso2is
nightingale|sample-db-url-schema|URL=https://raw.githubusercontent.com/ccfos/nightingale/8362cbe18f98b3c06af176d71b13994673912c46/docker/compose-postgres/initsql_for_postgres/a-n9e-for-Postgres.sql SCHEMA=nightingale|nightingale
danbooru|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/danbooru/danbooru/f8de3ba286db1f9a1f3efbbf495bcbc1001e7b9b/db/structure.sql SCHEMA=danbooru|danbooru
openolat|sample-db-url-schema|URL=https://raw.githubusercontent.com/OpenOLAT/OpenOLAT/8d07a02fe4dafb8c82179f3efa77e1cb985fdc7e/src/main/resources/database/postgresql/setupDatabase.sql SCHEMA=openolat|openolat
inaturalist|sample-db-pgdump-schema|URL=https://raw.githubusercontent.com/inaturalist/inaturalist/e52c649d2f0e8e260c2dce6fcf6448d971100415/db/structure.sql SCHEMA=inaturalist|inaturalist
joomla|sample-db-joomla||joomla
harbor|sample-db-harbor||harbor
bigbluebutton|sample-db-bigbluebutton|URL=https://raw.githubusercontent.com/bigbluebutton/bigbluebutton/4c3a477fe3e34a7da6854c491be2c8d02e83c083/bbb-graphql-server/bbb_schema.sql SCHEMA=bigbluebutton|bigbluebutton
listmonk|sample-db-url-schema|URL=https://raw.githubusercontent.com/knadh/listmonk/594b74056dd8a0d3a7621a32898ee38bfbe10e96/schema.sql SCHEMA=listmonk|listmonk
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
langfuse|sample-db-prisma|REPO=langfuse/langfuse SHA=330bb86bcc332a76d4b24569fa4e1c1086d651d1 DIR=packages/shared/prisma/migrations SCHEMA=langfuse|langfuse
icinga_ido|sample-db-url-schema|URL=https://raw.githubusercontent.com/Icinga/icinga2/3be2e74d386eaedb105b1acab69cbd88190957c9/lib/db_ido_pgsql/schema/pgsql.sql SCHEMA=icinga_ido|icinga_ido
openfire|sample-db-url-schema|URL=https://raw.githubusercontent.com/igniterealtime/Openfire/c1b8d31a49f17a273aea2ccd89321d34c72fad3b/distribution/src/database/openfire_postgresql.sql SCHEMA=openfire|openfire
bareos|sample-db-url-schema|URL=https://raw.githubusercontent.com/bareos/bareos/13a52fc1da1b2b131cf794fa4adb3936d39e049d/core/src/cats/ddl/creates/postgresql.sql SCHEMA=bareos|bareos
opencms|sample-db-url-schema|URL=https://raw.githubusercontent.com/alkacon/opencms-core/3411490f10d73474d15d3f0dd31d132793cb22cb/webapp/WEB-INF/setupdata/database/postgresql/create_tables.sql SCHEMA=opencms|opencms
marquez|sample-db-marquez||marquez
penpot|sample-db-penpot||penpot
dcm4chee|sample-db-dcm4chee||dcm4chee
kamailio|sample-db-kamailio||kamailio
alfresco|sample-db-alfresco||alfresco
roundcube|sample-db-url-schema|URL=https://raw.githubusercontent.com/roundcube/roundcubemail/4b54c2acfb54d5ee3d1c281ca7f143bed0dea804/SQL/postgres.initial.sql SCHEMA=roundcube|roundcube
shenyu|sample-db-shenyu||shenyu
nacos|sample-db-url-schema|URL=https://raw.githubusercontent.com/alibaba/nacos/d74b69fa71de104c3ed15310ef8e32d1cded8a95/plugin-default-impl/nacos-default-datasource-plugin/nacos-datasource-plugin-postgresql/src/main/resources/META-INF/pg-schema.sql SCHEMA=nacos|nacos
openreplay|sample-db-openreplay|URL=https://raw.githubusercontent.com/openreplay/openreplay/3fce37d89113ca7a06c9d9d767ba8d274df94d26/scripts/schema/db/init_dbs/postgresql/init_schema.sql SCHEMA=openreplay|openreplay,events,events_common,events_ios,spots
logto|sample-db-logto||logto
omero|sample-db-omero||omero
concourse|sample-db-concourse||concourse
affine|sample-db-affine||affine
teable|sample-db-prisma|REPO=teableio/teable SHA=5ef2238883cad7c3980084de9a9031135fb9734f DIR=packages/db-main-prisma/prisma/postgres/migrations SCHEMA=teable|teable
uyuni|sample-db-uyuni|CLIENT_MIN_MESSAGES=error|uyuni,access,rpm,deb,rhn_cache,rhn_channel,rhn_config,rhn_config_channel,rhn_entitlements,rhn_exception,rhn_org,rhn_server,rhn_user
lobehub|sample-db-lobehub||lobehub
endef

# Every loader pipes its schema into this psql. ON_ERROR_STOP makes a failing
# statement stop the load and fail the target, which the runner reports as
# FAIL (load). Without it psql prints the error, carries on, and exits 0, and
# the check runs against a schema quietly missing whatever the statement was
# going to create -- which dump and plan then agree about, so the sample passes.
#
# It also raises client_min_messages for every loader at once, rather than
# sample by sample. A dump that drops what it is about to create with IF
# EXISTS, an identifier past 63 characters the server truncates, an index a
# constraint takes over and renames: each says so on a fresh database, none of
# it is about the schema under test, and the runner passes a loader's stderr
# through. A sample noisier still can raise the level further from its SAMPLES
# record, as ranger does to error, since it commits outside a transaction,
# which adds a warning per statement.
#
# PGOPTS carries whatever else a loader needs, search_path above all. It is set
# per target rather than inline in front of $(PSQL), because an inline
# PGOPTIONS assignment and the one in PSQL would be two assignments to the same
# variable and the last one, PSQL's, would win.
CLIENT_MIN_MESSAGES ?= warning
PGOPTS =
PSQL = PGOPTIONS='$(strip -c client_min_messages=$(CLIENT_MIN_MESSAGES) $(PGOPTS))' psql -v ON_ERROR_STOP=1

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
# search_path, so no search_path is needed here.
MIMICIV_SQL_FILES = create.sql constraint.sql index.sql

.PHONY: sample-db-mimiciv
sample-db-mimiciv:
	for f in $(MIMICIV_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/MIT-LCP/mimic-code/3a914fce11e05888a4b659c7788e207bc34d1728/mimic-iv/buildmimic/postgres/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

# A plain SQL URL loaded into a schema of its own. These dumps name no schema at
# all, so search_path decides where they land, and upstream expects them in the
# search path's first schema. Loading them into `public` would collide with the
# other public samples when `make schema` puts everything in one database
# (mediawiki and pagila both define `actor` and `category`, for one), so each
# gets its own schema instead.

# CHECK_FUNCTION_BODIES defaults to on, the server default. coder turns it off:
# its dump is pg_dump output with the preamble stripped, and that preamble is
# where pg_dump turns it off itself. Without it a plpgsql function declaring a
# variable of a table's row type stops the load, since the table comes later in
# the file.
CHECK_FUNCTION_BODIES ?= on

sample-db-url-schema: PGOPTS = -c search_path=$(SCHEMA) -c check_function_bodies=$(CHECK_FUNCTION_BODIES)
.PHONY: sample-db-url-schema
sample-db-url-schema:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) | $(PSQL)

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
sample-db-chado: PGOPTS = -c search_path=chado
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
	  | $(PSQL)

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
# IF EXISTS before creating them.
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

sample-db-musicbrainz: PGOPTS = -c search_path=musicbrainz,public
.PHONY: sample-db-musicbrainz
sample-db-musicbrainz:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS musicbrainz'
	for f in $(MUSICBRAINZ_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/metabrainz/musicbrainz-server/424c5fad44da2b3ad55d08286fe8ad07c11ec471/admin/sql/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

# Znuny (znuny/Znuny, GPL-3.0). The schema ships as two files, so concatenate
# them: schema.postgresql.sql (tables and indexes) and then
# schema-post.postgresql.sql (the foreign keys, which need every table to
# exist). Neither names a schema, so search_path decides where they land and
# `public` would collide with the other public samples; create `znuny` up front
# and point search_path at it, as sample-db-url-schema does for the one-file
# dumps.
ZNUNY_SQL_FILES = schema.postgresql.sql schema-post.postgresql.sql

sample-db-znuny: PGOPTS = -c search_path=znuny
.PHONY: sample-db-znuny
sample-db-znuny:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS znuny'
	for f in $(ZNUNY_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/znuny/Znuny/0b894348ebc458621545ccdab5f3d24d1b396a70/scripts/database/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

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

sample-db-camunda: PGOPTS = -c search_path=camunda
.PHONY: sample-db-camunda
sample-db-camunda:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS camunda'
	for f in $(CAMUNDA_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/camunda/camunda-bpm-platform/ee4826e5e76c2348a1510ef46a2f4ccd3b080e48/engine/src/main/resources/org/camunda/bpm/engine/db/create/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

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
sample-db-pgdump-schema: PGOPTS = -c search_path=$(SCHEMA),public
.PHONY: sample-db-pgdump-schema
sample-db-pgdump-schema:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | sed -E "/^SELECT pg_catalog.set_config\('search_path', '', false\);\$$/d; /^SET search_path TO /,\$$d; s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g" \
	  | $(PSQL)

# Joomla CMS (joomla/joomla-cms, GPL-2.0-or-later). The PostgreSQL installer
# ships as three files that must load in that order -- base.sql (the core
# tables), extensions.sql (the bundled extensions and plugins), and
# supports.sql (the smart search tables, which reference extensions.sql's
# content types) -- none of which create a schema or set search_path
# themselves. Every table name carries the literal `#__` prefix Joomla
# replaces at install time; left as `#__` and quoted, it is just an ordinary
# identifier, so nothing needs rewriting.
JOOMLA_SQL_FILES = base.sql extensions.sql supports.sql

sample-db-joomla: PGOPTS = -c search_path=joomla
.PHONY: sample-db-joomla
sample-db-joomla:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS joomla'
	for f in $(JOOMLA_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/joomla/joomla-cms/b2648c39655a1dc9ceb2791cf9131acc4f98d22e/installation/sql/postgresql/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

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

sample-db-harbor: PGOPTS = -c search_path=harbor
.PHONY: sample-db-harbor
sample-db-harbor:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS harbor'
	$(PSQL) -c 'SET search_path = harbor; CREATE TABLE schema_migrations (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL DEFAULT false)'
	for f in $(HARBOR_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/goharbor/harbor/fb4e2406747df0b01dec994bf593f49261f6a874/make/migrations/postgresql/$$f || exit 1; \
	  echo ';'; \
	done | $(PSQL)
	$(PSQL) -c 'SET search_path = harbor; DROP TABLE schema_migrations'

# BigBlueButton (bigbluebutton/bigbluebutton, LGPL-3.0). The schema belongs in
# a schema of its own like the sample-db-url-schema dumps, and names none for
# almost everything it creates, but three of its views are qualified with
# `public`. Those three would land outside the sample's schema, and the views
# that select from them would then not resolve, so the qualifier is stripped
# and search_path places them with the rest. The file also installs unaccent,
# which a function behind three of its generated columns calls; it is contrib,
# so the official image already has it.
sample-db-bigbluebutton: PGOPTS = -c search_path=$(SCHEMA)
.PHONY: sample-db-bigbluebutton
sample-db-bigbluebutton:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | sed -E 's/^(CREATE OR REPLACE VIEW )public\./\1/' \
	  | $(PSQL)

# DHIS2 (dhis2/dhis2-core, BSD-3-Clause). A pg_dump of the base schema Flyway
# starts from, naming no schema and no owner, so it loads like the
# sample-db-url-schema dumps. One of its columns is a PostGIS geometry, though,
# and the dump does not install the extension, so install it first and leave
# `public` in the search path for the type to resolve from. PostGIS is not in
# the official image; compose.yaml and the samples CI job install it for the
# osm and inaturalist samples already.
sample-db-dhis2: PGOPTS = -c search_path=$(SCHEMA),public
.PHONY: sample-db-dhis2
sample-db-dhis2:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public'
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | $(PSQL)

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

sample-db-boundary: PGOPTS = -c search_path=boundary
.PHONY: sample-db-boundary
sample-db-boundary:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS boundary'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/hashicorp/boundary/tar.gz/$(BOUNDARY_SHA) \
	  | tar xz -C "$$dir" --strip-components=5 boundary-$(BOUNDARY_SHA)/internal/db/schema/migrations && \
	cd "$$dir" && \
	{ ls base/postgres/*.up.sql; ls oss/postgres/*/*.up.sql | sort -t/ -k3,3n -k4,4; } \
	  | while read -r f; do cat "$$f"; echo; done \
	  | $(PSQL)

# Hatchet (hatchet-dev/hatchet, MIT). The schema ships as three files, so they
# are concatenated in the order Hatchet's sqlc.yaml lists them: v0.sql,
# v1-core.sql, and v1-olap.sql. The pg-stubs.sql it lists after them stands in
# for catalog views sqlc cannot see and is not schema. None of the three names a
# schema, so `hatchet` is created up front and search_path places everything,
# btree_gist included, which is contrib. The partitions are created at run time
# by functions the files define, so only the partitioned parents load.
HATCHET_SQL_FILES = v0.sql v1-core.sql v1-olap.sql

sample-db-hatchet: PGOPTS = -c search_path=hatchet
.PHONY: sample-db-hatchet
sample-db-hatchet:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS hatchet'
	for f in $(HATCHET_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/hatchet-dev/hatchet/1b410eb672448014bb1c6a29df92c63554f46034/sql/schema/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

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

sample-db-thingsboard: PGOPTS = -c search_path=thingsboard
.PHONY: sample-db-thingsboard
sample-db-thingsboard:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS thingsboard'
	for f in $(THINGSBOARD_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/thingsboard/thingsboard/562b19aa90f92c97b96c255b14816c32da7f4958/dao/src/main/resources/sql/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

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
sample-db-lago: PGOPTS = -c search_path=lago,public
.PHONY: sample-db-lago
sample-db-lago:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS lago'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^-- Name: /{body=1} !body && /^(DROP |ALTER TABLE IF EXISTS |CREATE OR REPLACE VIEW )/{skip=1} body{skip=0} !skip' \
	  | awk -v RS= -v ORS='\n\n' '!/partman/' \
	  | sed -E "/^SELECT pg_catalog.set_config\('search_path', '', false\);\$$/d; /^SET search_path TO /,\$$d; s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g" \
	  | $(PSQL)

# A Prisma migration history replayed into a schema of its own. Prisma keeps
# one directory per migration under DIR, each holding a migration.sql, and
# applies them in name order, which starts with a timestamp. The repository
# tarball is fetched once and only DIR is extracted, since there are hundreds
# of files; the archive's top directory is named after the repository, so
# REPO is the name GitHub uses now. A few files end without a semicolon, or on
# a comment, so every file is followed by a newline and one. Prisma qualifies
# some statements with `public`, which is stripped so search_path places them.
sample-db-prisma: PGOPTS = -c search_path=$(SCHEMA)
.PHONY: sample-db-prisma
sample-db-prisma:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/$(REPO)/tar.gz/$(SHA) \
	  | tar xz -C "$$dir" --strip-components=$$((1 + $(words $(subst /, ,$(DIR))))) $(notdir $(REPO))-$(SHA)/$(DIR) && \
	cd "$$dir" && LC_ALL=C && \
	for f in */migration.sql; do cat "$$f"; printf '\n;\n'; done \
	  | sed -E 's/"public"\.//g; s/([^A-Za-z0-9_])public\./\1/g' \
	  | $(PSQL)

# Marquez (MarquezProject/marquez, Apache-2.0), the reference implementation of
# OpenLineage. The schema ships as Flyway migrations, and Flyway is the sixth
# migration tool in this list after Diesel, sqlx, golang-migrate, Prisma, and
# Drizzle: 81 versioned files and 3 repeatable ones in one directory, which the
# repository tarball is fetched once for.
#
# Flyway applies the versioned files in version order, which is not the name
# order the other migration loaders here replay in, since V10 comes after V9 and
# V17.1 between V17 and V18. So the version is cut out of each name and sorted
# on its own, and the repeatable files, the ones named R__, follow them in name
# order, which is where Flyway runs them. A few files end without a semicolon,
# so each is followed by a newline and one.
#
# Marquez ships seven Java migrations as well, which Flyway runs in the same
# sequence. Six of them backfill rows and the seventh creates facet views, so
# the sample is checked without those views, the way musicbrainz is checked
# without the triggers its file list leaves out. Nothing in the SQL files names
# a schema or qualifies anything with public, so `marquez` is created up front
# and search_path places everything.
MARQUEZ_SHA = 180f37b22387146187af1ef0279e3ee1d1ccd789

sample-db-marquez: PGOPTS = -c search_path=marquez
.PHONY: sample-db-marquez
sample-db-marquez:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS marquez'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/MarquezProject/marquez/tar.gz/$(MARQUEZ_SHA) \
	  | tar xz -C "$$dir" --strip-components=8 marquez-$(MARQUEZ_SHA)/api/src/main/resources/marquez/db/migration && \
	cd "$$dir" && LC_ALL=C && \
	{ for f in V*.sql; do v=$${f#V}; printf '%s\t%s\n' "$${v%%__*}" "$$f"; done | sort -V | cut -f2; ls R__*.sql; } \
	  | while read -r f; do cat "$$f"; printf '\n;\n'; done \
	  | $(PSQL)

# Mattermost (mattermost/mattermost, AGPL-3.0 and Apache-2.0). The schema ships
# as golang-migrate migrations, 227 .up.sql files replayed in name order, so the
# repository tarball is fetched once and only the migrations directory is
# extracted, as sample-db-boundary and sample-db-prisma do; fetching 227 files
# one at a time is slow. None of the files names a schema, and the guards they
# write against information_schema all say current_schema(), so `mattermost` is
# created up front and search_path places everything. Eight of the files end
# without a semicolon, which would merge the next file's opening statement into
# the last one, so each is followed by a newline and one regardless. Most of
# them also add and drop with IF NOT EXISTS and IF EXISTS.
MATTERMOST_SHA = 3db1a9adf80fd91d911de1b02632bd0bc3e9fe9d

sample-db-mattermost: PGOPTS = -c search_path=mattermost
.PHONY: sample-db-mattermost
sample-db-mattermost:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS mattermost'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/mattermost/mattermost/tar.gz/$(MATTERMOST_SHA) \
	  | tar xz -C "$$dir" --strip-components=6 mattermost-$(MATTERMOST_SHA)/server/channels/db/migrations/postgres && \
	cd "$$dir" && LC_ALL=C && \
	for f in *.up.sql; do cat "$$f"; printf '\n;\n'; done \
	  | $(PSQL)

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

sample-db-lemmy: PGOPTS = -c search_path=lemmy
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
	  | $(PSQL)
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
# so check_function_bodies is turned off as it is for coder.
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

sample-db-windmill: PGOPTS = -c search_path=windmill,extensions -c check_function_bodies=off
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
	  | $(PSQL)
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
# check_function_bodies is passed in instead. The
# `CREATE SCHEMA public` that opens the body is dropped as well, since reset-db
# has just created it.
#
# It needs PostGIS for three geography columns and pgvector for one vector
# column and the hnsw index over it; compose.yaml and the samples CI job install
# both for discourse, osm, inaturalist, and dhis2 already. pgcrypto, pg_trgm,
# and uuid-ossp are contrib.
sample-db-citizenlab: PGOPTS = -c search_path=citizenlab -c check_function_bodies=off
.PHONY: sample-db-citizenlab
sample-db-citizenlab:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS citizenlab'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^-- Name: /{body=1} body' \
	  | sed -E "/^CREATE SCHEMA public;\$$/d; /^SET search_path TO /,\$$d; s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g" \
	  | $(PSQL)

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
# it. Two of the foreign key names it writes run past the 63 character
# identifier limit, which the server truncates as it does wso2is's.
DOKPLOY_SHA = 853ca33659853093ee6a91af3719abdc4e1bae72

sample-db-dokploy: PGOPTS = -c search_path=dokploy
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
	  | $(PSQL)

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
# throughout.
HYPERSWITCH_SHA = 80d426974242eecedefed015b617202fbdcbdac1

sample-db-hyperswitch: PGOPTS = -c search_path=hyperswitch
.PHONY: sample-db-hyperswitch
sample-db-hyperswitch:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS hyperswitch'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/juspay/hyperswitch/tar.gz/$(HYPERSWITCH_SHA) \
	  | tar xz -C "$$dir" --strip-components=2 hyperswitch-$(HYPERSWITCH_SHA)/migrations && \
	cd "$$dir" && LC_ALL=C && \
	for f in */up.sql; do cat "$$f"; printf '\n;\n'; done \
	  | $(PSQL)

# Penpot (penpot/penpot, MPL-2.0), the design and prototyping platform. Its
# schema ships as 165 SQL migration files in one directory, so the repository
# tarball is fetched once and that directory is extracted along with the
# migrations.clj beside it.
#
# Which files to replay, and in which order, comes from migrations.clj rather
# than from the directory listing, because the two do not agree. Three files on
# disk are not in the list, so Penpot never applies them, and replaying
# XXXX-drop-obsolete-tables.sql alone would drop a table and three columns the
# sample is meant to carry. Nor is the list in name order: six files share their
# number with another and are listed the other way round, among them
# 0122-mod-file-table, which comes before the 0122-mod-file-data-fragment-table
# that sorts ahead of it. Two files end without a trailing newline, which would
# run the next file's first line into their last, so each is followed by a
# newline and a semicolon. The cat is guarded for the same reason the list is
# read at all: a name the list holds and the tarball does not would otherwise
# be skipped with nothing but a line on stderr, and the check would then run
# against a schema quietly missing that migration, which is what ON_ERROR_STOP
# keeps a failing statement from doing.
#
# The list also holds two migrations written in Clojure, which Penpot runs in
# the same sequence. Both rewrite rows rather than schema, so the sample is
# checked without them, the way marquez is checked without Flyway's Java
# migrations.
#
# The list names each file as a resource path under `app/`, which the extracted
# directory is the tail of, so that prefix is cut and the rest is the path on
# disk. Nothing in the files names a schema or qualifies anything with public,
# so `penpot` is created up front and search_path places everything.
#
# The first migration installs uuid-ossp, which the migrations call 36 times as
# they replay and one column still defaults through at the end. It is contrib,
# so the official image already has it, but the migration says IF NOT EXISTS
# and names no schema: in `make schema`, where an earlier sample has already
# installed it into `public`, that is a no-op and uuid_generate_v4 would then
# not resolve from `penpot` alone. So it is installed into `public` up front,
# as dhis2 installs PostGIS, and `public` stays second in the search path for
# it to resolve from.
PENPOT_SHA = d642fcbf5c87c54c103f2561a545d7a85748c6de

sample-db-penpot: PGOPTS = -c search_path=penpot,public
.PHONY: sample-db-penpot
sample-db-penpot:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public'
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS penpot'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/penpot/penpot/tar.gz/$(PENPOT_SHA) \
	  | tar xz -C "$$dir" --strip-components=4 \
	      penpot-$(PENPOT_SHA)/backend/src/app/migrations penpot-$(PENPOT_SHA)/backend/src/app/migrations.clj && \
	cd "$$dir" && \
	grep -oE 'app/migrations/sql/[^"]+\.sql' migrations.clj | sed 's#^app/##' \
	  | while read -r f; do cat "$$f" || exit 1; printf '\n;\n'; done \
	  | $(PSQL)

# dcm4chee-arc-light (dcm4che/dcm4chee-arc-light, MPL-2.0), the DICOM archive.
# Unlike most of the Java projects here it ships plain DDL rather than
# migrations, in three files that are concatenated in dependency order: the
# tables and their sequences, then the indexes over the foreign key columns,
# then the three case-insensitive ones, both of which need the tables. None of
# them names a schema, qualifies anything with public, or installs an
# extension, so `dcm4chee` is created up front and search_path places
# everything.
#
# The sequences are why it is here: 30 standalone ones, one per table that
# needs a surrogate key, declared apart from the columns that draw from them
# rather than through serial or identity. Only ranger, wso2apim, and wso2is
# bring more.
DCM4CHEE_SQL_FILES = create-psql.sql create-fk-index.sql create-case-insensitive-index.sql

sample-db-dcm4chee: PGOPTS = -c search_path=dcm4chee
.PHONY: sample-db-dcm4chee
sample-db-dcm4chee:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS dcm4chee'
	for f in $(DCM4CHEE_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/dcm4che/dcm4chee-arc-light/09a7bb64080d6b76294cfda3e6807fc6faedc7a0/dcm4chee-arc-entity/src/main/resources/sql/psql/$$f || exit 1; \
	  echo; \
	done | $(PSQL)

# Kamailio (kamailio/kamailio, GPL-2.0), the SIP server. Its schema is one file
# per module rather than one file per release, and which modules a database
# gets is a choice the installer makes: kamdbctl creates the standard set
# always and asks about the presence, extra, and uid sets. The list below is
# all four, in the order kamdbctl.base lists them, which puts `standard` first
# because every file, itself included, writes a row into the `version` table it
# creates. The five left over are four IMS ones and matrix, which kamdbctl
# does not offer at all.
#
# None of them names a schema, qualifies anything with public, or installs an
# extension, so `kamailio` is created up front and search_path places
# everything. That matters more here than usual: this schema is where the
# generic names live, `domain`, `group`, `location`, `subscriber`, `uri`,
# `address`, `version`, and loading it into `public` would put them on top of
# half the other public samples in `make schema`.
KAMAILIO_MODULES = \
	standard acc lcr domain group \
	permissions registrar usrloc msilo alias_db uri_db speeddial \
	avpops auth_db pdt dialog dispatcher dialplan topos \
	presence rls \
	imc cpl siptrace domainpolicy carrierroute \
	drouting userblocklist htable purple uac pipelimit mtree sca mohqueue \
	rtpproxy rtpengine secfilter ims_icscf \
	uid_auth_db uid_avp_db uid_domain uid_gflags uid_uri_db

sample-db-kamailio: PGOPTS = -c search_path=kamailio
.PHONY: sample-db-kamailio
sample-db-kamailio:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS kamailio'
	for m in $(KAMAILIO_MODULES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/kamailio/kamailio/72acbabee92122e24fc13a5c2f09228450da7263/utils/kamctl/postgres/$$m-create.sql || exit 1; \
	  echo; \
	done | $(PSQL)

# Alfresco Content Services (Alfresco/alfresco-community-repo, LGPL-3.0), the
# ECM. Its schema ships as 11 create scripts, one per subsystem, and the order
# they run in is not their name order but the list in db-schema-context.xml,
# which is repeated below: the repository tables first, since the rest key back
# into them, and the authorization tables last.
#
# Alfresco's own runner rewrites each script twice before running it, and the
# sed does the same two things. It replaces $${TRUE} with TRUE on a dialect that
# has a boolean type, which SchemaBootstrap does for PostgreSQL and which 8
# rows of bootstrap data here need. And it carries on past a statement marked
# --(optional): there is exactly one, a DROP TABLE that only means anything
# when an upgrade left the table behind, so on an empty database it can only
# fail and is dropped. The three statements marked -- (optional), with a space,
# create a sequence, an index, and a table, and they stay.
#
# The scripts are CRLF, which psql reads as whitespace, so only the annotation
# match has to allow for the carriage return before end of line.
#
# None of them names a schema, qualifies anything with public, or installs an
# extension, so `alfresco` is created up front and search_path places
# everything. Like dcm4chee it keeps its sequences apart from the columns that
# draw from them: 38 of them, and not one column default calls nextval.
ALFRESCO_SHA = b03db39742d872ee7e58951b7c7f7d98bb7cd643
ALFRESCO_SCRIPTS = RepoTables LockTables ContentTables PropertyValueTables \
	ContentUrlEncryptionTables AuditTables ActivityTables UsageTables \
	SubscriptionTables TenantTables AuthorizationTables

sample-db-alfresco: PGOPTS = -c search_path=alfresco
.PHONY: sample-db-alfresco
sample-db-alfresco:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS alfresco'
	for s in $(ALFRESCO_SCRIPTS); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/Alfresco/alfresco-community-repo/$(ALFRESCO_SHA)/repository/src/main/resources/alfresco/dbscripts/create/org.alfresco.repo.domain.dialect.PostgreSQLDialect/AlfrescoCreate-$$s.sql || exit 1; \
	  echo; \
	done \
	  | sed -E 's/\$$\{TRUE\}/TRUE/g; /--\(optional\)[[:space:]]*$$/d' \
	  | $(PSQL)

# Apache ShenYu (apache/shenyu, Apache-2.0), the API gateway. Its schema ships
# as one file, which loads like the sample-db-url-schema dumps but qualifies
# every name in it with "public", sequences and the DEFAULT nextval that reads
# them included. So the qualifier is stripped the way sample-db-prisma strips
# it and search_path places everything. That is not only about where the
# objects land: the file opens each table with DROP TABLE IF EXISTS
# "public"."<name>", and in `make schema`, where every sample shares one
# database, several of those names belong to another sample.
#
# What comes with it is comments. ShenYu comments 360 of its 391 columns and 6
# of its 45 tables, a denser share than any other sample, and leaves 22 of
# those tables without a primary key and all 45 without a foreign key.
SHENYU_SHA = cd514ff292d6e85f299ae3a549544f6fa2d04936

sample-db-shenyu: PGOPTS = -c search_path=shenyu
.PHONY: sample-db-shenyu
sample-db-shenyu:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS shenyu'
	curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/apache/shenyu/$(SHENYU_SHA)/db/init/pg/create-table.sql \
	  | sed 's/"public"\.//g' \
	  | $(PSQL)

# OpenReplay (openreplay/openreplay, ELv2), the session replay platform. Its
# schema is one file that qualifies almost everything it creates with `public`,
# so the qualifier is stripped the way sample-db-pgdump-schema strips it and
# search_path places the rest. The four schemas it names itself, events,
# events_common, events_ios, and spots, it creates itself, so the sample is
# checked with all five.
#
# It opens by asking whether `public.tenants` is already there and quitting
# with \q when it is, which is how OpenReplay refuses to re-run over an
# installed database. Here the tables land in the sample's own schema, so the
# lookup is scoped to it, the way windmill's and lemmy's catalog lookups are:
# left naming `public` it would be asking about a schema this sample never
# writes to, and in `make schema` it could match another sample's table and
# quietly load nothing. The `SET client_min_messages TO NOTICE` on the second
# line is dropped for the same reason every loader raises the level to warning.
#
# It installs pg_trgm, which all 54 of its gin indexes name gin_trgm_ops from,
# and pgcrypto. Both are contrib, so the official image has them, but the file says
# IF NOT EXISTS and names no schema: in `make schema`, where an earlier sample
# has already installed one into `public`, that is a no-op and the operator
# class would then not resolve from `openreplay` alone. So both are installed
# into `public` up front, as penpot installs uuid-ossp, and `public` stays
# second in the search path for them to resolve from.
#
# The identity columns are why it is here: 19 of its 62 tables draw their
# surrogate key from a GENERATED BY DEFAULT AS IDENTITY column rather than from
# a serial or a standalone sequence.
sample-db-openreplay: PGOPTS = -c search_path=$(SCHEMA),public
.PHONY: sample-db-openreplay
sample-db-openreplay:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public'
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public'
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | sed -E "/^SET client_min_messages TO /d; s/table_schema = 'public'/table_schema = current_schema()/; s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g" \
	  | $(PSQL)

# Logto (logto-io/logto, MPL-2.0), the OIDC identity provider. Its schema is
# one file per table under packages/schemas/tables, which Logto's CLI seeds
# rather than replaying migrations, so the repository tarball is fetched once
# and that directory is extracted, along with the model file beside it that
# holds the one table the directory does not.
#
# Row-level security is why it is here. Logto is multi-tenant on one schema, so
# seed/tables.ts follows every table it creates with _after_each.sql, which
# turns RLS on and writes two policies over it, one of them RESTRICTIVE. That
# leaves 153 policies over 77 tables, and windmill, the only other sample with
# a policy at all, declares none that are restrictive.
#
# The order tables load in is the one compareQuery sorts them into: the files
# that carry an `init_order` comment first, by that number, then the rest by
# name. So the number is cut out of each file and sorted on its own, and a file
# without one is given a number past every real order. tenants comes first,
# with order 0, and it is the table Logto keeps in packages/schemas/src/models
# rather than in tables/, as a template literal in a TypeScript file, so the
# SQL is cut out of the literal and the one placeholder in it is substituted
# with the tag Logto's own enum gives it. Every other table keys back into
# tenants, so it has to be there first. A few files end without a semicolon,
# so each is followed by a newline and one.
#
# _after_each.sql is emitted after every file that does not say
# `/* no_after_each */`, with ${name} substituted, which is what the CLI does
# with it. _before_all.sql is not run: all it does is create the role the
# grants below name, and roles are out of pistachio's scope, so the grant and
# revoke statements in _after_all.sql are dropped instead and the two
# statements around them, the ones that turn RLS on for tenants and write its
# policy, are kept.
#
# None of the files creates a schema, so `logto` is created up front and
# search_path places everything. The handful that qualify a table or a function
# with `public` have the qualifier stripped, and the two functions declared
# `set search_path = public` are pointed at the sample's schema instead, since
# that is the schema Logto means: its own tables.
LOGTO_SHA = ffe0c650c0752f80ed300325ec1cbe7a3160ffe3
LOGTO_PACKAGE = packages/schemas

sample-db-logto: PGOPTS = -c search_path=logto
.PHONY: sample-db-logto
sample-db-logto:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS logto'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/logto-io/logto/tar.gz/$(LOGTO_SHA) \
	  | tar xz -C "$$dir" --strip-components=3 \
	      logto-$(LOGTO_SHA)/$(LOGTO_PACKAGE)/tables logto-$(LOGTO_SHA)/$(LOGTO_PACKAGE)/src/models/tenants.ts && \
	cd "$$dir" && LC_ALL=C && \
	awk '/\/\* Sql \*\/ `/ { f = 1; next } f && /^`,/ { f = 0 } f' src/models/tenants.ts \
	  | sed 's/$${TenantTag\.Development}/development/' > tables/tenants.sql && \
	cd tables && \
	{ for f in *.sql; do \
	    case "$$f" in _before_all.sql|_after_all.sql|_after_each.sql) continue ;; esac; \
	    o=$$(sed -n 's,.*/\* *init_order *= *\([0-9.]*\) *\*/.*,\1,p' "$$f" | head -1); \
	    printf '%s\t%s\n' "$${o:-999999}" "$$f"; \
	  done | sort -k1,1g -k2,2 | cut -f2 \
	    | while read -r f; do \
	        cat "$$f"; printf '\n;\n'; \
	        grep -q no_after_each "$$f" || sed "s/\$${name}/$${f%.sql}/g" _after_each.sql; \
	      done; \
	  awk '/^(grant|revoke)/ { skip = 1 } skip { if (/;[[:space:]]*$$/) skip = 0; next } { print }' _after_all.sql; } \
	  | sed -E "s/^public\.//; s/([^A-Za-z0-9_])public\./\1/g; s/set search_path = public/set search_path = logto/" \
	  | $(PSQL)

# OMERO (ome/openmicroscopy, GPL-2.0), the OME platform for microscopy image
# data. A fresh database is the four files `omero db script` concatenates, in
# that order: psql-header.sql, which opens the transaction and declares the
# domains and the unit enums, schema.sql, the tables Hibernate generates,
# views.sql, and psql-footer.sql, which adds the indexes, the functions, and
# the triggers and commits. The loader repeats that.
#
# The sequences are why it is here. OMERO gives 129 of its 161 tables a
# sequence named after it and asks for the next value in the application, so
# its 130 standalone sequences are more than any other sample declares and
# only 2 of them are named in a column DEFAULT. It brings 130 triggers as
# well, 4 of them deferrable constraint triggers, and 7 enums whose 157 labels
# are unit symbols, so past ASCII.
#
# `omero db script` renders the header and the footer through Python's %
# formatting, which is why every literal percent sign in them is written twice;
# undoubling them is the whole substitution, since the header's %(TIME)s and
# the rest of its placeholders sit in comments. The footer's one @ROOTPASS@
# goes into a row of the password table rather than into the schema, so it
# loads as the literal string. Nothing in the files names a schema or an
# extension, so `omero` is created up front and search_path places everything.
OMERO_SHA = be0fd3d0efd5f419c2c373779e50107b5f1b6b8f
OMERO_DIR = sql/psql/OMERO5.4__0
OMERO_SQL_FILES = psql-header.sql schema.sql views.sql psql-footer.sql

sample-db-omero: PGOPTS = -c search_path=omero
.PHONY: sample-db-omero
sample-db-omero:
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS omero'
	for f in $(OMERO_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/ome/openmicroscopy/$(OMERO_SHA)/$(OMERO_DIR)/$$f || exit 1; \
	  echo; \
	done | sed 's/%%/%/g' | $(PSQL)

# Concourse (concourse/concourse, Apache-2.0), the CI server. The schema ships
# as golang-migrate migrations, 151 `.up.sql` files replayed in name order like
# mattermost's, so the repository tarball is fetched once and only the
# migrations directory is extracted. Seven migrations beside them are Go rather
# than SQL, replayed in the same sequence. Three of those only rewrite rows and
# four change a table: they drop `teams.basic_auth`, rename `teams.auth` to
# `legacy_auth` and add a new `auth`, and add `resources.type` and
# `resource_pins.config`. Nothing in the SQL files depends on any of that, so
# the sample is checked without them, the way marquez is checked without its
# Java migrations' views, and its `teams` keeps the `basic_auth` column
# upstream drops.
#
# The index storage parameters are why it is here: its two gin indexes are
# declared `WITH (FASTUPDATE = false)` and name `jsonb_path_ops`, so its dump
# is where a storage parameter on an index has to survive the round trip. A few
# files end without a semicolon, so each is followed by a newline and one.
#
# None of the files names a schema, so `concourse` is created up front and
# search_path places everything. One migration installs pgcrypto and hashes
# rows with `digest()` in the same file, so the extension has to be resolvable
# from the search path when that statement runs: it is contrib, so it is
# installed into `public` up front the way openreplay's is, and `public` stays
# second in the search path.
CONCOURSE_SHA = a3484bc8cc3655a59583527667708d6b8fdd8e50

sample-db-concourse: PGOPTS = -c search_path=concourse,public
.PHONY: sample-db-concourse
sample-db-concourse:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public'
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS concourse'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/concourse/concourse/tar.gz/$(CONCOURSE_SHA) \
	  | tar xz -C "$$dir" --strip-components=5 concourse-$(CONCOURSE_SHA)/atc/db/migration/migrations && \
	cd "$$dir" && LC_ALL=C && \
	for f in *.up.sql; do cat "$$f"; printf '\n;\n'; done \
	  | $(PSQL)

# AFFiNE (toeverything/AFFiNE, MIT), the workspace editor. The schema ships as
# a Prisma migration history, 123 directories each holding a migration.sql,
# replayed in name order into a schema of its own; sample-db-prisma does that
# for four other samples, but this one needs two extensions in the search path
# as well, so it has a target of its own.
#
# What it writes by hand is why it is here. Prisma declares no CHECK constraint
# and no trigger of its own, and AFFiNE's migrations add 54 CHECKs over its 72
# tables and 16 triggers over 11 of them, 4 of those firing `BEFORE UPDATE OF`
# a column list, one of them four columns long. It carries 11 enums, 2 hnsw
# indexes, and no sequence at all beside them.
#
# The extensions are pgvector, for the two `vector(1024)` columns those hnsw
# indexes cover, and pgcrypto, which a migration hashes existing rows with in
# the same file that installs it, so it has to be resolvable from the search
# path when that statement runs. Both are installed into `public` up front the
# way openreplay's are, with `public` second in the search path, since the
# `CREATE EXTENSION IF NOT EXISTS` the migrations write names no schema and
# gets nothing when another sample already installed it somewhere else.
AFFINE_SHA = d897bb3d84099e54a6b3c0bd5f4265f8aa87d190
AFFINE_DIR = packages/backend/server/migrations

sample-db-affine: PGOPTS = -c search_path=affine,public
.PHONY: sample-db-affine
sample-db-affine:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public'
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public'
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS affine'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/toeverything/AFFiNE/tar.gz/$(AFFINE_SHA) \
	  | tar xz -C "$$dir" --strip-components=5 AFFiNE-$(AFFINE_SHA)/$(AFFINE_DIR) && \
	cd "$$dir" && LC_ALL=C && \
	for f in */migration.sql; do cat "$$f"; printf '\n;\n'; done \
	  | sed -E 's/"public"\.//g; s/([^A-Za-z0-9_])public\./\1/g' \
	  | $(PSQL)

# Uyuni (uyuni-project/uyuni, GPL-2.0), the systems management server SUSE
# Manager is built from and the project Red Hat's Spacewalk became. Its schema
# is not a file but a source tree that a build step turns into one:
# `schema/spacewalk/common` holds a file per table, view, and reference data
# load shared with the Oracle port it came from, `schema/spacewalk/postgres` the
# PostgreSQL side -- the enums, the functions, the triggers, and one schema per
# Oracle package it rewrote -- and beside them a `.deps` file per directory
# saying what has to come first. `blend`, the Python tool in the tree, reads
# those and writes `main.sql`. The loader runs that build rather than
# reimplementing the order, so it is the one sample that needs GNU make and
# python3 as well as curl and psql. That inner make runs with MAKEFLAGS cleared
# and is not written as $(MAKE), so the flags this make was given, -n among
# them, reach neither it nor the recipe around it. It runs with PYTHONWARNINGS
# set as well: two of blend's string literals are not raw ones, `"[,\s]+"` and
# `"\i"`, and neither escape is a Python escape, which 3.12 turned from a
# deprecation into a SyntaxWarning it prints by default. Upstream silences the
# pylint check over each of them rather than the warning, so the build says it
# twice on a current python3 and the runner passes it through. Only
# SyntaxWarning is filtered; anything else the build says still comes through.
# The repository tarball is fetched once and only the schema tree and one file
# from the container image come out of it.
#
# Its size is why it is here: 433 tables, 2,614 columns, 935 indexes, and 692
# foreign keys put it among the five largest samples by each of them, and its
# 412 routines and 207 standalone sequences are more than any other sample
# declares, 224 of the routines sitting behind a trigger. It brings 21 identity
# columns beside openreplay's and 13 schemas, more than any other sample is
# checked with.
#
# One file in the tree is a template rather than SQL, common/data's
# rhnVersionInfo.pre, which Makefile.schema fills in with the schema name,
# version, and release before the build runs. blend stops when it is missing, so
# the loader substitutes it the same way; the values only reach a row.
#
# evr_t, the composite type rhnPackageEVR.evr is declared with, is not in the
# tree at all: the server container's entrypoint creates it, along with the
# functions, operators, and operator class that compare two of them. So the SQL
# in that script runs first, taken out of the heredoc around it with the shell's
# escaping undone. Two of its functions call rpm.vercmp and deb.debvercmp, which
# SUSE ships as C extensions; both bodies are plpgsql, so nothing resolves them
# when the function is created and nothing the check runs calls them.
#
# The tree names no schema, so `uyuni` is created up front and search_path
# places everything. It creates twelve more itself, `access` for the RBAC tables
# and one per package it ported, so the sample is checked with all thirteen. Two
# REFERENCES qualify `public`, which is where upstream installs, so the
# qualifier is stripped as sample-db-pgdump-schema strips it. pg_trgm, which the
# two indexes that name gin_trgm_ops need -- the third gin index is over
# to_tsvector and takes the built-in tsvector_ops -- is installed into `public`
# up front the way concourse's pgcrypto is, and the tree's own
# `CREATE EXTENSION pg_trgm` is dropped: it names no schema and no
# IF NOT EXISTS, so it stops the load in `make schema` once another sample has
# installed it. `public` stays second in the search path for the operator class
# to resolve from.
#
# The last thing the build appends is end.sql, which walks the catalog and puts
# a CHECK constraint on every varchar column, 635 of them here, rejecting the
# empty string Oracle would have read as NULL. It takes the tables current_user
# owns that search_path can see, which upstream is Uyuni's alone; here `public`
# is in the search path too, so in `make schema` it would reach every sample
# loaded into it. The lookup is scoped to the sample's schema instead, as
# sample-db-lemmy scopes its own catalog lookups. Every `commit` in the
# reference data loads warns that there is no transaction in progress, so the
# sample raises client_min_messages to error from its SAMPLES record, as ranger
# does.
UYUNI_SHA = fbd7328c459b607a47054793dd9b9a898399fad0
UYUNI_EVR_T = containers/server-postgresql-image/root/docker-entrypoint-upgdb.d/zz-evr_t.sh

sample-db-uyuni: PGOPTS = -c search_path=uyuni,public
.PHONY: sample-db-uyuni
sample-db-uyuni:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public'
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS uyuni'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/uyuni-project/uyuni/tar.gz/$(UYUNI_SHA) \
	  | tar xz -C "$$dir" --strip-components=1 \
	      uyuni-$(UYUNI_SHA)/schema/spacewalk uyuni-$(UYUNI_SHA)/$(UYUNI_EVR_T) && \
	cd "$$dir" && \
	sed -n '/^cat << EOF | run_sql$$/,/^EOF$$/p' $(UYUNI_EVR_T) \
	  | sed '1d; $$d; s/\\\$$/$$/g' \
	  | $(PSQL) && \
	sed -e "s!SCHEMA_NAME!'uyuni'!g" -e "s!SCHEMA_VERSION!'0'!g" -e "s!SCHEMA_RELEASE!'0'!" \
	  schema/spacewalk/common/data/rhnVersionInfo.pre > schema/spacewalk/common/data/rhnVersionInfo.sql && \
	MAKEFLAGS= PYTHONWARNINGS=ignore::SyntaxWarning \
	  make -C schema/spacewalk/postgres main >/dev/null && \
	sed -E "/^CREATE EXTENSION pg_trgm;\$$/d; s/([^A-Za-z0-9_])public\./\1/g; \
	        s/pg_catalog\.pg_table_is_visible\(c\.oid\)/c.relnamespace = 'uyuni'::regnamespace/" \
	  schema/spacewalk/postgres/main.sql \
	  | $(PSQL)

# LobeHub (lobehub/lobehub, Apache-2.0), the agent workspace built on the LLM
# providers, which was LobeChat before it was renamed; codeload names the
# archive's top directory after the repository as it is now, so both the URL
# and the prefix say lobehub. The schema ships as Drizzle migrations like
# dokploy's and loads the same way: the repository tarball is fetched once,
# only the migrations directory is extracted, and the tags are read out of
# meta/_journal.json rather than off the directory, since here too the two do
# not agree. 0065_add_document_fields.sql is on disk but not in the journal,
# left behind by the rename that made it 0066_add_document_fields.sql, and
# the two differ in the ON DELETE action of the foreign key they add, so
# replaying the directory would take the older one. A few files end without a
# semicolon, so each is followed by a newline and one, and the `public`
# qualifier Drizzle writes into a foreign key's target is stripped the way
# sample-db-prisma strips it, along with the one the `to_regclass` and
# `::regclass` guards in a handful of hand-written migrations carry, which
# leaves those guards reading through search_path.
#
# Twelve guards look a constraint up by name alone, `SELECT 1 FROM
# pg_constraint WHERE conname = '<name>'`, and skip the ALTER TABLE that
# follows when they find one. Upstream LobeHub owns its database, but here
# the samples before it are still there, so a name another schema already
# uses -- `users_email_unique` is one -- would silently cost this sample a
# constraint. Those lookups are scoped to the sample's schema, the way
# lemmy's are.
#
# The vectors are why it is here, and the first of the two reasons it does not
# share dokploy's loader. LobeHub keeps what it remembers about a user as
# embeddings, so 11 of its columns are `vector(1024)` and 10 hnsw indexes sit
# over them, naming `vector_cosine_ops` unqualified the way affine's two do;
# citizenlab and affine bring 3 hnsw indexes between them and one `vector`
# column each. pgvector is installed into `public` up front the way affine's
# is, with `public` second in the search path, since the `CREATE EXTENSION IF
# NOT EXISTS vector` a migration writes names no schema and gets nothing when
# another sample already installed it somewhere else.
#
# pg_search is the second. It is ParadeDB's, not contrib, and the official
# image does not ship it, so the two migrations that need it are skipped: one
# installs it, the other writes 14 bm25 indexes with it. Nothing else in the
# history reads them.
#
# What it leaves out is the other half of why it is here: 182 tables and 972
# indexes with no view, no enum, no domain, no trigger, and no routine at all,
# and only 20 unique constraints, since Drizzle writes a bare CREATE UNIQUE
# INDEX instead and 138 of its 340 unique indexes stand on their own that way.
LOBEHUB_SHA = 14dfc07b14eee1984e52c195df9319636d6b167b

sample-db-lobehub: PGOPTS = -c search_path=lobehub,public
.PHONY: sample-db-lobehub
sample-db-lobehub:
	$(PSQL) -c 'CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public'
	$(PSQL) -c 'CREATE SCHEMA IF NOT EXISTS lobehub'
	dir=$$(mktemp -d) && trap 'rm -rf "$$dir"' EXIT && \
	curl -sSfL --retry 3 --retry-delay 2 https://codeload.github.com/lobehub/lobehub/tar.gz/$(LOBEHUB_SHA) \
	  | tar xz -C "$$dir" --strip-components=4 lobehub-$(LOBEHUB_SHA)/packages/database/migrations && \
	cd "$$dir" && \
	grep -oE '"tag": "[^"]+"' meta/_journal.json | sed 's/.*: "//; s/"$$//' \
	  | while read -r t; do \
	      grep -qE 'pg_search|USING bm25' "$$t.sql" || { cat "$$t.sql"; printf '\n;\n'; }; \
	    done \
	  | sed -E "s/\"public\"\.//g; s/([^A-Za-z0-9_])public\./\1/g; \
	            s/WHERE conname = /WHERE connamespace = 'lobehub'::regnamespace AND conname = /g" \
	  | $(PSQL)

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
