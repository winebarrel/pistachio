export PGHOST := localhost
export PGUSER := postgres

# Ensure failures in any stage of a piped recipe (e.g. curl | awk | psql)
# fail the target. Default /bin/sh on most systems lacks pipefail, so a
# failing curl can be masked by a successful psql. -o pipefail must be
# on SHELL itself (not .SHELLFLAGS) so make passes it as a separate arg.
SHELL := /bin/bash -o pipefail
.SHELLFLAGS := -c

.PHONY: all
all: vet test build

.PHONY: build
build:
	go build ./cmd/pista

.PHONY: install
install:
	go install ./cmd/pista

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test -p 1 -v ./... $(TEST_OPTS)

.PHONY: lint
lint:
	golangci-lint run

.PHONY: fix
fix:
	golangci-lint run --fix

.PHONY: deadcode
deadcode:
	bash scripts/check-deadcode.sh

.PHONY: keywords
keywords:
	bash scripts/gen-keywords.sh

# Single source of the real-world sample schemas, consumed by both `schema`
# (loads them all into one database) and `test-samples` (loads each into an
# isolated database and checks pista round-trips it). One record per line:
#   name | loader-target | VAR=value ... | schemas for pista -n (blank = public)
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
northwind|sample-db-url|URL=https://raw.githubusercontent.com/pthom/northwind_psql/master/northwind.sql|
employees|sample-db-url|URL=https://raw.githubusercontent.com/h8/employees-database/master/employees_schema.sql|employees
mimiciv|sample-db-mimiciv||mimiciv_hosp,mimiciv_icu
mediawiki|sample-db-url-schema|URL=https://raw.githubusercontent.com/wikimedia/mediawiki/master/sql/postgres/tables-generated.sql SCHEMA=mediawiki|mediawiki
synapse|sample-db-url-schema|URL=https://raw.githubusercontent.com/element-hq/synapse/develop/synapse/storage/schema/main/full_schemas/72/full.sql.postgres SCHEMA=synapse|synapse
temporal|sample-db-url-schema|URL=https://raw.githubusercontent.com/temporalio/temporal/main/schema/postgresql/v12/temporal/schema.sql SCHEMA=temporal|temporal
imdb|sample-db-imdb||
adventureworks|sample-db-adventureworks||person,humanresources,production,purchasing,sales
clubdata|sample-db-clubdata|URL=https://pgexercises.com/dbfiles/clubdata.sql|cd
demodb|sample-db-demodb|URL=https://raw.githubusercontent.com/postgrespro/demodb/master/tables.sql|bookings
musicbrainz|sample-db-musicbrainz||musicbrainz
endef

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

.PHONY: sample-db
sample-db:
	curl -sSf --retry 3 --retry-delay 2 https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/refs/heads/main/$(SQL_FILE) | psql

.PHONY: sample-db-tar
sample-db-tar:
	curl -sSfL --retry 3 --retry-delay 2 $(TAR_URL) | tar xzO $(TAR_SQL_PATH) | psql

.PHONY: sample-db-url
sample-db-url:
	curl -sSfL --retry 3 --retry-delay 2 $(URL) | psql

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
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/MIT-LCP/mimic-code/main/mimic-iv/buildmimic/postgres/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c client_min_messages=warning' psql

# A plain SQL URL loaded into a schema of its own. These dumps name no schema at
# all, so search_path decides where they land, and upstream expects them in the
# search path's first schema. Loading them into `public` would collide with the
# other public samples when `make schema` puts everything in one database
# (mediawiki and pagila both define `actor` and `category`, for one), so each
# gets its own schema instead.
.PHONY: sample-db-url-schema
sample-db-url-schema:
	psql -c 'CREATE SCHEMA IF NOT EXISTS $(SCHEMA)'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) | PGOPTIONS='-c search_path=$(SCHEMA)' psql

# Join Order Benchmark (gregrahn/join-order-benchmark), the IMDB schema used by
# the JOB query set. The tables and their indexes ship as two separate files, so
# concatenate them: schema.sql first, then the foreign-key indexes.
.PHONY: sample-db-imdb
sample-db-imdb:
	for f in schema.sql fkindexes.sql; do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/gregrahn/join-order-benchmark/master/$$f || exit 1; \
	  echo; \
	done | psql

# AdventureWorks (lorint/AdventureWorks-for-Postgres, MIT). Schema-only load:
# strip \copy lines (data lives in CSVs we don't fetch) and the inline
# Production.ProductReview INSERT (FK target rows aren't loaded).
.PHONY: sample-db-adventureworks
sample-db-adventureworks:
	curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/lorint/AdventureWorks-for-Postgres/master/install.sql \
	  | awk '/^\\copy/ { next } /^INSERT INTO Production.ProductReview/ { skip=1 } skip { if (/\);[[:space:]]*$$/) skip=0; next } { print }' \
	  | psql

# PostgreSQL Exercises club data (pgexercises.com). The dump creates its own
# database and reconnects to it, which we cannot do mid-pipe; strip those two
# lines and load the rest into the current database. The remaining SQL creates
# the `cd` schema and sets search_path itself.
.PHONY: sample-db-clubdata
sample-db-clubdata:
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^CREATE DATABASE exercises;/ { next } /^\\c exercises/ { next } { print }' \
	  | psql

# Postgres Pro demo database (postgrespro/demodb, PostgreSQL License). The repo
# ships a schema-generation script, not a plain dump: tables.sql defines the
# `gen` and `bookings` schemas and \copy-loads reference data from .dat files we
# don't fetch. We only need the schema, so strip the \copy lines (their data is
# irrelevant to a round-trip check) and enable btree_gist first, which the
# bookings.routes exclusion constraint requires.
.PHONY: sample-db-demodb
sample-db-demodb:
	psql -c 'CREATE EXTENSION IF NOT EXISTS btree_gist'
	curl -sSfL --retry 3 --retry-delay 2 $(URL) \
	  | awk '/^[[:space:]]*\\copy/ { next } { print }' \
	  | psql

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
	psql -c 'CREATE SCHEMA IF NOT EXISTS musicbrainz'
	for f in $(MUSICBRAINZ_SQL_FILES); do \
	  curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/metabrainz/musicbrainz-server/master/admin/sql/$$f || exit 1; \
	  echo; \
	done | PGOPTIONS='-c search_path=musicbrainz,public -c client_min_messages=warning' psql

.PHONY: test-scenario
test-scenario:
	bash test/scenario/run.sh

.PHONY: test-samples
test-samples:
	bash test/samples/run.sh

# Every schema the samples create, plus public. Dropped one statement at a
# time: cascading through all of them in a single transaction runs the server
# out of lock table space, since musicbrainz alone owns ~1000 objects.
SAMPLE_SCHEMAS = \
	person humanresources production purchasing sales pe hr pr pu sa \
	employees bookings gen cd musicbrainz mediawiki synapse temporal \
	mimiciv_hosp mimiciv_icu mimiciv_derived public

.PHONY: clean-schema
clean-schema:
	for s in $(SAMPLE_SCHEMAS); do \
	  psql -q -c "SET client_min_messages TO warning; DROP SCHEMA IF EXISTS $$s CASCADE" || exit 1; \
	done
	psql -q -c 'CREATE SCHEMA public'

# Drop every user schema (not just public), so a prior sample's schemas can't
# leak into the next check. Used by test-samples between samples. One DROP per
# psql statement, for the same lock table reason as clean-schema.
.PHONY: reset-db
reset-db:
	psql -X -q -At -v ON_ERROR_STOP=1 -c "SELECT quote_ident(nspname) FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'" \
	  | while read -r s; do \
	      psql -X -q -v ON_ERROR_STOP=1 -c "SET client_min_messages TO warning; DROP SCHEMA IF EXISTS $$s CASCADE" || exit 1; \
	    done
	psql -X -q -v ON_ERROR_STOP=1 -c 'CREATE SCHEMA public'

.PHONY: demo
demo: clean-schema
	vhs demo.tape
