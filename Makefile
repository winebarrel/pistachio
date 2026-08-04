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
northwind|sample-db-url|URL=https://raw.githubusercontent.com/pthom/northwind_psql/master/northwind.sql|
employees|sample-db-url|URL=https://raw.githubusercontent.com/h8/employees-database/master/employees_schema.sql|employees
adventureworks|sample-db-adventureworks||person,humanresources,production,purchasing,sales
demodb|sample-db-demodb|URL=https://raw.githubusercontent.com/postgrespro/demodb/master/tables.sql|bookings
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

# AdventureWorks (lorint/AdventureWorks-for-Postgres, MIT). Schema-only load:
# strip \copy lines (data lives in CSVs we don't fetch) and the inline
# Production.ProductReview INSERT (FK target rows aren't loaded).
.PHONY: sample-db-adventureworks
sample-db-adventureworks:
	curl -sSfL --retry 3 --retry-delay 2 https://raw.githubusercontent.com/lorint/AdventureWorks-for-Postgres/master/install.sql \
	  | awk '/^\\copy/ { next } /^INSERT INTO Production.ProductReview/ { skip=1 } skip { if (/\);[[:space:]]*$$/) skip=0; next } { print }' \
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

.PHONY: test-scenario
test-scenario:
	bash test/scenario/run.sh

.PHONY: test-samples
test-samples:
	bash test/samples/run.sh

.PHONY: clean-schema
clean-schema:
	psql -c 'DROP SCHEMA IF EXISTS person, humanresources, production, purchasing, sales, employees, bookings, gen CASCADE ; DROP SCHEMA public CASCADE ; CREATE SCHEMA public'

# Drop every user schema (not just public), so a prior sample's schemas can't
# leak into the next check. Used by test-samples between samples.
.PHONY: reset-db
reset-db:
	psql -X -q -v ON_ERROR_STOP=1 -c "SET client_min_messages TO warning; DO \$$\$$ DECLARE s text; BEGIN FOR s IN SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema' LOOP EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', s); END LOOP; END \$$\$$; CREATE SCHEMA public"

.PHONY: demo
demo: clean-schema
	vhs demo.tape
