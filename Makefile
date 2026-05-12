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
	go install .

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

.PHONY: schema
schema: clean-schema
	$(MAKE) sample-db SQL_FILE=chinook.sql
	$(MAKE) sample-db SQL_FILE=happiness_index.sql
	$(MAKE) sample-db SQL_FILE=lego.sql
	$(MAKE) sample-db SQL_FILE=netflix.sql
	$(MAKE) sample-db SQL_FILE=pagila.sql
	$(MAKE) sample-db SQL_FILE=periodic_table.sql
	$(MAKE) sample-db SQL_FILE=titanic.sql
	$(MAKE) sample-db-tar TAR_URL=https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/world/world-1.0/world-1.0.tar.gz TAR_SQL_PATH=dbsamples-0.1/world/world.sql
	$(MAKE) sample-db-tar TAR_URL=https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/usda/usda-r18-1.0/usda-r18-1.0.tar.gz TAR_SQL_PATH=usda-r18-1.0/usda.sql
	$(MAKE) sample-db-url URL=https://raw.githubusercontent.com/pthom/northwind_psql/master/northwind.sql
	$(MAKE) sample-db-url URL=https://raw.githubusercontent.com/h8/employees-database/master/employees_schema.sql
	$(MAKE) sample-db-adventureworks

.PHONY: sample-db
sample-db:
	curl -sSf https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/refs/heads/main/$(SQL_FILE) | psql

.PHONY: sample-db-tar
sample-db-tar:
	curl -sSfL $(TAR_URL) | tar xzO $(TAR_SQL_PATH) | psql

.PHONY: sample-db-url
sample-db-url:
	curl -sSfL $(URL) | psql

# AdventureWorks (lorint/AdventureWorks-for-Postgres, MIT). Schema-only load:
# strip \copy lines (data lives in CSVs we don't fetch) and the inline
# Production.ProductReview INSERT (FK target rows aren't loaded).
.PHONY: sample-db-adventureworks
sample-db-adventureworks:
	curl -sSfL https://raw.githubusercontent.com/lorint/AdventureWorks-for-Postgres/master/install.sql \
	  | awk '/^\\copy/ { next } /^INSERT INTO Production.ProductReview/ { skip=1 } skip { if (/\);[[:space:]]*$$/) skip=0; next } { print }' \
	  | psql

.PHONY: test-scenario
test-scenario:
	bash test/scenario/run.sh

.PHONY: clean-schema
clean-schema:
	psql -c 'DROP SCHEMA IF EXISTS person, humanresources, production, purchasing, sales, employees CASCADE ; DROP SCHEMA public CASCADE ; CREATE SCHEMA public'

.PHONY: demo
demo: clean-schema
	vhs demo.tape
