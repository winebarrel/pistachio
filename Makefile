export PGHOST := localhost
export PGUSER := postgres

.PHONY: all
all: vet test build

.PHONY: build
build:
	go build ./cmd/pist

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

.PHONY: sample-db
sample-db:
	curl -sSf https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/refs/heads/main/$(SQL_FILE) | psql

.PHONY: sample-db-tar
sample-db-tar:
	curl -sSfL $(TAR_URL) | tar xzO $(TAR_SQL_PATH) | psql

.PHONY: clean-schema
clean-schema:
	psql -c 'DROP SCHEMA public CASCADE ; CREATE SCHEMA public'

.PHONY: demo
demo: clean-schema
	vhs demo.tape
