# The server every psql- and test-based target talks to. compose.yaml
# publishes each PostgreSQL version of the CI matrix on its own port, so all
# four can run side by side and PGPORT picks which one is used: 5415 for 15,
# 5416 for 16, 5417 for 17, 5418 for 18. Any other port works too (e.g.
# PGPORT=5432 for a local install), and TEST_PISTA_CONN_STR bypasses both.
export PGHOST := localhost
export PGUSER := postgres
export PGPORT ?= 5415

TEST_PISTA_CONN_STR ?= postgres://postgres@localhost:$(PGPORT)/postgres
export TEST_PISTA_CONN_STR

# Ensure failures in any stage of a piped recipe (e.g. curl | awk | psql)
# fail the target. Default /bin/sh on most systems lacks pipefail, so a
# failing curl can be masked by a successful psql. -o pipefail must be
# on SHELL itself (not .SHELLFLAGS) so make passes it as a separate arg.
SHELL := /bin/bash -o pipefail
.SHELLFLAGS := -c

# The sample schema loaders, the sample manifest, and the database wipe used
# around them. Included after `all` so that it stays the default goal.
.PHONY: all
all: vet test build

include sample-db.mk

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

.PHONY: test-scenario
test-scenario:
	bash test/scenario/run.sh

.PHONY: demo
demo: clean-schema
	vhs demo.tape
