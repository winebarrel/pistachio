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

# TEST_JUNIT names a file to write a JUnit XML report to, which is what
# Codecov Test Analytics ingests. Leave it empty and the report is skipped;
# `go test` then writes straight to the terminal. Set, the output goes through
# go-junit-report, which -iocopy passes on to the terminal unchanged.
#
# -set-exit-code is what carries a failure out of the pipeline: make on Windows
# does not use the SHELL set above, so pipefail is not in effect there and the
# pipeline would otherwise report go-junit-report's own status. It covers a
# failed test, a package that fails as a whole, and a build failure.
ifneq ($(TEST_JUNIT),)
TEST_REPORT = | go tool go-junit-report -iocopy -set-exit-code -out $(TEST_JUNIT)
endif

# The Go tests reset `public` and nothing else, so whatever `make schema`
# loaded into the other schemas is still there while they run. A test that
# looks something up without naming its schema then finds a sample's object
# instead of its own. CI runs against an empty database and never sees it, so
# wipe first and let a local run start from the same place.
#
# clean-schema goes through PGHOST/PGUSER/PGPORT, so overriding
# TEST_PISTA_CONN_STR alone points the tests at one server and the wipe at
# another. Override PGPORT, or wipe the other server yourself.
.PHONY: test
test: clean-schema
	go test -p 1 -v ./... $(TEST_OPTS) $(TEST_REPORT)

.PHONY: lint
lint:
	golangci-lint run

.PHONY: fix
fix:
	golangci-lint run --fix

.PHONY: deadcode
deadcode:
	bash scripts/check-deadcode.sh

# Fuzzing. The targets need no database, so this is the one test target that
# runs anywhere. FUZZTIME is per target, not for the run as a whole, and each
# has to be started on its own because `go test -fuzz` takes one target at a
# time. A crash is written under the package's testdata/fuzz/, where it turns
# into a seed the ordinary `go test` replays from then on.
FUZZTIME ?= 1m

.PHONY: fuzz
fuzz:
	go test ./parser/ -run='^$$' -fuzz=FuzzParseSQLWithSchema -fuzztime=$(FUZZTIME)
	go test ./diff/ -run='^$$' -fuzz=FuzzDiffSelf -fuzztime=$(FUZZTIME)

.PHONY: keywords
keywords:
	bash scripts/gen-keywords.sh

# Cleaned for the same reason as `test`: run.sh resets `public` per scenario
# and leaves every other schema alone.
.PHONY: test-scenario
test-scenario: clean-schema
	bash test/scenario/run.sh

# Cleaned for the same reason as `test`: run.sh resets `public` per schema and
# leaves every other schema alone.
.PHONY: test-fidelity
test-fidelity: clean-schema
	bash test/fidelity/run.sh

.PHONY: demo
demo: clean-schema
	vhs demo.tape
