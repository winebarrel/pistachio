# CLAUDE.md

## Project overview

Pistachio is a declarative schema management tool for PostgreSQL, written in Go. It parses desired schema definitions from SQL files and generates DDL diffs to bring the current database in line with the desired state.

## Build & test

```sh
make build          # go build ./cmd/pist (outputs ./pist at the repo root)
make vet            # go vet ./...
make test           # go test -p 1 -v ./... $(TEST_OPTS)
make test-scenario  # CLI scenario tests (bash, requires PostgreSQL)
make lint           # golangci-lint run
make fix            # golangci-lint run --fix (auto-fix lint errors)
```

- Tests require a running PostgreSQL instance (default: `postgres://postgres@localhost/postgres`, override with `TEST_PIST_CONN_STR`).
- Tests run with `-p 1` (sequential packages) because integration tests share a single database.
- `make schema` and other `psql`-based targets rely on `PGHOST=localhost` / `PGUSER=postgres` (exported from the Makefile).

## Project structure

- `cmd/pist/` - CLI entrypoint
- `cmd/command/` - CLI command implementations
- `parser/` - SQL parser (uses pg_query_go to parse and deparse PostgreSQL SQL)
- `catalog/` - Reads current schema state from PostgreSQL system catalogs (`pg_catalog`)
- `model/` - Data model structs (Table, Column, Constraint, ForeignKey, Index, View, Enum, Domain)
- `diff/` - Generates DDL diff between current and desired schemas
- `internal/testutil/` - Test helpers (DB connection, setup)
- `testdata/` - YAML-based test fixtures for multiple test suites, including integration and unit tests
- `test/scenario/` - CLI-level scenario tests (shell scripts that run `pist` CLI against sample schemas)

## Development workflow

1. Create a feature branch before starting implementation.
2. Write tests that assert the expected behavior first, confirm they fail, then implement the fix/feature.
3. Prefer simplicity — avoid complex implementations when a straightforward approach works.
4. After implementation:
   - Verify test cases are comprehensive (check for missing scenarios and edge cases).
   - Verify coverage has not decreased and cover any reachable paths that can be tested naturally (do not write unnatural tests for unreachable defensive code).
   - Consider whether similar issues exist elsewhere in the codebase.
   - Run `make lint` to check for lint errors.
   - Run `make schema` to load sample schema SQL files into the local database (requires `psql`, `curl`, and network access), then verify behavior with `pist plan` / `pist dump` against them.
5. Do not run tests in parallel (`make test` uses `-p 1`).

## Code conventions

- Package-level tests generally use external test packages (e.g., `package catalog_test`, `package model_test`). Use same-package tests only when access to unexported identifiers is required (e.g., `package diff`).
- Root-level integration tests use `package pistachio_test`.
- Test fixtures are YAML files in `testdata/`. The required fields vary by test suite — `apply` uses `init`/`desired`/`applied`, `plan` uses `init`/`desired`/`plan`/`error`, `dump` uses `init`/`dump`, `parser`/`fmt` use `input`/`expected`. The plan/apply/dump harnesses also accept many optional fields (e.g. `drop_policy`, `include`/`exclude`/`enable`/`disable`, `pre_sql`/`pre_sql_file`/`concurrently_pre_sql`/`concurrently_pre_sql_file`, `omit_schema`, `disable_index_concurrently`, `applied_sql`, `disallowed_drops`, `count`, `verify_no_drift`). The full set is the `planTestCase` / `applyTestCase` / `dumpTestCase` structs at the top of `plan_test.go` / `apply_test.go` / `dump_test.go` — check them when writing a new fixture.
- New plan/apply/dump tests should be added as YAML fixtures whenever the test is purely SQL-input → SQL/dump-output. Reach for a Go test only when the scenario can't be expressed that way: connection or auth errors, transaction/Writer plumbing, file-IO failures, the `--execute*` features, multi-schema setups that need helpers like `setupSchemaDB`, or assertions that examine internal Go data structures (`Files()` map, `ObjectCount` methods, schema-map helpers, etc.). When the harness lacks a field for a behavior you want to assert in a fixture, prefer extending the `*TestCase` struct with one optional field (defaulting to nil/zero so existing fixtures are unaffected) over keeping the test in Go.
- `orderedmap.Map` is used throughout for deterministic iteration order of schema objects.
- CLI scenario tests live in `test/scenario/`. Each `*.test.sh` script loads an initial schema, then applies incremental changes step by step, verifying plan output and drift-free state at each step. Shared helpers are in `helper.sh`; test SQL data is in `test/scenario/testdata/<schema>/`.
