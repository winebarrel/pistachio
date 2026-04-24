# CLAUDE.md

## Project overview

Pistachio is a declarative schema management tool for PostgreSQL, written in Go. It parses desired schema definitions from SQL files and generates DDL diffs to bring the current database in line with the desired state.

## Build & test

```sh
make build    # go build ./cmd/pist
make vet      # go vet ./...
make test     # go test -p 1 -v ./...
make lint     # golangci-lint run
```

- Tests require a running PostgreSQL instance (default: `postgres://postgres@localhost/postgres`, override with `TEST_PIST_CONN_STR`).
- Tests run with `-p 1` (sequential packages) because integration tests share a single database.

## Project structure

- `cmd/pist/` - CLI entrypoint
- `cmd/command/` - CLI command implementations
- `parser/` - SQL parser (uses pg_query_go to parse and deparse PostgreSQL SQL)
- `catalog/` - Reads current schema state from PostgreSQL system catalogs (`pg_catalog`)
- `model/` - Data model structs (Table, Column, Constraint, ForeignKey, Index, View, Enum, Domain)
- `diff/` - Generates DDL diff between current and desired schemas
- `internal/testutil/` - Test helpers (DB connection, setup)
- `testdata/` - YAML-based test fixtures for multiple test suites, including integration and unit tests

## Development workflow

1. Create a feature branch before starting implementation.
2. Write tests that assert the expected behavior first, confirm they fail, then implement the fix/feature.
3. Prefer simplicity — avoid complex implementations when a straightforward approach works.
4. After implementation:
   - Verify test cases are comprehensive (check for missing scenarios and edge cases).
   - Verify coverage has not decreased and cover any reachable paths that can be tested naturally (do not write unnatural tests for unreachable defensive code).
   - Consider whether similar issues exist elsewhere in the codebase.
   - Run `make lint` to check for lint errors.
   - Run `make schema` to create sample schemas, then verify behavior with `pist plan` / `pist dump` against them.
5. Do not run tests in parallel (`make test` uses `-p 1`).

## Code conventions

- Package-level tests generally use external test packages (e.g., `package catalog_test`, `package model_test`). Use same-package tests only when access to unexported identifiers is required (e.g., `package diff`).
- Root-level integration tests use `package pistachio_test`.
- Test fixtures are YAML files in `testdata/`, but fields vary by test suite (e.g., `apply` uses `init`/`desired`/`applied`, `plan` uses `init`/`desired`/`plan`/`error`, `dump` uses `init`/`dump`, `parser`/`fmt` use `input`/`expected`).
- `orderedmap.Map` is used throughout for deterministic iteration order of schema objects.
