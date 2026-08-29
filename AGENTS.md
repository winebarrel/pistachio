# Repository guidelines for AI agents

## Project overview

Pistachio is a declarative schema management tool for PostgreSQL, written in Go. It parses desired schema definitions from SQL files and generates DDL diffs to bring the current database in line with the desired state.

## Design policy

`pista dump` output fed back as the desired schema must plan clean. A break in that round trip is a bug.

Drift that appears only with a desired schema written some other way is low priority, since matching what `dump` writes avoids it.

`CREATE EXTENSION`, `CREATE ROLE` and `GRANT` are out of scope. They sit at a different privilege layer, so manage them where the rest of the infrastructure is managed, Terraform for example.

Avoid normalization that makes the diff complex.

Do not emit DDL a change does not need, and never emit it implicitly. Low load on the database comes before a simple interface: a directive the user writes beats an inference.

Do not chase corner cases. A rare input is not worth an implementation that is hard to follow, and the schemas people actually write come first.

Do not coddle the user. Assume they know PostgreSQL and their own schema.

## Build & test

```sh
make build          # go build ./cmd/pista (outputs ./pista at the repo root)
make vet            # go vet ./...
make test           # go test -p 1 -v ./... $(TEST_OPTS)
make test-scenario  # CLI scenario tests (bash, requires PostgreSQL)
make test-fidelity  # restore fidelity check (bash, requires PostgreSQL and pg_dump)
make lint           # golangci-lint run
make fix            # golangci-lint run --fix (auto-fix lint errors)
```

- Tests require a running PostgreSQL instance. `compose.yaml` publishes each version of the CI matrix on its own port (15 -> 5415, 16 -> 5416, 17 -> 5417, 18 -> 5418), so several can run side by side; `PGPORT` (default 5415) selects which one every `psql`- and test-based target uses. The Makefile builds `TEST_PISTA_CONN_STR` from it; set that variable to point somewhere else entirely.
- Tests run with `-p 1` (sequential packages) because integration tests share a single database.
- `make test`, `make test-scenario` and `make test-fidelity` depend on `clean-schema`, so they wipe every user schema before running. The tests themselves reset only `public`, and a sample schema left over from `make schema` is otherwise still visible to them. `clean-schema` follows `PGHOST`/`PGUSER`/`PGPORT`, not `TEST_PISTA_CONN_STR`, so override `PGPORT` rather than the connection string.
- `make schema` and other `psql`-based targets rely on `PGHOST=localhost` / `PGUSER=postgres` / `PGPORT` (exported from the Makefile).
- The sample schema targets (`schema`, `sample-db-*`, `test-samples`, `clean-schema`, `reset-db`) live in `sample-db.mk`, which the Makefile includes. See `docs/sample-database-tests.md`.

## Project structure

- `cmd/pista/` - CLI entrypoint
- `cmd/command/` - CLI command implementations
- `parser/` - SQL parser (uses pg_query_go to parse and deparse PostgreSQL SQL)
- `catalog/` - Reads current schema state from PostgreSQL system catalogs (`pg_catalog`)
- `model/` - Data model structs (Table, Column, Constraint, ForeignKey, Index, Policy, View, Enum, Domain)
- `diff/` - Generates DDL diff between current and desired schemas
- `internal/testutil/` - Test helpers (DB connection, setup)
- `testdata/` - YAML-based test fixtures for multiple test suites, including integration and unit tests
- `test/scenario/` - CLI-level scenario tests (shell scripts that run `pista` CLI against sample schemas)
- `test/fidelity/` - restore fidelity check: loads `pista dump` output into an empty database and diffs `pg_dump` against the original
- `docs/` - the documentation site, built with MkDocs and published to GitHub Pages. The Markdown is the source; `mkdocs build --strict` fails on a broken internal link. `CHANGELOG.md` and `TODO.md` stay at the repository root and are symlinked into `docs/about/`.

## Development workflow

1. Create a feature branch before starting implementation.
2. Write tests that assert the expected behavior first, confirm they fail, then implement the fix/feature.
3. Prefer simplicity; avoid complex implementations when a straightforward approach works.
4. After implementation:
   - Verify test cases are comprehensive (check for missing scenarios and edge cases).
   - Verify coverage has not decreased and cover any reachable paths that can be tested naturally (do not write unnatural tests for unreachable defensive code).
   - Consider whether similar issues exist elsewhere in the codebase.
   - Run `make lint` to check for lint errors.
   - Run `make schema` to load sample schema SQL files into the local database (requires `psql`, `curl`, and network access), then verify behavior with `pista plan` / `pista dump` against them.
5. Do not run tests in parallel (`make test` uses `-p 1`).

## Code conventions

- Package-level tests generally use external test packages (e.g., `package catalog_test`, `package model_test`). Use same-package tests only when access to unexported identifiers is required (e.g., `package diff`).
- Root-level integration tests use `package pistachio_test`.
- Test fixtures are YAML files in `testdata/`. Required fields vary by test suite: `apply` uses `init`/`desired`/`applied`, `plan` uses `init`/`desired`/`plan`/`error`, `dump` uses `init`/`dump`, and `parser` uses `input`/`expected`. The plan/apply/dump harnesses also accept optional fields, but **the set differs per suite** (the lists below are not interchangeable):
  - `dump`: `min_pg`, `omit_schema`, `sort_by_deps`, `manage_routine`, `skip_partition_child`, `include`/`exclude`/`enable`/`disable`, `dump_pg16`/`dump_pg17`/`dump_pg18`.
  - `plan`: `min_pg`/`max_pg`, `count`, `drop_policy`, `disallowed_drops`, `ignored`, `disable_index_concurrently`, `force_index_concurrently`, `bulk_alter`, `assume_validated`, `manage_routine`, `skip_partition_child`, `include`/`exclude`/`enable`/`disable`, `pre_sql`/`pre_sql_file`/`concurrently_pre_sql`/`concurrently_pre_sql_file`.
  - `apply`: everything `plan` accepts other than `plan`/`error`, `max_pg` and `ignored`, plus `applied_sql`, `skip_drift_check` and `applied_pg16`/`applied_pg17`/`applied_pg18`.
    Every `apply` case re-plans after the apply and requires no drift;
    `skip_drift_check` turns that off for a case that leaves drift on purpose.

  The authoritative list is the `planTestCase` / `applyTestCase` / `dumpTestCase` structs at the top of `plan_test.go` / `apply_test.go` / `dump_test.go`. Check the suite-specific struct when writing a new fixture.
- New plan/apply/dump tests should be added as YAML fixtures whenever the test is purely SQL-input -> SQL/dump-output. Reach for a Go test only when the scenario can't be expressed that way: connection or auth errors, transaction/Writer plumbing, file-IO failures, the `--execute*` features, multi-schema setups that need helpers like `setupSchemaDB`, or assertions that examine internal Go data structures (`Files()` map, `ObjectCount` methods, schema-map helpers, etc.). When the harness lacks a field for a behavior you want to assert in a fixture, prefer extending the `*TestCase` struct with one optional field (defaulting to nil/zero so existing fixtures are unaffected) over keeping the test in Go.
- `orderedmap.Map` is used throughout for deterministic iteration order of schema objects.
- CLI scenario tests live in `test/scenario/`. Each `*.test.sh` script loads an initial schema, then applies incremental changes step by step, verifying plan output and drift-free state at each step. Shared helpers are in `helper.sh`; test SQL data is in `test/scenario/testdata/<schema>/`.
- The restore fidelity check lives in `test/fidelity/`. It loads a schema, records `pg_dump -s`, loads `pista dump` into an empty database, and requires the second `pg_dump -s` to be identical. PostgreSQL supplies the expected output, so it catches what `dump` drops even when nobody wrote a fixture for it and the `plan` round trip agrees. Adding coverage means adding one `.sql` file under `schemas/`, holding only what pistachio manages; a construct it does not manage shows up as a diff and is a decision, not noise. It reaches the server through `PGHOST`/`PGUSER`/`PGPORT` alone, so `TEST_PISTA_CONN_STR` does not apply, and it resets and dumps `public` only, so a schema file lives in one schema. It needs a `pg_dump` at least as new as the server.

## Writing style

- Use ASCII characters only (no non-ASCII characters).
- Avoid exaggerated or roundabout expressions.
- Write simply and concisely.
- Write in prose.
