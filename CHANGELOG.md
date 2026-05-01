# Changelog

## [1.3.0] - 2026-05-01

* Add row-level security (RLS) and policy support. Parser, catalog, diff, and dump now handle `ALTER TABLE ... ENABLE/DISABLE/FORCE/NO FORCE ROW LEVEL SECURITY` and `CREATE/ALTER/DROP POLICY`. Policies are modeled as table-subordinate so they share the table's lifecycle, dump ordering, and `--allow-drop` semantics. Diff emits `ALTER POLICY` for in-place `TO` / `USING` / `WITH CHECK` changes, `DROP+CREATE` for `Command` / `Permissive` changes or clause removals, and `ALTER POLICY ... RENAME TO` via the existing `-- pist:renamed-from` directive. `--allow-drop` now accepts `policy`. Schema map and `--omit-schema` rewrite policy schema and expression references. ([#136](https://github.com/winebarrel/pistachio/pull/136))

## [1.2.0] - 2026-04-29

* Rewrite column references in same-table indexes, constraints, and foreign keys when a column is renamed via `-- pist:renamed-from`, so a single `ALTER TABLE ... RENAME COLUMN` is emitted without redundant `DROP/CREATE` on dependents. Covers regular / composite / partial / expression / `INCLUDE` / GiST indexes, `UNIQUE` / `PRIMARY KEY` / `CHECK` / `EXCLUDE` constraints, and same-table FKs. ([#123](https://github.com/winebarrel/pistachio/pull/123))
* Track column comments across `-- pist:renamed-from` renames: comment changes (including drops) on a renamed column are now detected, and unchanged comments no longer emit a redundant `COMMENT ON COLUMN` statement. ([#123](https://github.com/winebarrel/pistachio/pull/123))
* Validate column references in desired schema at plan time: indexes, constraints (CHECK / UNIQUE / PK / EXCLUDE), and foreign keys (local side) whose definitions reference columns absent from the owning table's desired column set are reported as a single aggregated error before any DDL is executed. Catches the common mistake of renaming a column via `-- pist:renamed-from` while forgetting to update the dependent definition. ([#124](https://github.com/winebarrel/pistachio/pull/124))
* Fix `GENERATED ALWAYS AS (<expr>) STORED` column handling: parsed desired columns now correctly retain the GENERATED form (previously emitted as `DEFAULT <expr>`), and no-diff plans on generated columns no longer produce a spurious `ALTER COLUMN ... SET DEFAULT` (which PostgreSQL rejects on generated columns). ([#125](https://github.com/winebarrel/pistachio/pull/125))
* Reject GENERATED toggles at plan time: changing a column between generated and non-generated now errors with `cannot toggle GENERATED — DROP COLUMN + ADD COLUMN is required` instead of silently emitting no DDL. ([#125](https://github.com/winebarrel/pistachio/pull/125))

## [1.1.0] - 2026-04-28

* Add `--concurrently-pre-sql` / `--concurrently-pre-sql-file` option to run SQL (e.g. `SET lock_timeout`) before CONCURRENTLY index DDL. ([#121](https://github.com/winebarrel/pistachio/pull/121))

## [1.0.0] - 2026-04-28

* Initial release.
