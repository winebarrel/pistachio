# Changelog

## [1.2.0] - 2026-04-29

* Rewrite column references in same-table indexes, constraints, and foreign keys when a column is renamed via `-- pist:renamed-from`, so a single `ALTER TABLE ... RENAME COLUMN` is emitted without redundant `DROP/CREATE` on dependents. Covers regular / composite / partial / expression / `INCLUDE` / GiST indexes, `UNIQUE` / `PRIMARY KEY` / `CHECK` / `EXCLUDE` constraints, and same-table FKs. ([#123](https://github.com/winebarrel/pistachio/pull/123))
* Track column comments across `-- pist:renamed-from` renames: comment changes (including drops) on a renamed column are now detected, and unchanged comments no longer emit a redundant `COMMENT ON COLUMN` statement. ([#123](https://github.com/winebarrel/pistachio/pull/123))
* Validate column references in desired schema at plan time: indexes, constraints (CHECK / UNIQUE / PK / EXCLUDE), and foreign keys (local side) whose definitions reference columns absent from the owning table's desired column set are reported as a single aggregated error before any DDL is executed. Catches the common mistake of renaming a column via `-- pist:renamed-from` while forgetting to update the dependent definition. ([#124](https://github.com/winebarrel/pistachio/pull/124))
* Fix `GENERATED ALWAYS AS (<expr>) STORED` column handling: parsed desired columns now correctly retain the GENERATED form (previously emitted as `DEFAULT <expr>`), and no-diff plans on generated columns no longer produce a spurious `ALTER COLUMN ... SET DEFAULT` (which PostgreSQL rejects on generated columns). ([#125](https://github.com/winebarrel/pistachio/pull/125))

## [1.1.0] - 2026-04-28

* Add `--concurrently-pre-sql` / `--concurrently-pre-sql-file` option to run SQL (e.g. `SET lock_timeout`) before CONCURRENTLY index DDL. ([#121](https://github.com/winebarrel/pistachio/pull/121))

## [1.0.0] - 2026-04-28

* Initial release.
