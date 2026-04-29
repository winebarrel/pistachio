# Changelog

## [1.2.0] - 2026-04-29

* Rewrite column references in same-table indexes, constraints, and foreign keys when a column is renamed via `-- pist:renamed-from`, so a single `ALTER TABLE ... RENAME COLUMN` is emitted without redundant `DROP/CREATE` on dependents. Covers regular / composite / partial / expression / `INCLUDE` / GiST indexes, `UNIQUE` / `PRIMARY KEY` / `CHECK` / `EXCLUDE` constraints, and same-table FKs. ([#123](https://github.com/winebarrel/pistachio/pull/123))

## [1.1.0] - 2026-04-28

* Add `--concurrently-pre-sql` / `--concurrently-pre-sql-file` option to run SQL (e.g. `SET lock_timeout`) before CONCURRENTLY index DDL. ([#121](https://github.com/winebarrel/pistachio/pull/121))

## [1.0.0] - 2026-04-28

* Initial release.
