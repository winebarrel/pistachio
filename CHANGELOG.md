# Changelog

## [1.5.0] - 2026-05-05

* Add `--bulk-alter` (also `$PIST_BULK_ALTER`) to combine consecutive `ALTER TABLE` actions on the same table into a single multi-line statement, reducing metadata-lock churn. Foreign keys, `RENAME`, `VALIDATE CONSTRAINT`, RLS toggles, and skipped DROPs are kept as separate statements so semantically distinct operations preserve their independence (`NOT VALID`'s whole point is to defer the expensive validation step). ([#147](https://github.com/winebarrel/pistachio/pull/147))
* Change `dump --split` stdout shape: replace the per-file path listing with a `-- Dump of <schema> (<summary>)` header followed by a `-- Wrote N file(s) to <dir>` footer, matching the non-split mode header style. Empty schemas now produce visible output instead of being silent. Note: this is a behavior change — anything piping the previous per-path listing into `xargs` (or similar) needs to enumerate the directory itself. ([#148](https://github.com/winebarrel/pistachio/pull/148))

## [1.4.0] - 2026-05-05

* Propagate the caller's `context.Context` through `Client.connect`, so timeout / cancellation on the `ctx` passed to `Plan` / `Apply` / `Dump` is honored at the connection establishment phase instead of being silently discarded. ([#139](https://github.com/winebarrel/pistachio/pull/139))
* Detect and emit DDL for `GENERATED ... AS IDENTITY` column transitions. `none → identity`, `identity → none`, and `ALWAYS ↔ BY DEFAULT` now produce the appropriate `ALTER TABLE ... ALTER COLUMN ADD/DROP/SET GENERATED` statements with required preconditions (`DROP DEFAULT` for columns with an explicit default or `serial`/`bigserial`/`smallserial` type, `SET NOT NULL` before `ADD IDENTITY`, `DROP NOT NULL` after `DROP IDENTITY` when desired is nullable). Previously these toggles were silently ignored, leaving the schema drifted. ([#140](https://github.com/winebarrel/pistachio/pull/140))
* Match foreign key dependency lookups against quoted map keys in `toposort`, so tables whose name or schema requires quoting (uppercase, reserved words, special characters) get their FK edges registered and DDL is emitted in the correct order. ([#141](https://github.com/winebarrel/pistachio/pull/141))
* Drop the raw-form fallback in the schema-replacer pair list. Only the canonical `model.Ident` form is added, so a `--schema-map` entry for a schema literally named `a.b` no longer collides with three-part column references like `a.b.col` (schema `a`, table `b`, column `col`). ([#142](https://github.com/winebarrel/pistachio/pull/142))
* Use unqualified keys in `OmitSchema` dump helpers, so the in-memory `tables()` / `views()` / `enums()` / `domains()` maps used to render `String()` / `Files()` are self-consistent (key matches the value's schema-stripped state). ([#143](https://github.com/winebarrel/pistachio/pull/143))
* Validate that `Options.Schemas` is non-empty and contains no empty / whitespace-only entries at the top of `Plan` / `Apply` / `Dump`, returning a clear `pistachio:`-prefixed error to library callers instead of relying on a downstream catalog or `model.Ident` failure. ([#144](https://github.com/winebarrel/pistachio/pull/144))

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
