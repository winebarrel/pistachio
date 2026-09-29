# Glossary

Schema file
:   A SQL file that declares the desired schema with `CREATE` statements. One or several make up the desired schema.

Desired schema
:   What the schema files declare. The state a plan brings the database to.

Current schema
:   What the database holds, read from the system catalogs. Under `diff`, the first file plays this role.

Plan
:   The DDL that takes the current schema to the desired one. `pista plan` prints it and `pista apply` runs it.

Plan file
:   A plan written by `plan --out` for `apply-from` to run later. See [Plan files](../guides/plan-files.md).

Executable DDL
:   A statement the plan would run: the DDL and the `-- pista:execute-first` and `-- pista:execute` statements it selects. A skipped drop and a comment are not.

Check SQL
:   The expression after `-- pista:execute` or `-- pista:execute-first`. The statement runs when it returns true.

Drift
:   A difference between the current schema and the desired schema, or between a database and the schema a plan file was computed against.

Plan clean
:   A plan with no changes. `pista dump` output fed back as the desired schema plans clean; a break in that round trip is a bug. See [The contract](../about/design.md#the-contract).

Managed
:   An object kind pistachio reads, compares and writes DDL for. Routines and storage parameters are managed only when opted in.

Scope
:   The options that decide what is read on both sides: the schemas, the filters, and the opt-ins. A plan file records them.

Directive
:   A `-- pista:<name>` comment placed before a statement. See [Directives](directives.md).

Skipped
:   A drop the plan does not run because `--allow-drop` does not name its type. Written as a `-- skipped:` comment.

Ignored
:   An object a `-- pista:ignore` directive takes out of both sides, or a routine with `SET ... FROM CURRENT`. Written as an `-- ignored:` comment.

Pure removal
:   An object the desired schema no longer holds. The kind of drop `--allow-drop` gates. The drop half of a definition change is not one, except that a view, routine or trigger recreate needs its type too. See [Controlling drops](../guides/drops.md).

Recreate
:   A change that PostgreSQL has no `ALTER` for, run as a drop and a create.

Dependent
:   An object PostgreSQL will not let another be dropped or changed under: a view that reads a table, a foreign key that references a key. Where pistachio checks, `plan` fails and names them before `apply` would. A table or column drop is not checked.

Default schema
:   The first schema in `--schemas`. A name written without a schema belongs to it.
