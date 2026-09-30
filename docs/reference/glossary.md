# Glossary

Schema file
:   A SQL file that declares the desired schema with `CREATE` statements. One or more such files make up the desired schema.

Desired schema
:   What the schema files declare. The state that a plan brings the database to.

Current schema
:   What the database contains, as read from the system catalogs. Under `diff`, the first file takes this role.

Plan
:   The DDL that changes the current schema into the desired schema. `pista plan` prints it and `pista apply` runs it.

Plan file
:   A plan written by `plan --out` for `apply-from` to run later. See [Plan files](../guides/plan-files.md).

Executable DDL
:   A statement that the plan would run. This includes the DDL and the `-- pista:execute-first` and `-- pista:execute` statements that the plan selects. A skipped drop and a comment are not executable DDL.

Check SQL
:   The expression after `-- pista:execute` or `-- pista:execute-first`. The statement runs when it returns true.

Drift
:   A difference between the current schema and the desired schema. It is also a difference between a database and the schema that a plan file was computed against.

Plan clean
:   A plan with no changes. When `pista dump` output is used as the desired schema, the plan is clean. A break in that round trip is a bug. See [The contract](../about/design.md#the-contract).

Managed
:   An object kind that pistachio reads, compares and writes DDL for. Routines and storage parameters are managed only when you opt in.

Scope
:   The options that decide what is read on both sides: the schemas, the filters, and the opt-ins. A plan file records them.

Directive
:   A `-- pista:<name>` comment placed before a statement. See [Directives](directives.md).

Skipped
:   A drop that the plan does not run because `--allow-drop` does not include its type. It is written as a `-- skipped:` comment.

Ignored
:   An object that a `-- pista:ignore` directive removes from both sides, or a routine with `SET ... FROM CURRENT`. It is written as an `-- ignored:` comment.

Pure removal
:   An object that the desired schema no longer contains. This is the kind of drop that requires `--allow-drop`. The drop that is part of a definition change is not a pure removal. One exception: recreating a view, routine or trigger also requires `--allow-drop` for its type. See [Controlling drops](../guides/drops.md).

Recreate
:   A change that PostgreSQL has no `ALTER` for. It is run as a drop and a create.

Dependent
:   An object that prevents PostgreSQL from dropping or changing another object. Examples: a view that reads a table, a foreign key that references a key. Where pistachio checks for dependents, `plan` fails and lists them before `apply` would fail. A table or column drop is not checked.

Default schema
:   The first schema in `--schemas`. A name written without a schema belongs to it.
