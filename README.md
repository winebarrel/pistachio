![pistachio](https://github.com/user-attachments/assets/d1e6ca05-778e-4329-af87-ce68d2abaebc)

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)
[![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/winebarrel/pistachio)](https://www.coderabbit.ai)
[![Greptile: The War on Bugs](https://www.greptile.com/badge.svg)](https://www.greptile.com/?utm_source=oss_badge&utm_medium=readme&utm_campaign=greptile_for_open_source)

pistachio is a declarative schema management tool for PostgreSQL. You write the schema you want as plain `CREATE` statements; `pista plan` prints the DDL that takes the database there, and `pista apply` runs it. There is no migration file to write and no DSL to learn: the schema file is SQL, and `pista dump` writes one from a database you already have.

> [!TIP]
> **[Try it in your browser](https://pistachio-demo.winebarrel.workers.dev)**: edit two schemas and see the DDL `pista diff` generates. Nothing to install.

**[Documentation](https://winebarrel.github.io/pistachio/)** | [Getting started](https://winebarrel.github.io/pistachio/getting-started/) | [Guides](https://winebarrel.github.io/pistachio/guides/) | [Commands](https://winebarrel.github.io/pistachio/reference/commands/) | [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/)

## How it works

![pistachio workflow](docs/workflow.svg)

![](https://github.com/user-attachments/assets/8ceaef33-7d4e-4bd8-bf94-1a79342cf1e1)

## Install

```bash
brew install winebarrel/pistachio/pistachio     # Homebrew
mise use github:winebarrel/pistachio            # mise
```

Or download a binary for macOS, Linux or Windows from [Releases](https://github.com/winebarrel/pistachio/releases).

To try it without a database of your own, the demo image bundles PostgreSQL and a sample schema:

```bash
docker run --rm -it ghcr.io/winebarrel/pistachio-demo
```

## Quick start

Dump the schema you have, change it, plan, apply:

```bash
export PISTA_CONN_STR='postgres://user@host:5432/mydb'

pista dump > schema.sql            # the current schema as SQL
$EDITOR schema.sql                 # add a column, an index, a table
pista plan schema.sql              # the DDL that gets there; nothing runs
pista apply schema.sql             # run it
```

```sql
$ pista plan schema.sql
-- Connected to postgres://user@host:5432/mydb
-- Plan for schema public (2 tables, 0 views, 1 enum, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
CREATE INDEX users_email_idx ON public.users USING btree (email);
```

A second `plan` prints `-- No changes`. Keep `schema.sql` in version control and repeat. See [Getting started](https://winebarrel.github.io/pistachio/getting-started/).

## What it does

- **Nothing is dropped unless you say so.** A drop is written as a `-- skipped:` comment until `--allow-drop` names its type. [Controlling drops](https://winebarrel.github.io/pistachio/guides/drops/)
- **Renames stay renames.** `-- pista:renamed-from old_name` above a table, column, index or any other object turns a drop and a create into `RENAME`. [Renaming objects](https://winebarrel.github.io/pistachio/guides/renaming/)
- **Index builds that do not block writes.** Opt an index into `CONCURRENTLY` with a directive, run the rest in a transaction with `--with-tx` or `--try-tx`, and bound lock waits with `--pre-sql`. [Transactions and locks](https://winebarrel.github.io/pistachio/guides/transactions/)
- **Know what a statement costs before it runs.** `plan --explain` says which statements scan or rewrite a table, what their lock blocks, and how big the table is. [Explaining a plan](https://winebarrel.github.io/pistachio/guides/explaining-plans/)
- **Plan here, apply there.** `plan --out` writes a plan file that `apply-from` runs later, refusing to run if the database changed in between. [Plan files](https://winebarrel.github.io/pistachio/guides/plan-files/)
- **No database needed to review a change.** `pista diff --git origin/main...HEAD schema.sql` prints the DDL a branch would apply, for CI. [Diffing schema files](https://winebarrel.github.io/pistachio/guides/diffing/)
- **One schema, many files.** `dump --split` writes one file per object; `plan` and `apply` take them all. `pista fmt` lays them out. [Formatting schema files](https://winebarrel.github.io/pistachio/guides/formatting/)
- **Tables, columns, constraints, indexes, views, materialized views, enums, domains, composite types, sequences, triggers, policies and comments**, with functions and procedures behind `--manage-routine`. [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/)

`CREATE EXTENSION`, `CREATE ROLE` and `GRANT` are out of scope: manage them where the rest of the infrastructure is managed. [Design and scope](https://winebarrel.github.io/pistachio/about/design/) says why, and [Known limitations](https://winebarrel.github.io/pistachio/about/limitations/) lists what pistachio does not handle yet.

## Development

```bash
docker compose up -d
make test
```

See [Contributing](https://winebarrel.github.io/pistachio/contributing/) for the test suites and the PostgreSQL version matrix.

## Related projects

- [ridgepole](https://github.com/ridgepole/ridgepole): DB schema management using a Rails DSL.
- [qrev](https://github.com/winebarrel/qrev): SQL execution history management tool.
