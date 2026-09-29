![pistachio](https://github.com/user-attachments/assets/d1e6ca05-778e-4329-af87-ce68d2abaebc)

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)
[![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/winebarrel/pistachio)](https://www.coderabbit.ai)
[![Greptile: The War on Bugs](https://www.greptile.com/badge.svg)](https://www.greptile.com/?utm_source=oss_badge&utm_medium=readme&utm_campaign=greptile_for_open_source)

Declarative schema management for PostgreSQL. Write the schema you want as SQL; `pista plan` prints the DDL that takes the database there, and `pista apply` runs it. No migration files, no DSL.

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

Or download a binary from [Releases](https://github.com/winebarrel/pistachio/releases): macOS and Linux on amd64 and arm64, Windows on amd64. `mise use github:winebarrel/pistachio@<version>` pins a version.

The demo image bundles PostgreSQL and a sample schema:

```bash
docker run --rm -it ghcr.io/winebarrel/pistachio-demo
```

## Quick start

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

See [Getting started](https://winebarrel.github.io/pistachio/getting-started/).

## What it does

- **No drop unless you allow it.** An object removed from the file is not dropped until `--allow-drop` names its type; the plan shows the drop as a `-- skipped:` comment. [Controlling drops](https://winebarrel.github.io/pistachio/guides/drops/)
- **Renames.** `-- pista:renamed-from old_name` above an object turns a drop and a create into `RENAME`. [Renaming objects](https://winebarrel.github.io/pistachio/guides/renaming/)
- **Transactions and `CONCURRENTLY`.** `apply --with-tx` runs the plan in one transaction. `-- pista:concurrently` opts an index into `CONCURRENTLY`, which cannot run in one; `--try-tx` handles both. [Transactions and locks](https://winebarrel.github.io/pistachio/guides/transactions/)
- **Cost before it runs.** `plan --explain` says which statements scan or rewrite a table, what they block, and how big the table is. [Explaining a plan](https://winebarrel.github.io/pistachio/guides/explaining-plans/)
- **Plan files.** `plan --out` writes a plan that `apply-from` runs later, and refuses if the database changed in between. [Plan files](https://winebarrel.github.io/pistachio/guides/plan-files/)
- **Diff without a database.** `pista diff --git origin/main...HEAD schema.sql` prints the DDL a branch would apply. [Diffing schema files](https://winebarrel.github.io/pistachio/guides/diffing/)
- **Many files.** `dump --split` writes one file per object; `pista fmt` lays them out. [Formatting schema files](https://winebarrel.github.io/pistachio/guides/formatting/)
- **Tables, columns, constraints, indexes, views, enums, domains, composite types, sequences, triggers, policies, comments**, and routines with `--manage-routine`. [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/)

`CREATE EXTENSION`, `CREATE ROLE` and `GRANT` are out of scope. See [Design and scope](https://winebarrel.github.io/pistachio/about/design/) and [Known limitations](https://winebarrel.github.io/pistachio/about/limitations/).

## Development

```bash
docker compose up -d
make test
```

See [Contributing](https://winebarrel.github.io/pistachio/contributing/) for the test suites and the PostgreSQL version matrix.

## Related projects

- [ridgepole](https://github.com/ridgepole/ridgepole): DB schema management using a Rails DSL.
- [qrev](https://github.com/winebarrel/qrev): SQL execution history management tool.
