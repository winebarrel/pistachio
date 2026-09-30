![pistachio](https://github.com/user-attachments/assets/d1e6ca05-778e-4329-af87-ce68d2abaebc)

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)
[![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/winebarrel/pistachio)](https://www.coderabbit.ai)
[![Greptile: The War on Bugs](https://www.greptile.com/badge.svg)](https://www.greptile.com/?utm_source=oss_badge&utm_medium=readme&utm_campaign=greptile_for_open_source)

pistachio is a declarative schema management tool for PostgreSQL with a Terraform-like plan/apply workflow, built on [pg_query_go](https://github.com/pganalyze/pg_query_go). Define the desired schema in SQL, and pistachio generates the DDL diff.

pistachio manages a PostgreSQL schema from SQL files. The files hold the whole schema as DDL, in the form that `pista dump` writes. `pista plan` reads the database's catalog, compares it with the files, and prints the DDL that makes the two agree. `pista apply` runs that DDL. `pista dump` writes the files from a database that you already have.

> [!TIP]
> The [playground](https://pistachio-demo.winebarrel.workers.dev) runs `pista diff` on two schemas that you edit in the page, with nothing to install.

**[Documentation](https://winebarrel.github.io/pistachio/)** | [Getting started](https://winebarrel.github.io/pistachio/getting-started/) | [Guides](https://winebarrel.github.io/pistachio/guides/) | [Commands](https://winebarrel.github.io/pistachio/reference/commands/) | [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/)

## How it works

Every run computes the difference between the database and the files, so there is no migration history to keep. A column added to the file becomes `ALTER TABLE ... ADD COLUMN`. A changed `CHECK` becomes a drop and an add. An object removed from the file is reported, and it is dropped only when `--allow-drop` names its type.

![pistachio workflow](docs/workflow.svg)

![](https://github.com/user-attachments/assets/8ceaef33-7d4e-4bd8-bf94-1a79342cf1e1)

## Install

```bash
brew install winebarrel/pistachio/pistachio     # Homebrew
mise use github:winebarrel/pistachio            # mise
```

Alternatively, download a binary from [Releases](https://github.com/winebarrel/pistachio/releases): macOS and Linux on amd64 and arm64, Windows on amd64.

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

A second `plan` prints `-- No changes`. From then on, edit the file, run `plan`, and run `apply`. Every command targets the `public` schema unless `-n` names another. [Getting started](https://winebarrel.github.io/pistachio/getting-started/) walks through this with a real database. [Commands](https://winebarrel.github.io/pistachio/reference/commands/) lists every option.

## Features

- An object removed from the file is dropped only when `--allow-drop` names its type. Until then, the plan shows the drop as a `-- skipped:` comment. See [Controlling drops](https://winebarrel.github.io/pistachio/guides/drops/).
- `-- pista:renamed-from old_name` above an object turns a drop and a create into `RENAME`. See [Renaming objects](https://winebarrel.github.io/pistachio/guides/renaming/).
- `apply --with-tx` runs the plan in one transaction. `-- pista:concurrently` opts an index into `CONCURRENTLY`, which cannot run in one. `--try-tx` handles both. See [Transactions and locks](https://winebarrel.github.io/pistachio/guides/transactions/).
- `plan --explain` says which statements scan or rewrite a table, what they block, and how big the table is. See [Explaining a plan](https://winebarrel.github.io/pistachio/guides/explaining-plans/).
- `plan --out` writes a plan file that `apply-from` runs later, and refuses to run if the database changed in between. See [Plan files](https://winebarrel.github.io/pistachio/guides/plan-files/).
- `pista diff --git origin/main...HEAD schema.sql` prints the DDL that a branch would apply, with no database. See [Diffing schema files](https://winebarrel.github.io/pistachio/guides/diffing/).
- `dump --split` writes one file per object, and `pista fmt` lays the files out. See [Formatting schema files](https://winebarrel.github.io/pistachio/guides/formatting/).
- Tables, columns, constraints, indexes, views, enums, domains, composite types, sequences, triggers, policies and comments are managed, and routines are managed with `--manage-routine`. See [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/).

`CREATE EXTENSION`, `CREATE ROLE` and `GRANT` are out of scope. See [Design and scope](https://winebarrel.github.io/pistachio/about/design/) and [Known limitations](https://winebarrel.github.io/pistachio/about/limitations/).

## Development

```bash
docker compose up -d
make test
```

See [Contributing](https://winebarrel.github.io/pistachio/contributing/) for the test suites and the PostgreSQL version matrix.

## Related projects

- [ridgepole](https://github.com/ridgepole/ridgepole) is a DB schema management tool using a Rails DSL.
- [qrev](https://github.com/winebarrel/qrev) is a SQL execution history management tool.
