![pistachio](https://github.com/user-attachments/assets/d1e6ca05-778e-4329-af87-ce68d2abaebc)

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)

pistachio is a declarative schema management tool for PostgreSQL with a Terraform-like plan/apply workflow. You write the schema you want as DDL in SQL files. pistachio compares the database with the files and prints the DDL that makes the database match.

`pista plan` prints that DDL. `pista apply` runs it. `pista dump` writes the files from an existing database.

> [!TIP]
> The [playground](https://pistachio-demo.winebarrel.workers.dev) runs `pista diff` on two schemas that you edit in the page. There is nothing to install.

**[Documentation](https://winebarrel.github.io/pistachio/)** | [Getting started](https://winebarrel.github.io/pistachio/getting-started/) | [Guides](https://winebarrel.github.io/pistachio/guides/) | [Commands](https://winebarrel.github.io/pistachio/reference/commands/) | [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/)

## How it works

Every run computes the difference between the database and the files. There is no migration history to keep. A column added to the file becomes `ALTER TABLE ... ADD COLUMN`. A changed `CHECK` becomes a drop and an add. An object removed from the file is reported. It is dropped only when `--allow-drop` includes its type.

pistachio reads the SQL files with [pg_query_go](https://github.com/pganalyze/pg_query_go), a Go binding for the PostgreSQL parser. The files can use any SQL that PostgreSQL accepts.

![pistachio workflow](docs/workflow.svg)

![](https://github.com/user-attachments/assets/8ceaef33-7d4e-4bd8-bf94-1a79342cf1e1)

## Install

```bash
brew install winebarrel/pistachio/pistachio     # Homebrew
mise use github:winebarrel/pistachio            # mise
```

Alternatively, download a binary from [Releases](https://github.com/winebarrel/pistachio/releases/latest). Binaries exist for macOS and Linux on amd64 and arm64, and for Windows on amd64.

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

A second `plan` prints `-- No changes`. From then on, edit the file, run `plan`, and run `apply`. Every command targets the `public` schema unless `-n` selects another. [Getting started](https://winebarrel.github.io/pistachio/getting-started/) shows these steps with a real database. [Commands](https://winebarrel.github.io/pistachio/reference/commands/) lists every option.

## Features

- An object removed from the file is dropped only when `--allow-drop` includes its type. Until then, the plan shows the drop as a `-- skipped:` comment. See [Controlling drops](https://winebarrel.github.io/pistachio/guides/drops/).
- `-- pista:renamed-from old_name` above an object changes a drop and a create into a `RENAME`. See [Renaming objects](https://winebarrel.github.io/pistachio/guides/renaming/).
- `apply --with-tx` runs the plan in one transaction. `-- pista:concurrently` marks an index for `CONCURRENTLY`, which cannot run in a transaction. `--try-tx` handles both cases. See [Transactions and locks](https://winebarrel.github.io/pistachio/guides/transactions/).
- `plan --explain` reports which statements scan or rewrite a table, what they block, and how big the table is. See [Explaining a plan](https://winebarrel.github.io/pistachio/guides/explaining-plans/).
- `plan --out` writes a plan file. `apply-from` runs it later, and refuses to run if the database changed in between. See [Plan files](https://winebarrel.github.io/pistachio/guides/plan-files/).
- `pista diff --git origin/main...HEAD schema.sql` prints the DDL that a branch would apply. It needs no database. See [Diffing schema files](https://winebarrel.github.io/pistachio/guides/diffing/).
- `dump --split` writes one file per object, and `pista fmt` formats the files. See [Formatting schema files](https://winebarrel.github.io/pistachio/guides/formatting/).
- pistachio manages tables, columns, constraints, indexes, views, enums, domains, composite types, sequences, triggers, policies and comments. Routines are managed with `--manage-routine`. See [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/).

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
