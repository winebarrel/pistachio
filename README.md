![pistachio](https://github.com/user-attachments/assets/d1e6ca05-778e-4329-af87-ce68d2abaebc)

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)

Declarative schema management tool for PostgreSQL with a Terraform-like plan/apply workflow, built on [pg_query_go](https://github.com/pganalyze/pg_query_go). Define the desired schema in SQL; pistachio generates the DDL diff.

> [!WARNING]
> **Changed in 1.52.0**: The connection options belong to the command that opens a connection, so they go after it, not before. `fmt` no longer takes them at all, and `parse` takes `-n` alone.
>
> ```bash
> pista -n myschema -c postgres://user@host/db dump   # before 1.52.0
> pista dump -n myschema -c postgres://user@host/db   # 1.52.0 and later
> ```
>
> `-C` / `--config`, `--pager` and `--version` are unchanged, and so are the environment variables and the config file.

**[Documentation](https://winebarrel.github.io/pistachio/)** | [Getting Started](https://winebarrel.github.io/pistachio/getting-started/) | [Commands](https://winebarrel.github.io/pistachio/reference/commands/) | [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/) | [Design and scope](https://winebarrel.github.io/pistachio/about/design/)

## Workflow

![pistachio workflow](docs/workflow.svg)

![](https://github.com/user-attachments/assets/8ceaef33-7d4e-4bd8-bf94-1a79342cf1e1)

## Installation

### Homebrew

```bash
brew install winebarrel/pistachio/pistachio
```

### mise

```bash
mise use github:winebarrel/pistachio            # latest
mise use github:winebarrel/pistachio@<version>  # a specific version
```

### Download binary

Download the latest binary from [Releases](https://github.com/winebarrel/pistachio/releases).

| OS      | Arch         |
|---------|--------------|
| macOS   | amd64, arm64 |
| Linux   | amd64, arm64 |
| Windows | amd64        |

## Example

Create a schema file:

```sql
CREATE TYPE public.status AS ENUM ('active', 'inactive');

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    status status NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE public.posts (
    id integer NOT NULL,
    user_id integer NOT NULL,
    title text NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_posts_user_id ON public.posts USING btree (user_id);

ALTER TABLE ONLY public.posts
    ADD CONSTRAINT posts_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES users(id);
```

Preview and apply:

```bash
pista plan schema.sql                  # review the diff (drops suppressed by default)
pista plan --allow-drop all schema.sql # review the diff (with drops)
pista apply schema.sql                 # apply it
```

See what each statement costs before it runs:

```sql
$ pista plan --explain schema.sql
-- rewrite, blocks reads and writes: public.orders (~2,000,000 rows, 210 MB, as of 2026-09-09, 3 indexes rebuilt)
ALTER TABLE public.orders ALTER COLUMN amount SET DATA TYPE numeric(12,2);
-- scan, blocks writes: public.orders (~2,000,000 rows, 210 MB, as of 2026-09-09), public.customers (~50,000 rows, 6280 kB, as of 2026-07-21)
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers (id);
-- scan, blocks nothing: public.orders (~2,000,000 rows, 210 MB, as of 2026-09-09)
CREATE INDEX CONCURRENTLY orders_created_at_idx ON public.orders USING btree (created_at);
```

The rows and bytes are the estimates the last VACUUM or ANALYZE wrote, with the date it wrote them, so the comment costs no read of the table. See [Explaining a plan](https://winebarrel.github.io/pistachio/guides/explaining-plans/).

Or split the schema across multiple files:

```bash
pista dump --split ./schema/       # dump per table/view/enum/domain/composite type
pista plan ./schema/*.sql          # review the diff
pista apply ./schema/*.sql         # apply it
```

Lay the files out:

```bash
pista fmt ./schema/*.sql           # rewrite them in place
pista fmt --check ./schema/*.sql   # exit 2 when a file is not formatted
```

Or diff two schema files without a database:

```bash
pista diff old.sql new.sql         # print the DDL that takes old.sql to new.sql
pista diff --check old.sql new.sql # exit 2 when the two differ
```

The output carries the same schema DDL as `plan`, so diffing two revisions of a schema file previews a migration in CI. See [Diffing schema files](https://winebarrel.github.io/pistachio/guides/diffing/).

Or read the schema as JSON:

```bash
pista parse ./schema/*.sql         # print the objects the files declare as JSON
```

It uses the same parser `plan` and `apply` use and reads no database, so another tool can consume the schema. `pista dump --json` writes the same shape, read from the database. See [Parsing schema files](https://winebarrel.github.io/pistachio/guides/parsing/).


## Demo

A demo image bundles PostgreSQL with a sample schema for trying `pista` without a local install:

```bash
docker run --rm -it ghcr.io/winebarrel/pistachio-demo
```

See [Try it with Docker](https://winebarrel.github.io/pistachio/#try-it-with-docker).

## Development

```bash
docker compose up -d
make test
```

See [Contributing](https://winebarrel.github.io/pistachio/contributing/) for the test suites and the PostgreSQL version matrix.

## Related projects

- [ridgepole](https://github.com/ridgepole/ridgepole): DB schema
  management using a Rails DSL.
- [qrev](https://github.com/winebarrel/qrev): SQL execution history management tool.
