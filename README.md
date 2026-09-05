![pistachio](https://github.com/user-attachments/assets/d1e6ca05-778e-4329-af87-ce68d2abaebc)

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)

Declarative schema management tool for PostgreSQL with a Terraform-like plan/apply workflow, built on [pg_query_go](https://github.com/pganalyze/pg_query_go). Define the desired schema in SQL; pistachio generates the DDL diff.

**[Documentation](https://winebarrel.github.io/pistachio/)** | [Getting Started](https://winebarrel.github.io/pistachio/getting-started/) | [Commands](https://winebarrel.github.io/pistachio/reference/commands/) | [Supported objects](https://winebarrel.github.io/pistachio/reference/objects/) | [Design and scope](https://winebarrel.github.io/pistachio/about/design/)

## Workflow

![pistachio workflow](docs/workflow.svg)

![](https://github.com/user-attachments/assets/8ceaef33-7d4e-4bd8-bf94-1a79342cf1e1)

## Installation

### Homebrew

```bash
brew install winebarrel/pistachio/pistachio
```

### Download binary

Download the latest binary from [Releases](https://github.com/winebarrel/pistachio/releases).

| OS      | Arch         |
|---------|--------------|
| macOS   | amd64, arm64 |
| Linux   | amd64, arm64 |
| Windows | amd64        |

The macOS binaries need macOS 13 or later.

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

Or split the schema across multiple files:

```bash
pista dump --split ./schema/       # dump per table/view/enum/domain/composite type
pista plan ./schema/*.sql          # review the diff
pista apply ./schema/*.sql         # apply it
```


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
