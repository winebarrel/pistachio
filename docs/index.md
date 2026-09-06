---
# The nav calls this page Home, which is the wrong headline for the social
# card a link to the site expands into. Name the project there instead.
social:
  cards_layout_options:
    title: pistachio
---

# ![pistachio](assets/logo.webp)

Declarative schema management tool for PostgreSQL with a Terraform-like
plan/apply workflow, built on
[pg_query_go](https://github.com/pganalyze/pg_query_go). Define the desired
schema in SQL; pistachio generates the DDL diff.

![pistachio workflow](workflow.svg)

![](https://github.com/user-attachments/assets/8ceaef33-7d4e-4bd8-bf94-1a79342cf1e1)

## Try it with Docker

A demo image bundles PostgreSQL with a sample schema for trying `pista` without a local install:

```bash
docker run --rm -it ghcr.io/winebarrel/pistachio-demo
```

The container starts a shell in `/demo` with `pista` and `psql` preconfigured. Edit `desired.sql`, then run:

```bash
pista plan  desired.sql     # show the DDL diff
pista apply desired.sql     # apply the changes
pista plan  desired.sql     # ...should now print "No changes"
pista dump                  # dump the current schema
```

The image sets `$PISTA_MANAGE_ROUTINE`, so the functions and procedures in the demo schema are managed too.

The source for the image is under [`demo/`](https://github.com/winebarrel/pistachio/tree/main/demo).



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


## Where to go next

- [Getting Started](getting-started.md) walks through dump, edit, plan, apply.
- [Guides](guides/renaming.md) cover one task each: renaming, filtering,
  drops, transactions, multiple schemas.
- [Commands](reference/commands.md) lists every flag.
- [Design and scope](about/design.md) says what pistachio does not manage, and
  why.

## Related projects

- [ridgepole](https://github.com/ridgepole/ridgepole): DB schema
  management using a Rails DSL.
- [qrev](https://github.com/winebarrel/qrev): SQL execution history management tool.
