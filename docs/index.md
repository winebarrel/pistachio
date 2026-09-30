---
# The nav calls this page Home, which is the wrong headline for the social
# card a link to the site expands into. Name the project there instead.
social:
  cards_layout_options:
    title: pistachio
---

# ![pistachio](assets/logo.webp)

pistachio is a declarative schema management tool for PostgreSQL with a
Terraform-like plan/apply workflow, built on
[pg_query_go](https://github.com/pganalyze/pg_query_go). Define the desired
schema in SQL, and pistachio generates the DDL diff.

!!! tip "Try it in your browser"
    The **[pistachio playground](https://pistachio-demo.winebarrel.workers.dev)**
    runs `pista diff` on two schemas that you edit in the page and shows the DDL
    that it generates. There is nothing to install.

![pistachio workflow](workflow.svg)

<video src="https://github.com/user-attachments/assets/7db0e761-2446-47cd-9e00-f37b1152dcff"
       width="800" style="max-width: 100%"
       controls autoplay muted loop playsinline></video>

## Try it with Docker

A demo image bundles PostgreSQL with a sample schema. It lets you try `pista` without a local install:

```bash
docker run --rm -it ghcr.io/winebarrel/pistachio-demo
```

The container starts a shell in `/demo` with `pista` and `psql` preconfigured. Edit `desired.sql`, then run:

```bash
pista plan  desired.sql     # show the DDL diff
pista apply desired.sql     # apply the changes
pista plan  desired.sql     # ...should now print -- No changes
pista dump                  # dump the current schema
```

The image sets `$PISTA_MANAGE_ROUTINE`, so pistachio manages the functions and procedures in the demo schema too.

The source for the image is under [`demo/`](https://github.com/winebarrel/pistachio/tree/main/demo).



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
pista plan schema.sql                  # review the diff (drops skipped by default)
pista plan --allow-drop all schema.sql # review the diff (with drops)
pista apply schema.sql                 # apply it
```

Alternatively, split the schema across multiple files:

```bash
pista dump --split ./schema/       # dump per table/view/enum/domain/composite type/sequence
pista plan ./schema/*.sql          # review the diff
pista apply ./schema/*.sql         # apply it
```


## Where to go next

- [Getting started](getting-started.md) shows the steps: dump, edit, plan and apply.
- [Guides](guides/index.md) cover one task each: renaming, drops, transactions and multiple schemas.
- [Commands](reference/commands/index.md) has a page per command with every option.
- [Design and scope](about/design.md) explains what pistachio does not manage, and
  why.

## Related projects

- [ridgepole](https://github.com/ridgepole/ridgepole) is a DB schema
  management tool using a Rails DSL.
- [qrev](https://github.com/winebarrel/qrev) is a SQL execution history management tool.
