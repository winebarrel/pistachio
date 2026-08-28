![pistachio](https://github.com/user-attachments/assets/d1e6ca05-778e-4329-af87-ce68d2abaebc)

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)

Declarative schema management tool for PostgreSQL with a Terraform-like plan/apply workflow, built on [pg_query_go](https://github.com/pganalyze/pg_query_go). Define the desired schema in SQL; pistachio generates the DDL diff.

See also: [Getting Started Guide](getting-started.md) / [Directives](directives.md) / [Performance](performance.md) / [Sample Database Tests](sample-db-test.md)

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

## Demo

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

The source for the image is under [`demo/`](demo/).

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

## Supported Objects

- Domain types (`CREATE DOMAIN`, `ALTER DOMAIN SET/DROP DEFAULT`, `SET/DROP NOT NULL`, `ADD/DROP/VALIDATE CONSTRAINT`)
- Enum types (`CREATE TYPE ... AS ENUM`, `ALTER TYPE ... ADD VALUE`)
- Composite types (`CREATE TYPE ... AS (...)`, `ALTER TYPE ... ADD/DROP/ALTER ATTRIBUTE`, `RENAME ATTRIBUTE`). Attributes are matched by name, so reordering them produces no diff (PostgreSQL cannot reorder attributes). `ALTER ATTRIBUTE ... TYPE` fails at apply while a table column uses the type; PostgreSQL does not allow it and `CASCADE` does not help.
- Sequences (`CREATE SEQUENCE`, `ALTER SEQUENCE`, `DROP SEQUENCE`). Only standalone sequences are managed; sequences owned by a serial or identity column are handled as part of that column, not as separate objects.
- Tables (including unlogged and partitioned tables)
- Views
- Materialized views
- Columns (serial/bigserial/smallserial, identity, generated, TOAST storage and compression)
- Constraints (primary key, unique, check, exclusion, foreign key)
- Indexes (unique, partial, expression, hash, multi-column)
- Comments (on tables, columns, views, types, domains, composite types, composite attributes, sequences)
- Row-level security (`ALTER TABLE ... ENABLE/DISABLE/FORCE/NO FORCE ROW LEVEL SECURITY`, policies via `CREATE POLICY` / `ALTER POLICY` / `DROP POLICY`)
- Triggers (`CREATE TRIGGER`, `CREATE CONSTRAINT TRIGGER`, `INSTEAD OF` triggers on views, and the enable state via `ALTER TABLE ... ENABLE/DISABLE TRIGGER`); see [Triggers](#triggers)
- Routines (`CREATE FUNCTION`, `CREATE PROCEDURE`), opt-in with `--manage-routine`. An overload set is several objects, keyed by argument type. See [Routines](#routines).
- Renaming (tables, views, enums, enum values, domains, composite types, composite attributes, sequences, columns, constraints, foreign keys, indexes, policies, triggers via `-- pista:renamed-from` directive)
- Array, JSON, UUID, and other built-in types
- Quoted identifiers

pistachio parses only the statements above. It drops any other statement in a schema file, such as `SET`, `GRANT`, or `CREATE EXTENSION`, and prints a `pistachio: ignored unsupported statement:` warning to standard error for each one. The same warning covers the parts it does not read of a statement it does parse, such as the `ALTER TABLE ... ADD COLUMN` and `ALTER COLUMN ... SET DEFAULT` a `pg_dump` file carries, or `COMMENT ON INDEX`. The `ALTER COLUMN ... SET STORAGE` and `SET COMPRESSION` such a file carries are read. To keep an unsupported statement in the file and run it during `apply`, mark it with `-- pista:execute`, which also silences the warning. A `BEGIN` or `COMMIT` warning points at `--with-tx` and `--try-tx`, which wrap the apply in a transaction.

## Usage

```
Usage: pista <command> [flags]

Flags:
  -h, --help                  Show context-sensitive help.
  -c, --conn-string="postgres://postgres@localhost/postgres"
                              PostgreSQL connection string. See
                              https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                              ($PISTA_CONN_STR)
  -d, --dbname=STRING         PostgreSQL database name. Overrides the dbname in
                              --conn-string ($PISTA_DBNAME).
      --password=STRING       PostgreSQL password ($PISTA_PASSWORD).
  -n, --schemas=public,...    Schemas to inspect and modify ($PISTA_SCHEMAS).
  -m, --schema-map=KEY=VALUE;...
                              Schema name mapping (e.g. -m old=new).
      --search-path=public    search_path for the database connection.
                              The catalog reports an object reachable through
                              it without its schema, so this decides how dump
                              writes that object. Pass an empty value to qualify
                              everything ($PISTA_SEARCH_PATH).
  -C, --config=FILE           Load options from a YAML file ($PISTA_CONFIG).
      --version
      --[no-]pager            Force paging via $PISTA_PAGER even when stdout is
                              not a TTY. PISTA_PAGER must be set.

Commands:
  apply <files> ... [flags]
    Apply schema changes to the database.

  plan <files> ... [flags]
    Print the schema diff SQL without applying it.

  dump [flags]
    Dump the current database schema as SQL.

Run "pista <command> --help" for more information on a command.
```

> [!note]
> By default, pistachio targets the `public` schema.
> 
> See [Getting Started#working-with-specific-schemas](getting-started.md#working-with-specific-schemas) for details.

<details>
<summary><code>pista plan --help</code></summary>

```
Usage: pista plan <files> ... [flags]

Print the schema diff SQL without applying it.

Arguments:
  <files> ...    Path to the desired schema SQL file(s).

Flags:
  -h, --help                    Show context-sensitive help.
  -c, --conn-string="postgres://postgres@localhost/postgres"
                                PostgreSQL connection string. See
                                https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                                ($PISTA_CONN_STR)
  -d, --dbname=STRING           PostgreSQL database name. Overrides the dbname
                                in --conn-string ($PISTA_DBNAME).
      --password=STRING         PostgreSQL password ($PISTA_PASSWORD).
  -n, --schemas=public,...      Schemas to inspect and modify ($PISTA_SCHEMAS).
  -m, --schema-map=KEY=VALUE;...
                                Schema name mapping (e.g. -m old=new).
      --search-path=public      search_path for the database connection.
                                The catalog reports an object reachable through
                                it without its schema, so this decides how
                                dump writes that object. Pass an empty value to
                                qualify everything ($PISTA_SEARCH_PATH).
  -C, --config=FILE             Load options from a YAML file ($PISTA_CONFIG).
      --version
      --[no-]pager              Force paging via $PISTA_PAGER even when stdout
                                is not a TTY. PISTA_PAGER must be set.

  -I, --include=INCLUDE,...     Include only
                                tables/views/enums/domains/composite
                                types/sequences/routines matching the pattern
                                (wildcard: *, ?; /re/ for a regular expression)
                                ($PISTA_INCLUDE).
  -E, --exclude=EXCLUDE,...     Exclude tables/views/enums/domains/composite
                                types/sequences/routines matching the pattern
                                (wildcard: *, ?; /re/ for a regular expression)
                                ($PISTA_EXCLUDE).
      --enable=ENABLE,...       Enable only specified object types (can be
                                repeated) ($PISTA_ENABLE).
      --disable=DISABLE,...     Disable specified object types (can be repeated)
                                ($PISTA_DISABLE).
      --manage-routine          Manage functions and procedures. Off by default;
                                --allow-drop routine still gates dropping them
                                ($PISTA_MANAGE_ROUTINE).
      --skip-partition-child    Leave partition children unmanaged and
                                manage the partitioned parent alone.
                                For a schema whose partitions another tool
                                creates. An INHERITS child is unaffected
                                ($PISTA_SKIP_PARTITION_CHILD).
      --allow-drop=ALLOW-DROP,...
                                Allow dropping these object types (repeatable;
                                'all' allows everything) ($PISTA_ALLOW_DROP).
      --pre-sql=STRING          SQL to prepend to the plan output
                                ($PISTA_PRE_SQL).
      --pre-sql-file=STRING     Path to a SQL file to prepend to the plan output
                                ($PISTA_PRE_SQL_FILE).
      --concurrently-pre-sql=STRING
                                SQL to run before CONCURRENTLY index DDL (e.g.
                                SET lock_timeout). Emitted only when
                                the diff contains CONCURRENTLY index DDL
                                ($PISTA_CONCURRENTLY_PRE_SQL).
      --concurrently-pre-sql-file=STRING
                                Path to a SQL file to run before CONCURRENTLY
                                index DDL ($PISTA_CONCURRENTLY_PRE_SQL_FILE).
      --disable-index-concurrently
                                Ignore CONCURRENTLY opt-ins (directive and
                                inline) and emit plain CREATE/DROP INDEX
                                ($PISTA_DISABLE_INDEX_CONCURRENTLY).
      --force-index-concurrently
                                Force CONCURRENTLY on every
                                CREATE/DROP INDEX, including pure drops
                                ($PISTA_FORCE_INDEX_CONCURRENTLY).
      --bulk-alter              Combine consecutive ALTER TABLE actions on the
                                same table into a single statement. FK changes,
                                RENAME, VALIDATE CONSTRAINT, RLS toggles, and
                                skipped DROPs stay separate ($PISTA_BULK_ALTER).
      --assume-validated        Treat every table constraint, domain constraint,
                                and foreign key as validated: ignore NOT
                                VALID and never emit VALIDATE CONSTRAINT
                                ($PISTA_ASSUME_VALIDATED).
      --no-read-only            Open the database connection read-write.
                                By default plan uses a read-only connection
                                ($PISTA_NO_READ_ONLY).
      --check                   Exit with code 2 when the plan contains
                                executable changes ($PISTA_CHECK).
```

</details>

<details>
<summary><code>pista apply --help</code></summary>

```
Usage: pista apply <files> ... [flags]

Apply schema changes to the database.

Arguments:
  <files> ...    Path to the desired schema SQL file(s).

Flags:
  -h, --help                    Show context-sensitive help.
  -c, --conn-string="postgres://postgres@localhost/postgres"
                                PostgreSQL connection string. See
                                https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                                ($PISTA_CONN_STR)
  -d, --dbname=STRING           PostgreSQL database name. Overrides the dbname
                                in --conn-string ($PISTA_DBNAME).
      --password=STRING         PostgreSQL password ($PISTA_PASSWORD).
  -n, --schemas=public,...      Schemas to inspect and modify ($PISTA_SCHEMAS).
  -m, --schema-map=KEY=VALUE;...
                                Schema name mapping (e.g. -m old=new).
      --search-path=public      search_path for the database connection.
                                The catalog reports an object reachable through
                                it without its schema, so this decides how
                                dump writes that object. Pass an empty value to
                                qualify everything ($PISTA_SEARCH_PATH).
  -C, --config=FILE             Load options from a YAML file ($PISTA_CONFIG).
      --version
      --[no-]pager              Force paging via $PISTA_PAGER even when stdout
                                is not a TTY. PISTA_PAGER must be set.

  -I, --include=INCLUDE,...     Include only
                                tables/views/enums/domains/composite
                                types/sequences/routines matching the pattern
                                (wildcard: *, ?; /re/ for a regular expression)
                                ($PISTA_INCLUDE).
  -E, --exclude=EXCLUDE,...     Exclude tables/views/enums/domains/composite
                                types/sequences/routines matching the pattern
                                (wildcard: *, ?; /re/ for a regular expression)
                                ($PISTA_EXCLUDE).
      --enable=ENABLE,...       Enable only specified object types (can be
                                repeated) ($PISTA_ENABLE).
      --disable=DISABLE,...     Disable specified object types (can be repeated)
                                ($PISTA_DISABLE).
      --manage-routine          Manage functions and procedures. Off by default;
                                --allow-drop routine still gates dropping them
                                ($PISTA_MANAGE_ROUTINE).
      --skip-partition-child    Leave partition children unmanaged and
                                manage the partitioned parent alone.
                                For a schema whose partitions another tool
                                creates. An INHERITS child is unaffected
                                ($PISTA_SKIP_PARTITION_CHILD).
      --allow-drop=ALLOW-DROP,...
                                Allow dropping these object types (repeatable;
                                'all' allows everything) ($PISTA_ALLOW_DROP).
      --pre-sql=STRING          SQL to execute before applying changes
                                ($PISTA_PRE_SQL).
      --pre-sql-file=STRING     Path to a SQL file to execute before applying
                                changes ($PISTA_PRE_SQL_FILE).
      --concurrently-pre-sql=STRING
                                SQL to execute before CONCURRENTLY
                                index DDL (e.g. SET lock_timeout).
                                Runs outside any transaction, only when
                                the diff contains CONCURRENTLY index DDL
                                ($PISTA_CONCURRENTLY_PRE_SQL).
      --concurrently-pre-sql-file=STRING
                                Path to a SQL file to execute
                                before CONCURRENTLY index DDL
                                ($PISTA_CONCURRENTLY_PRE_SQL_FILE).
      --with-tx                 Execute pre-SQL and schema changes in a
                                transaction ($PISTA_WITH_TX).
      --try-tx                  Execute pre-SQL and schema changes in a
                                transaction when possible. A diff containing
                                CONCURRENTLY index DDL runs without a
                                transaction instead of failing ($PISTA_TRY_TX).
      --disable-index-concurrently
                                Ignore CONCURRENTLY opt-ins (directive and
                                inline) and emit plain CREATE/DROP INDEX
                                ($PISTA_DISABLE_INDEX_CONCURRENTLY).
      --force-index-concurrently
                                Force CONCURRENTLY on every CREATE/DROP INDEX,
                                including pure drops. Cannot be combined with
                                --with-tx ($PISTA_FORCE_INDEX_CONCURRENTLY).
      --bulk-alter              Combine consecutive ALTER TABLE actions on the
                                same table into a single statement. FK changes,
                                RENAME, VALIDATE CONSTRAINT, RLS toggles, and
                                skipped DROPs stay separate ($PISTA_BULK_ALTER).
      --assume-validated        Treat every table constraint, domain constraint,
                                and foreign key as validated: ignore NOT
                                VALID and never emit VALIDATE CONSTRAINT
                                ($PISTA_ASSUME_VALIDATED).
```

</details>

<details>
<summary><code>pista dump --help</code></summary>

```
Usage: pista dump [flags]

Dump the current database schema as SQL.

Flags:
  -h, --help                    Show context-sensitive help.
  -c, --conn-string="postgres://postgres@localhost/postgres"
                                PostgreSQL connection string. See
                                https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                                ($PISTA_CONN_STR)
  -d, --dbname=STRING           PostgreSQL database name. Overrides the dbname
                                in --conn-string ($PISTA_DBNAME).
      --password=STRING         PostgreSQL password ($PISTA_PASSWORD).
  -n, --schemas=public,...      Schemas to inspect and modify ($PISTA_SCHEMAS).
  -m, --schema-map=KEY=VALUE;...
                                Schema name mapping (e.g. -m old=new).
      --search-path=public      search_path for the database connection.
                                The catalog reports an object reachable through
                                it without its schema, so this decides how
                                dump writes that object. Pass an empty value to
                                qualify everything ($PISTA_SEARCH_PATH).
  -C, --config=FILE             Load options from a YAML file ($PISTA_CONFIG).
      --version
      --[no-]pager              Force paging via $PISTA_PAGER even when stdout
                                is not a TTY. PISTA_PAGER must be set.

  -I, --include=INCLUDE,...     Include only
                                tables/views/enums/domains/composite
                                types/sequences/routines matching the pattern
                                (wildcard: *, ?; /re/ for a regular expression)
                                ($PISTA_INCLUDE).
  -E, --exclude=EXCLUDE,...     Exclude tables/views/enums/domains/composite
                                types/sequences/routines matching the pattern
                                (wildcard: *, ?; /re/ for a regular expression)
                                ($PISTA_EXCLUDE).
      --enable=ENABLE,...       Enable only specified object types (can be
                                repeated) ($PISTA_ENABLE).
      --disable=DISABLE,...     Disable specified object types (can be repeated)
                                ($PISTA_DISABLE).
      --manage-routine          Manage functions and procedures. Off by default;
                                --allow-drop routine still gates dropping them
                                ($PISTA_MANAGE_ROUTINE).
      --skip-partition-child    Leave partition children unmanaged and
                                manage the partitioned parent alone.
                                For a schema whose partitions another tool
                                creates. An INHERITS child is unaffected
                                ($PISTA_SKIP_PARTITION_CHILD).
      --split=STRING            Output each table/view/enum/domain/composite
                                type/sequence as a separate file in the
                                specified directory.
      --omit-schema             Omit schema name from the dump output.
      --sort-by-deps            Order the dump output by object dependency
                                instead of by name. Errors when the dependency
                                graph has a cycle. Cannot be used with --split.
      --no-read-only            Open the database connection read-write.
                                By default dump uses a read-only connection
                                ($PISTA_NO_READ_ONLY).
```

</details>

### plan

Compare schema file(s) against the current database and print the SQL needed to reconcile them.

```bash
pista plan schema.sql

# Multiple files
pista plan tables.sql views.sql

# Include pre-SQL in the output
pista plan schema.sql --pre-sql "SET statement_timeout = '5s';"
pista plan schema.sql --pre-sql-file pre.sql
```

`--pre-sql` / `--pre-sql-file` are also available as `$PISTA_PRE_SQL` / `$PISTA_PRE_SQL_FILE`.

Use `--check` to detect schema drift from the exit code. The exit code is 2 if the plan contains executable changes, 0 if not, and 1 on error. The output does not change. Suppressed drops alone exit 0 because they generate no executable DDL. Also available as `$PISTA_CHECK`.

```bash
pista plan --check schema.sql
echo $?  # 0: no changes, 2: changes, 1: error
```

`plan` and `dump` open a read-only connection, so they cannot write to the database. Pass `--no-read-only` (env `$PISTA_NO_READ_ONLY`) to use a read-write connection.

Every connection sets `search_path` to `public`, so a server-side `ALTER ROLE ... SET search_path` does not reach it. `--search-path` (env `$PISTA_SEARCH_PATH`) sets another value. PostgreSQL's own default, `"$user", public`, is not used: it would read the objects of a schema named after the connecting role without their schema, and the role that runs migrations is often not the role the application connects as.

The catalog reports an object reachable through `search_path` without its schema. `dump` writes the object as the catalog reports it, and `plan` compares that form against the desired schema, so a desired schema that qualifies an object the catalog reports bare differs on every run. Under `--search-path=` the objects pistachio manages keep their schema. Under `--search-path=myschema` the objects in `myschema` lose theirs, and under the default so do those in `public`.

> [!NOTE]
> `apply` sets `search_path` to the target schemas plus `public` so unqualified type and object references resolve. `plan` output does not include that `SET search_path`, so piping `pista plan -n <schema>` into `psql` for a non-public schema may fail on an unqualified reference. Qualify the reference or run `pista apply`.

### apply

Apply the diff to the database.

```bash
pista apply schema.sql

# Multiple files
pista apply tables.sql views.sql
```

Use `--pre-sql` or `--pre-sql-file` to run SQL before applying changes (mutually exclusive). Also available as `$PISTA_PRE_SQL` / `$PISTA_PRE_SQL_FILE`. Use `--with-tx` to wrap the apply in a transaction (also available as `$PISTA_WITH_TX`).

```bash
# Inline SQL
pista apply schema.sql --pre-sql "SET statement_timeout = '5s';" --with-tx

# From file
pista apply schema.sql --pre-sql-file pre.sql --with-tx
```

Use `--try-tx` to wrap the apply in a transaction only when possible. It works like `--with-tx`, except that a diff containing `CREATE/DROP INDEX CONCURRENTLY` runs without a transaction instead of failing. The two flags are mutually exclusive. Also available as `$PISTA_TRY_TX`.

```bash
pista apply --try-tx schema.sql
```

The skipped transaction is recorded in the output:

```sql
-- Transaction skipped: plan contains CONCURRENTLY index DDL
CREATE INDEX CONCURRENTLY idx_users_name ON public.users USING btree (name);
```

An apply that skips the transaction is not all-or-nothing. A failure leaves the statements that already ran in place, and re-running `pista apply` applies the rest. A failed `CREATE INDEX CONCURRENTLY` also leaves an invalid index that must be dropped by hand.

To apply `CONCURRENTLY` to individual indexes, either write `CREATE INDEX CONCURRENTLY` directly or use the `-- pista:concurrently` directive before the `CREATE INDEX` statement. Both are treated equivalently:

```sql
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);

-- Equivalent: inline CONCURRENTLY
CREATE INDEX CONCURRENTLY idx_users_email ON public.users USING btree (email);

-- This index will NOT use CONCURRENTLY
CREATE INDEX idx_users_id ON public.users USING btree (id);
```

Use `--concurrently-pre-sql` (or `--concurrently-pre-sql-file`) to run SQL (typically `SET lock_timeout = '...'`) before any `CONCURRENTLY` index DDL. The SQL is emitted only when the plan contains `CREATE/DROP INDEX CONCURRENTLY`. Because `SET` is session-scoped and `CONCURRENTLY` runs outside a transaction, the value carries over to every subsequent `CONCURRENTLY` statement in the same `apply`. Also available as `$PISTA_CONCURRENTLY_PRE_SQL` / `$PISTA_CONCURRENTLY_PRE_SQL_FILE`.

```bash
pista apply schema.sql --concurrently-pre-sql "SET lock_timeout = '5s';"
```

Use `--disable-index-concurrently` to ignore all `CONCURRENTLY` opt-ins (both inline and directive) and emit plain `CREATE INDEX` / `DROP INDEX` instead. This lets you keep the directives in your schema files while running a one-off plan/apply inside a transaction. Also available as `$PISTA_DISABLE_INDEX_CONCURRENTLY`.

```bash
pista plan --disable-index-concurrently schema.sql
pista apply --disable-index-concurrently --with-tx schema.sql
```

Use `--force-index-concurrently` to apply `CONCURRENTLY` to every `CREATE INDEX` and `DROP INDEX` the diff emits, regardless of per-index directives. This also covers pure drops (indexes removed from the desired schema), which the directive cannot reach. Conflicts with `--disable-index-concurrently` and `--with-tx`. Also available as `$PISTA_FORCE_INDEX_CONCURRENTLY`.

```bash
pista plan --force-index-concurrently schema.sql
pista apply --force-index-concurrently schema.sql
```

> [!NOTE]
> When the generated diff includes `CREATE INDEX CONCURRENTLY` or `DROP INDEX CONCURRENTLY`, `--with-tx` cannot be used because `CONCURRENTLY` operations cannot run inside a transaction. If there are no index changes, `--with-tx` is allowed even when an index is opted into `CONCURRENTLY`. To run `apply` inside a transaction in spite of the opt-in, combine `--with-tx` with `--disable-index-concurrently`. To keep the opt-in and run without a transaction instead, use `--try-tx`.

Use `--bulk-alter` to combine consecutive `ALTER TABLE` actions on the same table into a single statement with comma-separated actions. This reduces metadata-lock churn and lets PostgreSQL plan the changes together. Foreign keys, `RENAME`, `VALIDATE CONSTRAINT`, RLS toggles, and skipped DROPs are kept as separate statements. Also available as `$PISTA_BULK_ALTER`.

```bash
pista plan --bulk-alter schema.sql
pista apply --bulk-alter schema.sql
```

```sql
ALTER TABLE public.users
  ADD COLUMN email text,
  ALTER COLUMN name SET NOT NULL,
  DROP COLUMN legacy,
  ADD CONSTRAINT users_id_pos CHECK (id > 0);
```

To merge `ALTER TABLE` actions for individual tables only, put the `-- pista:bulk-alter` directive before the `CREATE TABLE` statement. Other tables keep one statement per action. `--bulk-alter` merges every table regardless of the directive.

```sql
-- pista:bulk-alter
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

Use `--assume-validated` to treat every table constraint, domain constraint, and foreign key as validated. Pistachio ignores `NOT VALID` in the desired schema and never emits `NOT VALID` or `VALIDATE CONSTRAINT`. Use it when `NOT VALID` is a one-off migration step you do not want in the desired state. Also available as `$PISTA_ASSUME_VALIDATED`.

```bash
pista plan --assume-validated schema.sql
pista apply --assume-validated schema.sql
```

By default, `plan` and `apply` do not drop tables, views, enums, domains, composite types, columns, constraints, foreign keys, or indexes. Use `--allow-drop` to enable dropping specific object types (`all`, `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`, `column`, `constraint`, `foreign_key`, `index`, `policy`, `trigger`). Also available as `$PISTA_ALLOW_DROP`. `constraint` covers CHECK / UNIQUE / PRIMARY KEY / EXCLUSION; foreign keys are governed by `foreign_key` separately. `composite_type` also gates `DROP ATTRIBUTE` on a composite type. `routine` covers functions and procedures, and also gates the drop half of a recreate.

```bash
# Allow all drops
pista plan --allow-drop all schema.sql

# Allow only column and table drops
pista apply --allow-drop column,table schema.sql
```

Suppressed drops are emitted as commented-out DDL prefixed with `-- skipped:`. The plan still reports `-- No changes` when the only diff would be a suppressed drop, since no executable DDL is generated:

```sql
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
-- skipped: DROP TABLE public.legacy_users;
-- No changes
```

> [!NOTE]
> Only pure removals of constraints, foreign keys, and indexes (those absent from the desired schema) are governed by `--allow-drop=constraint` / `--allow-drop=foreign_key` / `--allow-drop=index`. Definition changes still execute regardless of `--allow-drop`: constraints and foreign keys as DROP + ADD, and indexes as DROP + CREATE, because PostgreSQL has no `ALTER CONSTRAINT` and no general `ALTER INDEX` form for definition changes.
>
> Foreign-key drops emitted because the owning table is being dropped follow the table-drop policy (not `foreign_key`): if the table drop is suppressed, the FK drop is suppressed too and surfaces as `-- skipped:` alongside the table.

### Executing arbitrary SQL

Use `-- pista:execute` to include non-managed SQL (grants, extensions) in your schema files. Functions and procedures can be managed declaratively instead; see [Routines](#routines). The check SQL after the directive is evaluated by both `plan` and `apply`. When it returns `true` the statement is executed, otherwise skipped, and `plan` leaves out the statements `apply` would skip. A common pattern skips when an object already exists:

```sql
-- pista:execute SELECT to_regprocedure('public.my_func()') IS NULL
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ ... $$ LANGUAGE plpgsql;
```

To manage a function whose body changes over time, embed a version tag in `COMMENT ON FUNCTION` and execute only when the installed comment differs. Wrap the `CREATE` and `COMMENT` in a `DO` block so they are a single statement:

```sql
-- pista:execute SELECT obj_description(to_regprocedure('public.get_user_count()'), 'pg_proc') IS DISTINCT FROM 'v1'
DO $do$ BEGIN
  CREATE OR REPLACE FUNCTION public.get_user_count() RETURNS bigint AS $body$
    SELECT count(*) FROM public.users;
  $body$ LANGUAGE sql;
  COMMENT ON FUNCTION public.get_user_count() IS 'v1';
END $do$;
```

When the body changes, update the tag in both places (e.g. `'v1'` -> `'v2'`); the next `apply` will re-run.

`-- pista:execute` runs after the managed DDL. Use `-- pista:execute-first` when the managed DDL calls the function, as a `CHECK` constraint, a `GENERATED` expression, an index expression, or a policy can:

```sql
-- pista:execute-first SELECT to_regprocedure('public.lower_v(text)') IS NULL
CREATE OR REPLACE FUNCTION public.lower_v(t text) RETURNS text AS $$ SELECT lower(t) $$ LANGUAGE sql IMMUTABLE;

CREATE TABLE public.users (
    id integer NOT NULL,
    v text,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_v_check CHECK (lower_v(v) <> 'x')
);
```

The check SQL is evaluated where the statement runs, so an `execute-first` check sees the schema before the change and an `execute` check sees it after.

See [Getting Started](getting-started.md) for details.

### dump

Dump the current database schema as SQL. Output can be used directly as a schema file.

```bash
pista dump
```

### Paging long output

Set `$PISTA_PAGER` to forward `plan` / `apply` / `dump` output through an external command when stdout is a TTY. The command is interpreted by `sh -c` (`cmd /c` on Windows), so quoting and arguments work as in the shell. Pipes and redirects (`pista dump > file.sql`, `pista dump | grep ...`) are unaffected; the pager runs only for interactive output. Use `--no-pager` to disable it for a single invocation, or `--pager` to force it on when stdout is not a TTY (e.g. when piping into another pager-aware tool). `PISTA_PAGER` must still be set for `--pager` to do anything.

```bash
# Page with less, keeping ANSI colors
PISTA_PAGER='less -R' pista dump

# Pipe through a syntax highlighter that supports SQL
PISTA_PAGER='source-highlight -s sql -f esc | less -R' pista plan schema.sql

# One-off override
pista --no-pager plan schema.sql

# Force the pager even when stdout is not a TTY
PISTA_PAGER='source-highlight -s sql -f esc' pista --pager dump
```

### Configuration file

Use `-C` / `--config` to load options from a YAML file.

Keys are flag names (for example `conn-string`, not `conn_string`). An unknown key is an error.

```yaml
# pista.yml
conn-string: postgres://user@db.example.com/app
schemas:
  - public
  - billing
schema-map:
  staging: public
exclude:
  - tmp_*
```

```bash
pista --config pista.yml dump
pista --config pista.yml plan schema.sql

# Or set the path with an environment variable
export PISTA_CONFIG=pista.yml
pista dump
```

One file works for every command. Keys that the running command does not use are ignored.

Precedence: command-line flag > environment variable > config file > default.

### Schema name mapping

Use `-m` / `--schema-map` to remap schema names when the database schema name differs from the one used in your SQL files.

For example, to dump a `staging` schema as if it were `public`:

```bash
pista -n staging -m staging=public dump
```

You can also use it with `plan` and `apply`. The desired SQL files use the mapped name (`public`), while the generated SQL targets the real database schema (`staging`):

```bash
# schema.sql uses "public" as the schema name
pista -n staging -m staging=public plan schema.sql
pista -n staging -m staging=public apply schema.sql
```

### Filtering objects

Use `-I` / `--include` to include only matching objects by name, or `-E` / `--exclude` to exclude them. Patterns support `*` and `?` wildcards. A pattern wrapped in slashes is a regular expression, which matches anywhere in the name unless it is anchored; a wildcard has to match the whole name. Patterns match against object names only (not schema-qualified names). Also available as `$PISTA_INCLUDE` / `$PISTA_EXCLUDE` environment variables.

Use `--enable` to restrict operations to specific object types, or `--disable` to exclude specific types. Valid types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. Can be repeated. Also available as `$PISTA_ENABLE` / `$PISTA_DISABLE` environment variables.

These flags are available on the `dump`, `plan`, and `apply` subcommands.

```bash
# Dump only objects matching "user*"
pista dump -I 'user*'

# Plan changes excluding temporary tables
pista plan -E 'tmp_*' schema.sql

# Combine include and exclude
pista apply -I 'user*' -E 'user_tmp' schema.sql

# Exclude numbered partitions
pista plan -E '/^posts_\d+$/' schema.sql

# Mix the two forms
pista dump -I 'user*' -I '/^audit_(log|trail)$/'

# Dump only enums
pista dump --enable enum

# Dump only tables and views
pista dump --enable table,view

# Dump everything except views
pista dump --disable view

# Plan changes for enums only
pista plan --enable enum schema.sql

# Plan changes for routines only (--manage-routine is still required)
pista plan --manage-routine --enable routine schema.sql

# Using environment variables
PISTA_ENABLE=enum pista dump
PISTA_DISABLE=view pista dump
PISTA_INCLUDE='user*' pista dump
PISTA_EXCLUDE='tmp_*' pista plan schema.sql
```

> [!NOTE]
> `--enable` takes precedence over `--disable`. When `--enable` is set, only the specified types are included regardless of `--disable`. These flags may exclude dependent objects (e.g. `--enable table` omits enums/domains that table columns may reference); use them for inspection (`dump`, `plan`), not `apply`. `routine` narrows what `--manage-routine` turned on; it does not turn routines on by itself.

> [!NOTE]
> When both a CLI flag and its corresponding environment variable are set, the CLI flag overrides the environment variable (values are not merged). For example, running `PISTA_EXCLUDE='tmp_*' pista plan -E 'foo_*' schema.sql` excludes only `foo_*`; `tmp_*` is ignored.

### Skipping partition children

Use `--skip-partition-child` to leave every partition of a partitioned table unmanaged and manage the parent alone. `dump` writes the parent without its partitions, and `plan` / `apply` neither create a partition the desired schema declares nor drop one the database holds. Also available as `$PISTA_SKIP_PARTITION_CHILD`. It works on the `dump`, `plan`, and `apply` subcommands.

Use it where another tool owns the partitions, pg_partman for example. `-E '/^posts_\d+$/'` reaches a regularly numbered set, but nothing reaches one whose names carry no pattern, and a desired schema that declares the parent alone plans a `DROP TABLE` for every partition it does not know about.

```bash
# Manage the partitioned parent and leave its partitions alone
pista plan --skip-partition-child schema.sql

# Dump writes the parent without its partitions
pista dump --skip-partition-child
```

A partition is left out whether or not it is partitioned itself, so a sub-partitioned level goes with the leaves under it. The parent still carries a change down: PostgreSQL applies `ADD COLUMN` and an index created on a partitioned table to every partition. A table attached with `INHERITS` is not a partition and stays managed.

### Omit schema

Use `--omit-schema` to omit schema names from the dump output.

```bash
pista dump --omit-schema
# => CREATE TABLE users (...) instead of CREATE TABLE public.users (...)

pista dump --omit-schema --split ./schema/
# -- Dump of schema public (2 tables, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
# -- Wrote 2 file(s) to ./schema/
# (writes ./schema/users.sql, ./schema/orders.sql, ...)
```

When schema is omitted in SQL files, `plan` and `apply` use the schema specified by `-n`:

```bash
pista -n staging plan schema.sql   # schema-less SQL is treated as "staging"
pista -n staging apply schema.sql
```

### Sort by dependency

By default the dump orders objects by name within each type. Use `--sort-by-deps` to order them by dependency instead, so each object comes after the objects it depends on. For example, a table is placed after the types it uses and after the tables its foreign keys reference.

```bash
pista dump --sort-by-deps
```

The output can then be loaded from top to bottom without forward references. If the dependency graph has a cycle, such as two tables with mutual foreign keys, the dump cannot be ordered and errors. The flag cannot be combined with `--split`, which writes each object to a separate file with no inter-file order.

### Renaming objects

Use `-- pista:renamed-from <old_name>` directives to rename objects instead of dropping and recreating them.

**Tables, views, enums:**

```sql
-- pista:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active', 'inactive');

-- pista:renamed-from public.old_users
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

-- pista:renamed-from public.old_view
CREATE VIEW public.new_view AS SELECT 1;
```

**Enum values** (inside `CREATE TYPE ... AS ENUM`, before the value). The rename emits `ALTER TYPE ... RENAME VALUE`, which keeps stored data and the value's position:

```sql
CREATE TYPE public.status AS ENUM (
    'active',
    -- pista:renamed-from 'inactive'
    'disabled'
);
```

**Composite types and their attributes**. The statement-level directive renames the type (`ALTER TYPE ... RENAME TO`). A directive inside `CREATE TYPE ... AS (...)`, before an attribute, renames that attribute (`ALTER TYPE ... RENAME ATTRIBUTE`) and keeps stored data:

```sql
-- pista:renamed-from public.address
CREATE TYPE public.postal_address AS (
    -- pista:renamed-from street
    road text,
    city text
);
```

**Columns, constraints, indexes** (inside `CREATE TABLE` or before `CREATE INDEX` / `ALTER TABLE ADD CONSTRAINT`):

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pista:renamed-from users_name_key
    CONSTRAINT users_display_name_key UNIQUE (display_name)
);

-- pista:renamed-from idx_users_name
CREATE INDEX idx_users_display_name ON public.users (display_name);

-- pista:renamed-from fk_old_name
ALTER TABLE public.orders ADD CONSTRAINT fk_new_name FOREIGN KEY (user_id) REFERENCES public.users(id);
```

> [!TIP]
> Rename directives that have already been applied are silently skipped. Leave them in place until cleanup.

#### Column rename caveats

When a column is renamed, pistachio rewrites column references in same-table indexes, constraints, foreign keys (including `EXCLUDE`, partial / expression / `INCLUDE` indexes), triggers, policies and generated expressions on the current side, so a single `ALTER TABLE ... RENAME COLUMN` is emitted without redundant DDL on the dependents.

The desired-side SQL must use the new column name in those dependent definitions:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- Reference the new column name here:
CREATE INDEX idx_users_name ON public.users (display_name);
```

If the desired side still references the old name, `pista plan` errors out at parse time with a message like `column name referenced in index idx_users_name does not exist on table public.users` (identifiers are quoted only when they aren't safe unquoted). Index, constraint, foreign-key, `DEFAULT` and generated definitions are checked, and all such unresolved references are reported in a single error.

The following references are not auto-rewritten and may produce a redundant `DROP/CREATE` on the first plan (the second run after applying the rename is clean):

- View / materialized view definitions that `SELECT` the renamed column
- Foreign keys in other tables whose `REFERENCES this_table(renamed_col)` points at the renamed column
- Definitions on a partition child of the renamed table, which pistachio diffs on its own before the rewrite runs

### Ignoring objects

Use the `-- pista:ignore` directive to leave an object unmanaged. pistachio does not create, alter, or drop a `CREATE TABLE` / `CREATE TYPE ... AS ENUM` / `CREATE TYPE ... AS (...)` / `CREATE DOMAIN` / `CREATE VIEW` / `CREATE FUNCTION` / `CREATE PROCEDURE` marked with it. This is the in-file equivalent of `--exclude` for a single object, useful for a table managed by another tool or one whose definition intentionally drifts.

```sql
-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
```

Each ignored object is reported as an `-- ignored: <name>` comment in `plan` and `apply` output. The directive attaches to a statement in the schema file, so it can only ignore an object you have declared. To keep an existing object that would otherwise be dropped, write its `CREATE` statement with the directive.

### Split dump

Use `--split` to output each table/view/enum/domain/composite type as a separate file in the specified directory.

```bash
pista dump --split ./schema/
# -- Dump of schema public (3 tables, 0 views, 1 enum, 0 domains, 0 composite types, 0 sequences)
# -- Wrote 4 file(s) to ./schema/
# (writes ./schema/public.status.sql, ./schema/public.users.sql, ./schema/public.orders.sql, ...)
```

## Routines

Functions and procedures are managed only when `--manage-routine` is passed (also `$PISTA_MANAGE_ROUTINE`). Without it `pg_proc` is never read, so a schema maintained with `-- pista:execute` keeps working and plan output is unchanged.

```bash
pista dump --manage-routine
pista plan --manage-routine schema.sql
pista apply --manage-routine schema.sql
```

A routine is identified by its name and its argument types, so an overload set is several independent objects:

```sql
CREATE FUNCTION public.normalize(e text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT lower(e) $$;

CREATE FUNCTION public.normalize(e text, keep_case boolean) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT CASE WHEN keep_case THEN e ELSE lower(e) END $$;
```

The body, the language, and the attributes (`IMMUTABLE` / `STABLE` / `VOLATILE`, `STRICT`, `SECURITY DEFINER`, `LEAKPROOF`, `PARALLEL`, `COST`, `ROWS`, `SET`) are compared and applied with `CREATE OR REPLACE`. The changes PostgreSQL refuses in place run as `DROP` and `CREATE`, which needs `--allow-drop routine`:

- changing the return type, including the columns of a `RETURNS TABLE`
- renaming a parameter
- removing a parameter default
- turning a function into a procedure, or back

Adding or removing a parameter is not a modification either. The argument types are the identity, so the new signature is a new routine and the old one is dropped, which needs `--allow-drop routine` as well.

An attribute left at its default is not written back. PostgreSQL reports `VOLATILE`, `PARALLEL UNSAFE` and the default `COST` as absent, so a desired schema may spell them out or leave them off.

A routine is created after the types its signature names and before every table, because a `CHECK` constraint, a `GENERATED` expression, an index expression, a policy or a trigger can call one. A signature can name a relation instead of a type, as `RETURNS SETOF <table>` does; such a routine comes after its own relation and before every other one. Dropping runs the other way: views, then tables, then routines, then types.

> [!IMPORTANT]
> That order means a `LANGUAGE sql` routine whose body reads a table created in the same run fails to apply, because PostgreSQL parses a SQL body at creation time. The same holds for one that calls a routine defined later. `plpgsql` is unaffected. Mark such a routine `-- pista:ignore` and create it with `-- pista:execute` instead.

Argument and return types are reported without their schema when `search_path` reaches them, the same as any other name pistachio reads back. A desired schema may write a type in the routine's own schema either way; the two spellings are one routine. A comment goes on the full signature:

```sql
COMMENT ON FUNCTION public.normalize(text) IS 'v1';
```

The following are not managed. Both sides of the diff leave them out, so `dump` does not write them and `plan` does not propose dropping them:

- aggregates and window functions
- a routine whose body is written in the SQL-standard `BEGIN ATOMIC` form. Such a body records real dependencies on the tables it reads, so it cannot be created ahead of them
- a routine an extension owns
- a routine carrying an option pistachio does not read, `SUPPORT` and `TRANSFORM FOR TYPE` among them
- renaming, via `-- pista:renamed-from`

An unmanaged routine is warned about and skipped, never an error, because the desired schema is read whether or not `--manage-routine` is set.

`SET <parameter> FROM CURRENT` is handled differently. It captures the session value at creation time, so the statement carries nothing to compare, but the database reports the resolved value and the routine is read back like any other. Such a routine is treated as `-- pista:ignore`: it is neither created, altered, nor dropped, and its signature is listed under `-- ignored:`.

## Triggers

pistachio manages triggers. The function a trigger calls is managed only with `--manage-routine`; see [Routines](#routines). Without that flag, write the function with `-- pista:execute-first` so it exists before the `CREATE TRIGGER` that references it runs:

```sql
-- pista:execute-first
CREATE OR REPLACE FUNCTION public.stamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE public.events (
    id integer NOT NULL,
    updated_at timestamptz,
    CONSTRAINT events_pkey PRIMARY KEY (id)
);

CREATE TRIGGER events_stamp BEFORE UPDATE ON public.events
    FOR EACH ROW EXECUTE FUNCTION public.stamp();
```

A trigger names the table or view it sits on, so that relation has to be declared earlier in the file, or in an earlier file when the schema is split across several.

A definition change is applied with `CREATE OR REPLACE TRIGGER`, which takes a lighter lock than dropping and recreating. Turning a trigger into a constraint trigger, or back, is the one change PostgreSQL rejects in place; it runs as `DROP TRIGGER` and `CREATE TRIGGER` and needs `--allow-drop trigger`, the same as any other trigger drop. Without it the trigger keeps its current definition.

`CREATE TRIGGER` has no syntax for a disabled trigger, so a desired schema asks for one the way `dump` writes it:

```sql
ALTER TABLE public.events DISABLE TRIGGER events_stamp;
```

`ENABLE ALWAYS` and `ENABLE REPLICA` work the same way. PostgreSQL rejects the statement on a view, so a view's triggers have no state to set. Both `CREATE OR REPLACE TRIGGER` and a recreate leave a trigger enabled, so pistachio re-applies any other state right after.

The catalog reports a trigger's function without its schema when `search_path` reaches it, the same as any other name pistachio reads back. A desired schema that writes `public.stamp()` while the default `--search-path=public` is in effect therefore differs on every run; `--search-path=` keeps every schema and matches a `pg_dump`-style file.

A trigger on a partitioned table is cloned onto every partition. pistachio reads and writes the parent's trigger alone, and PostgreSQL keeps the clones in step.

The `ALL` and `USER` forms of `ENABLE TRIGGER` name no single trigger and are ignored, as are event triggers, which belong to the database rather than to a schema.

## Constraint and index naming

> [!IMPORTANT]
> Unnamed constraints (e.g. `id integer PRIMARY KEY`, `name text UNIQUE`, `col integer REFERENCES other(id)`) are auto-named by pistachio following PostgreSQL's convention: `{table}_pkey`, and `{table}_{col}..._key`, `{table}_{col}..._fkey`, `{table}_{col}..._excl` joining every key column. A `CHECK` becomes `{table}_{col}_check` when its expression references one column and `{table}_check` when it references none or several, which is what PostgreSQL does even for a constraint written on a column.
>
> An index written without a name (e.g. `CREATE INDEX ON users (name)`) is named `{table}_{col}..._idx`, joining every index element including the `INCLUDE` list. An element written as an expression takes the name PostgreSQL gives it: the function it calls (`(lower(name))` gives `users_lower_idx`), the field of a field selection, the column under a subscript, the type of a cast that has nothing under it, or `expr` when it has none of its own.
>
> A name that does not fit in 63 bytes (NAMEDATALEN - 1) is shortened the way PostgreSQL shortens it, by trimming the table and column parts and keeping the trailing label.
>
> The auto-naming has one limitation: when a generated name meets a name already in use, whether generated or explicit, PostgreSQL appends a number with no separator (e.g. `users_id_check1`, `users_name_idx1`) that pistachio cannot predict, so such a file is rejected as a duplicate name.
>
> Use explicit `CONSTRAINT <name>` clauses and index names to avoid these issues.

## Development

```bash
docker compose up -d
make test
```

`compose.yaml` carries one service per PostgreSQL version in the CI matrix,
each published on its own port, so several versions can run side by side:

```bash
docker compose up -d               # 15 only, on port 5415
docker compose up -d pg17          # 17 only, on port 5417
docker compose --profile all up -d # 15, 16, 17 and 18
```

`PGPORT` selects the one the tests use, and defaults to 5415:

```bash
make PGPORT=5417 test
```

## Related projects

- [ridgepole](https://github.com/ridgepole/ridgepole): DB schema
  management using a Rails DSL.
- [qrev](https://github.com/winebarrel/qrev): SQL execution history management tool.
