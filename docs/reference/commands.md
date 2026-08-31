# Commands

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

!!! note
    By default, pistachio targets the `public` schema.

    See [Working with multiple schemas](../guides/multiple-schemas.md) for details.

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
      --skip-partition-child    Manage a partitioned table without its
                                partitions. For a schema whose partitions
                                another tool creates. An INHERITS child is
                                unaffected ($PISTA_SKIP_PARTITION_CHILD).
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
  -h, --help                       Show context-sensitive help.
  -c, --conn-string="postgres://postgres@localhost/postgres"
                                   PostgreSQL connection string. See
                                   https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                                   ($PISTA_CONN_STR)
  -d, --dbname=STRING              PostgreSQL database name. Overrides the
                                   dbname in --conn-string ($PISTA_DBNAME).
      --password=STRING            PostgreSQL password ($PISTA_PASSWORD).
  -n, --schemas=public,...         Schemas to inspect and modify
                                   ($PISTA_SCHEMAS).
  -m, --schema-map=KEY=VALUE;...
                                   Schema name mapping (e.g. -m old=new).
      --search-path=public         search_path for the database connection. The
                                   catalog reports an object reachable through
                                   it without its schema, so this decides how
                                   dump writes that object. Pass an empty value
                                   to qualify everything ($PISTA_SEARCH_PATH).
  -C, --config=FILE                Load options from a YAML file
                                   ($PISTA_CONFIG).
      --version
      --[no-]pager                 Force paging via $PISTA_PAGER even when
                                   stdout is not a TTY. PISTA_PAGER must be set.

  -I, --include=INCLUDE,...        Include only
                                   tables/views/enums/domains/composite
                                   types/sequences/routines matching the
                                   pattern (wildcard: *, ?; /re/ for a regular
                                   expression) ($PISTA_INCLUDE).
  -E, --exclude=EXCLUDE,...        Exclude tables/views/enums/domains/composite
                                   types/sequences/routines matching the
                                   pattern (wildcard: *, ?; /re/ for a regular
                                   expression) ($PISTA_EXCLUDE).
      --enable=ENABLE,...          Enable only specified object types (can be
                                   repeated) ($PISTA_ENABLE).
      --disable=DISABLE,...        Disable specified object types (can be
                                   repeated) ($PISTA_DISABLE).
      --manage-routine             Manage functions and procedures. Off by
                                   default; --allow-drop routine still gates
                                   dropping them ($PISTA_MANAGE_ROUTINE).
      --skip-partition-child       Manage a partitioned table without its
                                   partitions. For a schema whose partitions
                                   another tool creates. An INHERITS child is
                                   unaffected ($PISTA_SKIP_PARTITION_CHILD).
      --allow-drop=ALLOW-DROP,...
                                   Allow dropping these object types
                                   (repeatable; 'all' allows everything)
                                   ($PISTA_ALLOW_DROP).
      --pre-sql=STRING             SQL to execute before applying changes
                                   ($PISTA_PRE_SQL).
      --pre-sql-file=STRING        Path to a SQL file to execute before applying
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
      --with-tx                    Execute pre-SQL and schema changes in a
                                   transaction ($PISTA_WITH_TX).
      --try-tx                     Execute pre-SQL and schema changes in
                                   a transaction when possible. A diff
                                   containing CONCURRENTLY index DDL runs
                                   without a transaction instead of failing
                                   ($PISTA_TRY_TX).
      --disable-index-concurrently
                                   Ignore CONCURRENTLY opt-ins (directive and
                                   inline) and emit plain CREATE/DROP INDEX
                                   ($PISTA_DISABLE_INDEX_CONCURRENTLY).
      --force-index-concurrently
                                   Force CONCURRENTLY on every CREATE/DROP
                                   INDEX, including pure drops.
                                   Cannot be combined with --with-tx
                                   ($PISTA_FORCE_INDEX_CONCURRENTLY).
      --bulk-alter                 Combine consecutive ALTER TABLE actions on
                                   the same table into a single statement.
                                   FK changes, RENAME, VALIDATE CONSTRAINT,
                                   RLS toggles, and skipped DROPs stay separate
                                   ($PISTA_BULK_ALTER).
      --assume-validated           Treat every table constraint, domain
                                   constraint, and foreign key as validated:
                                   ignore NOT VALID and never emit VALIDATE
                                   CONSTRAINT ($PISTA_ASSUME_VALIDATED).
      --exclusive                  Make apply runs on the same database
                                   mutually exclusive: fail immediately
                                   when another exclusive apply is running
                                   ($PISTA_EXCLUSIVE).
      --exclusive-wait=DURATION    Like --exclusive, but wait up to
                                   the given duration (0 waits without
                                   limit) for the other apply to finish
                                   ($PISTA_EXCLUSIVE_WAIT).
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
      --skip-partition-child    Manage a partitioned table without its
                                partitions. For a schema whose partitions
                                another tool creates. An INHERITS child is
                                unaffected ($PISTA_SKIP_PARTITION_CHILD).
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


## plan

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

!!! note
    `apply` sets `search_path` to the target schemas plus `public` so unqualified type and object references resolve. `plan` output does not include that `SET search_path`, so piping `pista plan -n <schema>` into `psql` for a non-public schema may fail on an unqualified reference. Qualify the reference or run `pista apply`.


## apply

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

!!! note
    When the generated diff includes `CREATE INDEX CONCURRENTLY` or `DROP INDEX CONCURRENTLY`, `--with-tx` cannot be used because `CONCURRENTLY` operations cannot run inside a transaction. If there are no index changes, `--with-tx` is allowed even when an index is opted into `CONCURRENTLY`. To run `apply` inside a transaction in spite of the opt-in, combine `--with-tx` with `--disable-index-concurrently`. To keep the opt-in and run without a transaction instead, use `--try-tx`.

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

Use `--exclusive` to make apply runs on the same database mutually exclusive. When another exclusive apply is running, the command fails immediately, before anything is read or applied. Use `--exclusive-wait` instead to wait for the other apply to finish, up to the given duration (`0` waits without limit). The two flags conflict. Also available as `$PISTA_EXCLUSIVE` / `$PISTA_EXCLUSIVE_WAIT`. See [Preventing concurrent applies](../guides/exclusive-apply.md).

```bash
pista apply schema.sql --exclusive
pista apply schema.sql --exclusive-wait=5m
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

!!! note
    Only pure removals of constraints, foreign keys, and indexes (those absent from the desired schema) are governed by `--allow-drop=constraint` / `--allow-drop=foreign_key` / `--allow-drop=index`. Definition changes still execute regardless of `--allow-drop`: constraints and foreign keys as DROP + ADD, and indexes as DROP + CREATE, because PostgreSQL has no `ALTER CONSTRAINT` and no general `ALTER INDEX` form for definition changes.

    Foreign-key drops emitted because the owning table is being dropped follow the table-drop policy (not `foreign_key`): if the table drop is suppressed, the FK drop is suppressed too and surfaces as `-- skipped:` alongside the table.


## dump

Dump the current database schema as SQL. Output can be used directly as a schema file.

```bash
pista dump
```

