# Plan files

`plan --out` writes what the plan would run to a file, and `apply-from` runs it:

```bash
pista plan --out plan.json schema.sql
# review plan.json, get it approved, hand it to the deploy job
pista apply-from plan.json
```

`apply-from` reads no schema file. The statements were decided when the plan was written, and it runs those. What it does read from the database is one thing: whether the database is still the state the plan was computed against.

## What is checked

`plan --out` records a hash of the schema it read from the system catalogs. `apply-from` reads the same way and compares. A table altered, dropped, or created again under the plan changes the hash, and the apply stops before anything runs:

```
pista: error: the database has drifted since plan file plan.json was written: run plan again, or pass --force to apply it as it is
```

`--force` runs the plan anyway and reports the drift as a warning:

```sql
-- Warning: the database has drifted since the plan was written
```

The hash covers what the plan compared and nothing else. An object `--include` or `--exclude` left out is not in it, and neither are the storage parameters under the default `--manage-storage-param`, so an autovacuum setting changed on a table does not stand between a plan and its apply. A table dropped and created again with the same definition is a change: the hash carries each object's OID.

It is a hash of the schema, not of the database. Data is not in it, and neither is the identity of the server: a plan written against one database applies to another whose schema is identical. The connection is yours to get right, as it is for `apply`.

The major version of the server is recorded and compared too. It decides what the catalog reads and what DDL the server takes, so a plan file does not travel between two of them. `--force` does not reach that check.

## The scope comes from the file

The plan file records the options that decide what the plan read: `--schemas`, `--schema-map`, `--search-path`, and the filters (`--include`, `--exclude`, `--enable`, `--disable`, `--manage-routine`, `--manage-storage-param`, `--skip-partition-child`). `apply-from` reads the database with those rather than with its own.

That is why `apply-from` does not offer them. A filter given there would narrow the read the hash is taken over, and report drift that is not there.

What it does offer is the connection (`--conn-string`, `--dbname`, `--password`) and the flags that decide how the statements are run: `--with-tx`, `--try-tx`, `--timing`, `--exclusive`, `--exclusive-wait`.

The pre-SQL is part of the plan, not of the apply: `plan --out --pre-sql ...` writes it into the file, and `apply-from` runs it. The same goes for `--concurrently-pre-sql`, and for whether the plan contains `CONCURRENTLY` index DDL, which is what makes `--with-tx` refuse it.

## Execute directives

A `-- pista:execute` check is evaluated once, when the plan file is written. The file holds the statements that check said to run, without the condition, and `apply-from` runs them.

A check that cannot be evaluated therefore fails `plan --out`:

```
pista: error: failed to evaluate check SQL for --out: SELECT ...: ERROR: ...
```

Without `--out` such a check is noted in the plan and `apply` decides it later. A plan file has no later, so writing the guess as a promise is refused instead. See [Executing SQL](executing-sql.md).

A check is a question about the schema or about the data. The state hash answers for the schema; data can change under a plan file without the hash noticing, and the statement runs as the plan decided.

## The file

The plan file is JSON, written by pista and read by pista. There is no schema published for it and no promise about its shape beyond the version it carries: a file of another version is refused with a message that says to plan again.

That version is raised whenever what the file holds or how the hash is taken changes, which includes a change to the model pistachio reads the schema into. Upgrading pista between `plan --out` and `apply-from` is therefore a plan again, not a drift.
