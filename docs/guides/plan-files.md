# Plan files

`plan --out` writes the plan to a file and `apply-from` runs it.

```bash
pista plan --out plan.json schema.sql
pista apply-from plan.json
```

`apply-from` reads no schema file and diffs nothing. The statements were decided when the plan was written. It reads the database to check that the schema is still what the plan was computed against, then runs them.

## The drift check

`plan --out` records a hash of the schema it read. `apply-from` reads the same way and compares. When the two differ, nothing runs:

```
pista: error: the database has drifted since plan file plan.json was written: run plan again, or pass --force to apply it as it is
```

`--force` runs the plan anyway and reports the drift as a warning:

```sql
-- Warning: the database has drifted since the plan was written
```

The hash covers what the plan compared. An object left out by `--include` or `--exclude` is not in it, and neither are the storage parameters under the default `--manage-storage-param`. A table dropped and created again with the same definition does change it, since each object's OID is part of it.

An object the desired schema marks `-- pista:ignore` goes the other way: it is in the hash. `apply-from` reads no desired schema and cannot tell which objects those are, so it hashes them too. A change to one therefore reports drift and stops the apply, although the statements do not touch it. Where something else owns the object and changes it, plan again or pass `--force`.

Data is not covered, and neither is which database the connection points at. A plan applies to another database whose schema is identical.

The major version of the server is recorded and compared as well. It decides what the catalog reads and what DDL the server accepts, so a plan file does not move between two of them. `--force` does not affect that check.

## The scope comes from the file

The plan file records the options that decide what the plan read: `--schemas`, `--schema-map`, `--search-path`, `--include`, `--exclude`, `--enable`, `--disable`, `--manage-routine`, `--manage-storage-param` and `--skip-partition-child`. `apply-from` reads the database with those.

It does not take them on the command line. A filter given there would narrow the read the hash is taken over and report drift that is not there.

It does take the connection options and the flags that decide how the statements run: `--with-tx`, `--try-tx`, `--timing`, `--exclusive` and `--exclusive-wait`.

`--pre-sql` and `--concurrently-pre-sql` belong to the plan. `plan --out` writes them into the file and `apply-from` runs them. So does whether the plan contains `CONCURRENTLY` index DDL, which is what makes `--with-tx` refuse it.

## Execute directives

A `-- pista:execute` check is evaluated when the plan file is written. The file holds the statements it selected, without the condition, and `apply-from` runs them.

A check that cannot be evaluated fails `plan --out`:

```
pista: error: failed to evaluate check SQL for --out: SELECT ...: ERROR: ...
```

Without `--out` the check is noted in the plan and `apply` evaluates it again later. A plan file records a decision instead, so it fails here. See [Executing SQL](executing-sql.md).

A check that reads data can go stale. The hash does not cover data, and the statement runs as the plan decided.

## The file

The plan file is JSON. pista writes it and pista reads it. No schema is published for it and nothing about its shape is guaranteed beyond the version it carries; a file of another version is refused.

That version is raised when what the file holds or how the hash is taken changes, which includes a change to the model pistachio reads the schema into. Upgrading pista between `plan --out` and `apply-from` means planning again.
