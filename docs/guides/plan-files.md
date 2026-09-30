# Plan files

`plan --out` writes the plan to a file and `apply-from` runs it.

```bash
pista plan --out plan.json schema.sql
pista apply-from plan.json
```

`apply-from` reads no schema file and computes no diff. The statements were decided when the plan was written. `apply-from` reads the database to check that the schema is still the one the plan was computed against. Then it runs the statements.

## The drift check

`plan --out` records a hash of the schema that it read. `apply-from` reads the schema the same way and compares the hashes. When they differ, nothing runs:

```
pista: error: the database has drifted since plan file plan.json was written: run plan again, or pass --force to apply it as it is
```

`--force` runs the plan anyway and reports the drift as a warning:

```sql
-- Warning: the database has drifted since the plan was written
```

The hash covers what the plan compared. An object left out by `--include` or `--exclude` is not in it. The storage parameters are not in it either, under the default `--manage-storage-param`. A table that is dropped and created again with the same definition does change the hash, because each object's OID is part of it.

An object that the desired schema marks `-- pista:ignore` is not in the hash either. The plan file lists those objects. `apply-from` removes them from what it read before it computes the hash. So another tool can own such a table and change it, without blocking the apply of a plan.

The hash does not cover data. It also does not cover which database the connection points at. A plan applies to another database whose schema is identical.

The major version of the server is recorded and compared as well. It decides what the catalog read returns and what DDL the server accepts. So a plan file cannot move between two major versions. `--force` does not affect that check.

## The scope comes from the file

The plan file records the options that decide what the plan read: `--schemas`, `--schema-map`, `--search-path`, `--include`, `--exclude`, `--enable`, `--disable`, `--manage-routine`, `--manage-storage-param` and `--skip-partition-child`. `apply-from` reads the database with those options.

`apply-from` does not accept them on the command line. A filter given there would narrow the schema that is hashed, and report drift that does not exist.

It does accept the connection options, and the flags that decide how the statements run: `--with-tx`, `--try-tx`, `--timing`, `--exclusive` and `--exclusive-wait`.

`--pre-sql` and `--concurrently-pre-sql` belong to the plan. `plan --out` writes them into the file, and `apply-from` runs them. The file also records whether the plan contains `CONCURRENTLY` index DDL. That is what makes `--with-tx` refuse the plan.

## Execute directives

A `-- pista:execute` check is evaluated when the plan file is written. The file contains the statements that the check selected, without the condition. `apply-from` runs them.

A check that cannot be evaluated fails `plan --out`:

```
pista: error: failed to evaluate check SQL for --out: SELECT ...: ERROR: ...
```

Without `--out`, the check is noted in the plan, and `apply` evaluates it again later. A plan file records a decision instead, so the check fails here. See [Running arbitrary SQL](executing-sql.md).

A check that reads data can become stale. The hash does not cover data, and the statement runs as the plan decided.

## The file

The plan file is JSON. pista writes it and pista reads it. No schema is published for it. Nothing about its shape is guaranteed, except the version that it includes. A file of another version is refused.

That version is raised when the contents of the file change, or when the way the hash is computed changes. A change to the model that pistachio reads the schema into counts as such a change. If you upgrade pista between `plan --out` and `apply-from`, you must plan again.
