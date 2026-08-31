# Preventing concurrent applies

Two `pista apply` runs against the same database can interleave: each computes its diff from a snapshot the other is still changing, then issues DDL based on it. `--exclusive` makes apply runs mutually exclusive. When another exclusive apply is running, the command fails immediately, before anything is read or applied:

```bash
pista apply schema.sql --exclusive
```

Use `--exclusive-wait` instead to wait for the other apply to finish, up to the given duration. `0` waits without limit. The two flags conflict.

```bash
pista apply schema.sql --exclusive-wait=5m
```

Also available as `$PISTA_EXCLUSIVE` / `$PISTA_EXCLUSIVE_WAIT`.

## How it works

The exclusion is a session-level [advisory lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS), taken right after connecting and before the current schema is read, so the diff is never computed against a state another exclusive apply is still changing. It is released when the connection closes, including on a crash, and it holds no table lock, so it does not block reads or writes to the schema.

Because the lock is session-level, transaction boundaries do not affect it: it behaves the same with and without `--with-tx`, and across `CREATE INDEX CONCURRENTLY`, which runs outside a transaction. The lock key includes a hash of the database name, so applies to different databases on the same cluster do not exclude each other.

## Limitations

The exclusion only covers apply runs that opt in with `--exclusive` or `--exclusive-wait`. An apply without the flag, another tool, or a psql session issuing DDL is not blocked.
