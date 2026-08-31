# Preventing concurrent applies

Two apply runs on one database can interleave: each computes its diff from a state the other is changing. `--exclusive` makes apply runs mutually exclusive. While another exclusive apply is running, the command fails at once, before anything is read or applied:

```bash
pista apply schema.sql --exclusive
```

`--exclusive-wait` waits for the other apply instead, up to the given duration. `0` waits without limit. The two flags conflict.

```bash
pista apply schema.sql --exclusive-wait=5m
```

Also available as `$PISTA_EXCLUSIVE` / `$PISTA_EXCLUSIVE_WAIT`.

## How it works

The exclusion is a session-level [advisory lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS), taken right after connecting and before the current schema is read, so the diff never reflects a state another exclusive apply is changing. It holds no table lock and does not block reads or writes. It is released when the connection closes, including on a crash.

Transaction boundaries do not affect a session-level lock, so it works the same with and without `--with-tx` and across `CREATE INDEX CONCURRENTLY`. The lock key includes a hash of the database name; applies to different databases on one cluster do not exclude each other.

Only apply runs that pass `--exclusive` or `--exclusive-wait` are excluded. DDL from any other source is not blocked.
