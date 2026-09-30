# Preventing concurrent applies

Two apply runs on one database can interleave. Each computes its diff from a state that the other is changing. `--exclusive` makes apply runs mutually exclusive. While another exclusive apply is running, the command fails at once, before it reads the database or applies anything:

```bash
pista apply schema.sql --exclusive
```

`--exclusive-wait` waits for the other apply instead, up to the given duration. `0` waits without limit. The two flags cannot be combined.

```bash
pista apply schema.sql --exclusive-wait=5m
```

The options are also available as `$PISTA_EXCLUSIVE` / `$PISTA_EXCLUSIVE_WAIT`.

## How it works

The exclusion is a session-level [advisory lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS). pistachio takes it right after connecting, before it reads the current schema. So the diff never reflects a state that another exclusive apply is changing. The desired schema files are read before the connection is opened. So a file that cannot be read or parsed fails without taking the lock. The exclusion takes no table lock, and does not block reads or writes. It is released when the connection closes, including on a crash.

Transaction boundaries do not affect a session-level lock. So it works the same with and without `--with-tx`, and across `CREATE INDEX CONCURRENTLY`. The lock key includes a hash of the database name. Applies to different databases on one cluster do not exclude each other.

A wait retries once a second, instead of blocking on the lock. A blocked statement holds a snapshot while it waits. `CREATE INDEX CONCURRENTLY` in the other apply waits for every backend that holds a snapshot. That is a cycle. PostgreSQL breaks a cycle by killing one session in it, and it picks the waiter. Advisory locks share a lock manager with table locks, so the deadlock detector sees the cycle. Between attempts, the waiting session is idle and holds no snapshot, so there is no cycle. A lock that becomes free is taken within a second. The queue is not first-come-first-served.

Only apply runs that pass `--exclusive` or `--exclusive-wait` are excluded. DDL from any other source is not blocked.
