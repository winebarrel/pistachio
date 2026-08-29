# Running the tests

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

`make test-scenario` runs the CLI scenario tests, and `make test-fidelity`
checks that a `pista dump` reloaded into an empty database produces the schema
it was taken from, comparing `pg_dump` output on both sides. The latter needs a
`pg_dump` at least as new as the server.

