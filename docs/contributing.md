# Development

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

`make test-samples` checks pista against real-world schemas downloaded from
their upstream sources: each is loaded into a database of its own, dumped, and
planned back, and the plan has to come out empty. The sample list, the check
itself, and how to add a sample live in
[SAMPLE-DB-TESTS.md](https://github.com/winebarrel/pistachio/blob/main/SAMPLE-DB-TESTS.md).

`make fuzz` runs the fuzz targets: one feeds arbitrary text to the parser and
renders whatever comes back, one diffs a parsed schema against itself and
requires no DDL, and one formats a file and requires formatting the result to
change nothing. None needs a database. `FUZZTIME` sets how long each target
runs and defaults to one minute:

```bash
make FUZZTIME=10m fuzz
```

An input that fails is written under the package's `testdata/fuzz/`, and every
ordinary `make test` replays it from then on, so commit it with the fix.


## The JSON Schema

`docs/json/schema-1.0.json` describes the JSON `pista parse` and `pista dump --json`
write. It is
generated from the structs rather than written by hand.

```sh
make json-schema
```

A test fails when the committed file is not what the generator produces, so run
it after changing a field of `model` or `parser.ParseResult` and commit the
result.
