# Contributing

```bash
docker compose up -d
make test
```

`compose.yaml` has one service per PostgreSQL version in the CI matrix. Each
service is published on its own port, so several versions can run side by side:

```bash
docker compose up -d               # 15 only, on port 5415
docker compose up -d pg17          # 17 only, on port 5417
docker compose --profile all up -d # 15, 16, 17 and 18
```

`PGPORT` selects the version that the tests use. It defaults to 5415:

```bash
make PGPORT=5417 test
```

`make test-scenario` runs the CLI scenario tests. `make test-fidelity` loads a
`pista dump` into an empty database and checks that it produces the schema it
was taken from. It compares `pg_dump` output on both sides. `make test-fidelity`
needs a `pg_dump` at least as new as the server.

`make test-samples` checks pista against real-world schemas downloaded from
their upstream sources. Each schema is loaded into its own database, dumped, and
planned back. The plan must be empty. `SAMPLE=lemmy,cratesio` limits the run to
the named samples.
[SAMPLE-DB-TESTS.md](https://github.com/winebarrel/pistachio/blob/main/SAMPLE-DB-TESTS.md)
lists the samples, describes the check, and explains how to add a sample.

`make fuzz` runs the fuzz targets:

- One feeds arbitrary text to the parser and renders the result.
- One diffs a parsed schema against itself and requires no DDL.
- One formats a file and requires that formatting the result changes nothing.

None of them needs a database. `FUZZTIME` sets how long each target runs. It
defaults to one minute:

```bash
make FUZZTIME=10m fuzz
```

An input that fails is written under the package's `testdata/fuzz/`. Every
ordinary `make test` replays it from then on. Commit it with the fix.


## The JSON Schema

`docs/json/schema-1.2.json` describes the JSON that `pista parse` and
`pista dump --json` write. It is generated from the structs, not written by
hand.

```sh
make json-schema
```

A test fails when the committed file differs from what the generator produces.
Run the generator after changing a field of `model` or `parser.ParseResult`, and
commit the result.
