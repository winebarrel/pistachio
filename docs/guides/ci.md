# Using pistachio in CI

`plan --check` turns the diff into an exit code: 0 when the database already
matches the desired schema, 2 when it does not, 1 on error. The output does
not change, so the job log still shows the DDL. Drops that are suppressed
generate no executable DDL and exit 0.

```bash
pista plan --check schema.sql
echo $?  # 0: no changes, 2: changes, 1: error
```

`plan` and `dump` open a read-only connection, so a job that only inspects the
database cannot write to it.

## Failing a pull request on drift

Run the check against the database the branch is supposed to describe. The job
fails when the two have diverged, which is what catches a schema file edited
without an apply, or a database changed by hand.

```yaml
name: schema
on: pull_request

jobs:
  drift:
    runs-on: ubuntu-latest
    env:
      PISTA_CONN_STR: ${{ secrets.DATABASE_URL }}
    steps:
      - uses: actions/checkout@v5
      - name: Install pista
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          gh release download --repo winebarrel/pistachio \
            --pattern 'pistachio_*_linux_amd64.tar.gz' --output - \
            | sudo tar xz -C /usr/local/bin pista
      - run: pista plan --check schema.sql
```

Exit code 2 fails the step, so nothing else is needed to turn drift into a red
build.

## Showing the plan on the pull request

`plan` writes SQL to standard output, so posting it as a comment is a pipe.

```yaml
      - id: plan
        run: |
          pista plan schema.sql > plan.sql
      - run: gh pr comment "$PR" --body-file plan.sql
        env:
          PR: ${{ github.event.number }}
          GH_TOKEN: ${{ github.token }}
```

## Applying on merge

`apply` takes the same files. Wrap it in a transaction with `--with-tx` so a
failure halfway leaves nothing behind, or `--try-tx` when the plan may contain
`CONCURRENTLY` index DDL, which PostgreSQL refuses to run inside one.

```yaml
      - run: pista apply --with-tx schema.sql
```

`--pre-sql` runs SQL before the diff, which is where a `statement_timeout` or
`lock_timeout` belongs:

```bash
pista apply --with-tx --pre-sql "SET lock_timeout = '5s';" schema.sql
```

## Keeping the diff reviewable

`dump --split` writes one file per object, so a pull request shows which table
changed rather than one large file. `plan` accepts the directory back:

```bash
pista dump --split ./schema/
pista plan ./schema/*.sql
```
