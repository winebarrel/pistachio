# Exit status

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | Error: a connection failure, a file that does not parse, a plan that cannot be computed, a statement that fails during apply, or drift under `apply-from`. |
| 2 | `plan --check` or `diff --check` found executable DDL, or `fmt --check` found a file that is not formatted. |
| 80 | Usage error: an unknown flag, a missing argument, or two options that conflict. |

A skipped drop is not executable DDL, so a plan that holds nothing else exits with 0 under `--check`. The output is the same with and without the flag.

```bash
pista plan --check schema.sql
echo $?  # 0: no changes, 2: changes, 1: error
```
