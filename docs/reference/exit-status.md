# Exit status

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | An error: a connection failure, a file that does not parse, a lint rule that does not compile or evaluate, a plan that cannot be computed, a statement that fails during apply, or drift under `apply-from`. |
| 2 | `plan --check` or `diff --check` found executable DDL, `fmt --check` found a file that is not formatted, or `lint` found an object that a rule is false for. |
| 80 | A usage error: an unknown flag, a missing argument, or two options that conflict. |

A skipped drop is not executable DDL. A plan that contains nothing else exits with 0 under `--check`. The output is the same with and without the flag.

```bash
pista plan --check schema.sql
echo $?  # 0: no changes, 2: changes, 1: error
```
