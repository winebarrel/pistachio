# Environment variables

Every variable stands for one option and takes the value that the option takes. The option overrides the variable. The two are not merged. A command-line flag overrides an environment variable, which overrides the config file, which overrides the default.

`--schema-map`, `--split`, `--omit-schema`, `--force` and `--pager` have no variable. `PISTA_PAGER` names the pager command, not the flag.

| Variable | Option | Commands |
|---|---|---|
| `PISTA_CONFIG` | `--config` | all |
| `PISTA_PAGER` | the pager command (see [Paging long output](configuration.md#paging-long-output)) | all |
| `PISTA_CONN_STR` | `--conn-string` | plan, apply, apply-from, dump |
| `PISTA_DBNAME` | `--dbname` | plan, apply, apply-from, dump |
| `PISTA_PASSWORD` | `--password` | plan, apply, apply-from, dump |
| `PISTA_SCHEMAS` | `--schemas` | plan, apply, dump, diff, parse |
| `PISTA_SEARCH_PATH` | `--search-path` | plan, apply, dump |
| `PISTA_INCLUDE` | `--include` | plan, apply, dump, diff |
| `PISTA_EXCLUDE` | `--exclude` | plan, apply, dump, diff |
| `PISTA_ENABLE` | `--enable` | plan, apply, dump, diff |
| `PISTA_DISABLE` | `--disable` | plan, apply, dump, diff |
| `PISTA_MANAGE_ROUTINE` | `--manage-routine` | plan, apply, dump, diff |
| `PISTA_MANAGE_STORAGE_PARAM` | `--manage-storage-param` | plan, apply, dump, diff |
| `PISTA_SKIP_PARTITION_CHILD` | `--skip-partition-child` | plan, apply, dump, diff |
| `PISTA_ALLOW_DROP` | `--allow-drop` | plan, apply, diff |
| `PISTA_PRE_SQL` | `--pre-sql` | plan, apply |
| `PISTA_PRE_SQL_FILE` | `--pre-sql-file` | plan, apply |
| `PISTA_CONCURRENTLY_PRE_SQL` | `--concurrently-pre-sql` | plan, apply |
| `PISTA_CONCURRENTLY_PRE_SQL_FILE` | `--concurrently-pre-sql-file` | plan, apply |
| `PISTA_DISABLE_INDEX_CONCURRENTLY` | `--disable-index-concurrently` | plan, apply, diff |
| `PISTA_FORCE_INDEX_CONCURRENTLY` | `--force-index-concurrently` | plan, apply, diff |
| `PISTA_BULK_ALTER` | `--bulk-alter` | plan, apply, diff |
| `PISTA_ASSUME_VALIDATED` | `--assume-validated` | plan, apply, diff |
| `PISTA_NO_READ_ONLY` | `--no-read-only` | plan, dump |
| `PISTA_EXPLAIN` | `--explain` | plan, diff |
| `PISTA_OUT` | `--out` | plan |
| `PISTA_CHECK` | `--check` | plan, diff |
| `PISTA_GIT` | `--git` | diff |
| `PISTA_WITH_TX` | `--with-tx` | apply, apply-from |
| `PISTA_TRY_TX` | `--try-tx` | apply, apply-from |
| `PISTA_TIMING` | `--timing` | apply, apply-from |
| `PISTA_EXCLUSIVE` | `--exclusive` | apply, apply-from |
| `PISTA_EXCLUSIVE_WAIT` | `--exclusive-wait` | apply, apply-from |
| `PISTA_NO_FORMAT` | `--no-format` | dump |
| `PISTA_DUMP_JSON` | `--json` | dump |
| `PISTA_DUMP_EXPLAIN` | `--explain` | dump |
| `PISTA_DUMP_OMIT_PARTITION_CHILD_INDEX` | `--omit-partition-child-index` | dump |
| `PISTA_FMT_CHECK` | `--check` | fmt |

Each option is described on its command's page under [Commands](commands/index.md).
