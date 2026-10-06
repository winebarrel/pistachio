# Standard lint rules

These are rules for [`pista lint`](https://winebarrel.github.io/pistachio/reference/commands/lint/). pistachio does not load them by itself. Copy the files you want into your repository and change them there.

| File | Rule | Checks |
|---|---|---|
| `keys.yml` | `require-primary-key` | Every table has a primary key. |
| | `prefer-bigint-key` | A primary key column is not `smallint` or `integer`. |
| | `fk-needs-index` | An index, a primary key or a unique constraint starts with the columns of each foreign key. |
| `indexes.yml` | `duplicate-index` | No two indexes have the same columns, `INCLUDE` columns, access method and uniqueness. |
| | `redundant-index` | No B-tree index has columns that another B-tree index starts with. |
| `types.yml` | `prefer-timestamptz` | No column is `timestamp without time zone`. |
| | `no-timetz` | No column is `time with time zone`. |
| | `prefer-text` | No column is `varchar` or `char`. |
| | `no-money` | No column is `money`. |
| | `prefer-identity` | No column is `serial`, `smallserial` or `bigserial`. |
| | `prefer-jsonb` | No column is `json`. |
| `naming.yml` | `snake-case-table` | Every table name is snake_case. |
| | `snake-case-column` | Every column name is snake_case. |

## Usage

1. Copy the files into a directory of your repository, such as `lint/`.
2. Delete the rules you do not want, and change the others to fit your schema.
3. Run the check:

   ```sh
   pista lint -r lint/ schema/*.sql
   ```

   The exit status is 2 when an object breaks a rule.

To skip one object, write `-- pista:lint-ignore <rule>` before it:

```sql
-- pista:lint-ignore require-primary-key
CREATE TABLE public.event_log (body text NOT NULL);
```

[Linting schema files](https://winebarrel.github.io/pistachio/guides/linting/) describes the rule format.
