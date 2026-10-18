# gitlab lint rules

These are rules for [`pista lint`](https://winebarrel.github.io/pistachio/reference/commands/lint/). pistachio does not load them by itself. Copy the files you want into your repository and change them there.

The rules follow the [GitLab database guidelines](https://docs.gitlab.com/development/database/).

| File | Rule | Checks |
|---|---|---|
| `naming.yml` | `pk-name` | A primary key is named `pk_<table>`. |
| | `unique-name` | A unique constraint is named `unique_<table>_<column>[_and_<column>]*`. |
| | `check-name` | A check constraint name starts with `check_<table>_`. |
| | `exclusion-name` | An exclusion constraint name starts with `excl_<table>_`. |
| | `fk-name` | A foreign key is named `fk_<table>_<column>[_and_<column>]*_<referenced table>`. |
| | `index-name` | An index is named `index_<table>_on_<column>[_and_<column>]*`. A partial index can add `_and_<column>` for its `WHERE` clause. An index on an expression is skipped. |
| | `lowercase-index-name` | An index name is lowercase. |
| `foreign_keys.yml` | `fk-bigint` | No foreign key column is `smallint` or `integer`. |
| | `fk-on-delete` | A foreign key has `ON DELETE` other than `NO ACTION`. |
| | `id-column-needs-fk` | A column whose name ends in `_id` has a foreign key. |
| `types.yml` | `text-needs-limit` | A `text` column has a check constraint with `char_length(<column>) <=`. Arrays and domains are skipped. |
| | `no-enum-type` | No column uses an enum type. |

The naming rules skip objects that have no name in the file. To require names, use the `named-*` rules in [`../pgwiki/naming.yml`](../pgwiki/naming.yml).

For an index on each foreign key, use `fk-needs-index` in [`../pgwiki/keys.yml`](../pgwiki/keys.yml).

Some parts of the guidelines are not checked:

- Names of 63 characters or less. The parser truncates longer names, as PostgreSQL does. A name that is truncated or shortened to fit does not match what the naming rules expect, so write `-- pista:lint-ignore` before the object.
- Writing `ON DELETE`. The parser reads an omitted `ON DELETE` as `NO ACTION`, so `fk-on-delete` rejects both.
- Column names in a check constraint name. The parser does not record the columns of a check constraint, so `check-name` checks only the prefix.
- SHA1 hashes stored as `bytea`, and column order. The schema alone does not show these.

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
CREATE TABLE public.payments (
    id bigint NOT NULL,
    -- pista:lint-ignore id-column-needs-fk -- an ID in another service
    stripe_id bigint,
    CONSTRAINT pk_payments PRIMARY KEY (id)
);
```

You can use these rules with those in [`../pgwiki/`](../pgwiki/). The rule names do not overlap.

[Linting schema files](https://winebarrel.github.io/pistachio/guides/linting/) describes the rule format.
