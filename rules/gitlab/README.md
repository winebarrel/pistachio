# Standard lint rules: gitlab

These are rules for [`pista lint`](https://winebarrel.github.io/pistachio/reference/commands/lint/). pistachio does not load them by itself. Copy the files you want into your repository and change them there.

The rules follow the [GitLab database guidelines](https://docs.gitlab.com/development/database/). They cover the parts of the guidelines that a rule can check in a schema file.

| File | Rule | Checks |
|---|---|---|
| `naming.yml` | `pk-name` | A primary key is named `pk_<table>`. |
| | `unique-name` | A unique constraint is named `unique_<table>_<column>[_and_<column>]*`. |
| | `check-name` | A check constraint name starts with `check_<table>_`. |
| | `exclusion-name` | An exclusion constraint name starts with `excl_<table>_`. |
| | `fk-name` | A foreign key is named `fk_<table>_<column>[_and_<column>]*_<referenced table>`. |
| | `index-name` | An index is named `index_<table>_on_<column>[_and_<column>]*`. A partial index can add more `_and_<column>` for its `WHERE` clause. An index on an expression is skipped. |
| | `lowercase-index-name` | An index name has no uppercase letter. |
| `foreign_keys.yml` | `fk-bigint` | The columns of a foreign key are `bigint`. |
| | `fk-on-delete` | A foreign key has an `ON DELETE` action other than `NO ACTION`. |
| | `id-column-needs-fk` | A column whose name ends in `_id` is in a foreign key. |
| `types.yml` | `text-needs-limit` | A `text` column has a check constraint that contains `char_length(<column>) <=`. An array and a domain are skipped. |
| | `no-enum-type` | No column is of an enum type, or an array or a domain of one. |

The naming rules skip an object declared without a name. To require a name, use the `named-*` rules in [`../pgwiki/naming.yml`](../pgwiki/naming.yml).

The guidelines also require an index on the columns of each foreign key. `fk-needs-index` in [`../pgwiki/keys.yml`](../pgwiki/keys.yml) checks it.

These parts of the guidelines have no rule:

- A name is at most 63 characters. The parser truncates a longer name, as PostgreSQL does, so a rule never sees one.
- `ON DELETE` is written. The parser does not tell an omitted `ON DELETE` from `ON DELETE NO ACTION`, so `fk-on-delete` refuses both.
- A check constraint name holds its columns. The parser does not record the columns of a check constraint, so `check-name` checks the prefix only.
- A SHA1 hash is stored as `bytea`, and the columns of a table are ordered to reduce padding. A rule cannot tell these from the schema alone.

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

The rules can be used together with those in [`../pgwiki/`](../pgwiki/). No rule name appears in both directories.

[Linting schema files](https://winebarrel.github.io/pistachio/guides/linting/) describes the rule format.
