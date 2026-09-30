# Guides

Each guide covers one task. [Commands](../reference/commands/index.md) lists every option and [Supported objects](../reference/objects.md) lists every object kind.

## Writing the schema

- [Working with multiple schemas](multiple-schemas.md): targeting a schema other than `public`, files without schema names, mapping names.
- [Renaming objects](renaming.md): a rename instead of a drop and a create.
- [Running arbitrary SQL](executing-sql.md): statements that pistachio does not manage, run before or after the DDL.

## Running plan and apply

- [Controlling drops](drops.md): what `--allow-drop` gates.
- [Filtering what is managed](filtering.md): by name, by object type, one object, partitions.
- [Transactions and locks](transactions.md): `--with-tx`, index builds that do not block writes, lock timeouts.
- [Explaining a plan](explaining-plans.md): what each statement scans or rewrites, and what it blocks.
- [Plan files](plan-files.md): plan on one machine, apply on another.
- [Preventing concurrent applies](exclusive-apply.md): one apply at a time per database.

## Without a database

- [Diffing schema files](diffing.md): the DDL between two files or two git revisions.
- [Formatting schema files](formatting.md): `pista fmt`.
- [Parsing schema files](parsing.md): the schema as JSON.
