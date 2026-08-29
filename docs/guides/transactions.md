# Transactions and locks

Wrap apply in a transaction; all changes succeed or fail as a unit:

```bash
pista apply schema.sql --with-tx
```


## Running pre-migration SQL

Execute SQL before applying schema changes (e.g. setting a statement timeout). Use `--pre-sql` for inline SQL or `--pre-sql-file` for a file (mutually exclusive):

```bash
pista apply schema.sql --pre-sql "SET statement_timeout = '5s';" --with-tx
pista apply schema.sql --pre-sql-file pre.sql --with-tx
```

