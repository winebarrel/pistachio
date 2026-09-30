# Formatting schema files

`pista fmt` formats schema SQL files. It rewrites each file in place and prints the names of the files that it changed.

```bash
pista fmt schema/*.sql
```

`--check` reports the files that are not formatted, without writing them. It exits with code 2 when there are any. When every file is already formatted, the run prints nothing and exits with 0.

```bash
pista fmt --check schema/*.sql
```

The command reads no database.


## An example

A hand-written table:

```sql
create table "public"."items" (id bigserial
  , "email" text not null
  , tags text [ ]
  , price numeric(10,2) check ( price > 0 )
  , constraint items_pkey primary key ( id ));
```

after `pista fmt`:

```sql
create table public.items (
    id bigserial,
    email text not null,
    tags text[],
    price numeric(10,2) check (price > 0),
    constraint items_pkey primary key (id)
);
```

`fmt` made these changes:

- The definition list is split into lines.
- The commas move to the end of their lines.
- The quotes that the identifiers do not need are removed.
- The spaces inside the parentheses and the array brackets are removed.

The lower-case keywords remain lower-case.


## What it changes

Only the whitespace between the tokens changes:

- Keywords keep their case.
- Identifiers keep their spelling.
- Expressions are left as written.
- No schema name is added to a reference that does not have one.

The definition list of `CREATE TABLE` and `CREATE TYPE` is written one element per line. The comma is at the end of the line, and the closing parenthesis is on a line of its own. A clause after that parenthesis, `INHERITS` or `PARTITION BY` for example, also starts a new line.

```sql
CREATE TABLE public.events (id bigint, at timestamptz) PARTITION BY RANGE (at);
```

```sql
CREATE TABLE public.events (
    id bigint,
    at timestamptz
)
PARTITION BY RANGE (at);
```

A line inside parentheses is indented four spaces more than the line that opened them. The closing parenthesis is aligned with that line again. The clauses of `CREATE FUNCTION` and `CREATE PROCEDURE` are indented one level.

```sql
ALTER TABLE public.items
  ADD CONSTRAINT items_note_check CHECK (
length ( note ) < 100
      );
```

```sql
ALTER TABLE public.items
  ADD CONSTRAINT items_note_check CHECK (
      length (note) < 100
  );
```

The `ADD CONSTRAINT` line is outside the parentheses, so it keeps the two spaces that it was written with. The body is indented relative to it.

Consecutive spaces collapse to one. A space is removed in these places:

- before a comma or a semicolon
- just inside a parenthesis
- around an array subscript
- around a cast

Consecutive blank lines collapse to one. Trailing whitespace is removed. A statement that shares a line with another statement moves to its own line. A statement starts at the first column. So does a comment on a line of its own outside a statement.

A quoted identifier that means the same without its quotes loses them. So `"items"` becomes `items`, while `"Value"`, `"select"` and `"left"` keep their quotes.

Everything else remains where it was written. A statement written on one line remains on one line. A statement broken across several lines keeps its breaks. The definition lists above are the only thing that `fmt` splits on its own. The body of a view is left unchanged, because PostgreSQL writes it back with its own indentation. The body of a routine is left unchanged too, because it is a single quoted token. A statement that pistachio does not manage, a `GRANT` for example, is formatted like any other.

```sql
CREATE OR REPLACE VIEW public.recent AS
 SELECT items.id
   FROM public.items
  WHERE (items.price > 0);

CREATE FUNCTION public.norm(e text) RETURNS text
    LANGUAGE sql
    AS $$   SELECT lower(e)   $$;
```

The view keeps the layout that PostgreSQL gave its body. The routine's clauses are indented, and the spacing inside `$$ ... $$` is left as it is.

Line endings are written as newlines, so a file that uses CRLF comes back with LF.


## What it guarantees

A formatted file has the same tokens as the input. `fmt` checks this before writing. When the check fails, it leaves the file unchanged.

The quoting is the one deliberate exception. An identifier loses its quotes only when the statement means the same after it is parsed again without them. Whether a bare name is legal depends on the position. `"name"` is written bare as a column name, and remains quoted as a function name.

A file that cannot be parsed is reported and left unchanged. The other files on the command line are still formatted. The run exits with 1.


## The relation to dump

`pista dump` writes its output through the same formatter. So a dump needs no formatting, and `fmt` leaves it unchanged. Pass `--no-format` to `dump` to get the layout that the model renders on its own.

Running `fmt` over a hand-written file does not make it identical to a dump of the same schema. It changes the layout, not the spelling of the names or the expressions. Use `dump` for the canonical form.
