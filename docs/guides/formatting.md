# Formatting schema files

`pista fmt` lays out schema SQL files. It rewrites each file in place and prints the name of the ones it changed.

```bash
pista fmt schema/*.sql
```

`--check` reports the files that are not formatted without writing them, and exits with code 2 when there are any. A run where every file is already formatted prints nothing and exits with 0.

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

The definition list is broken up, the commas move to the end of their lines, the quotes the identifiers do not need are gone, and the spaces inside the parentheses and the array brackets are closed up. The lower-case keywords stay lower-case.


## What it changes

Only the whitespace between the tokens moves. Keywords keep their case, identifiers keep their spelling, expressions are left as written, and no schema name is added to a reference that does not carry one.

The definition list of `CREATE TABLE` and `CREATE TYPE` is written one element per line, with the comma at the end of the line and the closing parenthesis on a line of its own. A clause after that parenthesis, `INHERITS` or `PARTITION BY` for example, starts a new line too.

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

A line inside parentheses is indented four spaces past the line that opened them, and the closing parenthesis lines up with that line again. The clauses of `CREATE FUNCTION` and `CREATE PROCEDURE` are indented one level.

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

The `ADD CONSTRAINT` line is outside the parentheses, so it keeps the two spaces it was written with, and the body hangs off it.

Consecutive spaces collapse to one, and the space before a comma or a semicolon, just inside a parenthesis, around an array subscript, and around a cast is closed up. Consecutive blank lines collapse to one, trailing whitespace goes, and a statement that shares a line with another moves to its own line.

A quoted identifier that reads the same without its quotes loses them, so `"items"` becomes `items` while `"Value"`, `"select"` and `"left"` keep theirs.

Everything else stays where it was written. A statement written on one line stays on one line, and one broken across several keeps its breaks; the definition lists above are the only thing `fmt` breaks up on its own. The body of a view is left alone, since PostgreSQL writes it back with an indentation of its own, and so is the body of a routine, which is a single quoted token. A statement pistachio does not manage, a `GRANT` for example, is formatted like any other.

```sql
CREATE OR REPLACE VIEW public.recent AS
 SELECT items.id
   FROM public.items
  WHERE (items.price > 0);

CREATE FUNCTION public.norm(e text) RETURNS text
    LANGUAGE sql
    AS $$   SELECT lower(e)   $$;
```

The view keeps the layout PostgreSQL gave its body. The routine's clauses are indented, and the spacing inside `$$ ... $$` is left as it is.

Line endings are written as newlines, so a file that uses CRLF comes back with LF.


## What it guarantees

A formatted file carries the same tokens as the input. `fmt` checks this before writing, and leaves the file alone when the check fails.

The quoting is the one deliberate exception. An identifier loses its quotes only when the statement, parsed again without them, means what it meant before. Where a bare name is legal depends on the position: `"name"` is written bare as a column name and stays quoted as a function name.

A file that cannot be parsed is reported and left as it was. The other files on the command line are still formatted, and the run exits with 1.


## The relation to dump

`pista dump` writes its output through the same formatter, so a dump needs no formatting and `fmt` leaves it untouched. Pass `--no-format` to `dump` for the layout the model renders on its own.

Running `fmt` over a hand-written file does not make it identical to a dump of the same schema. It changes the layout, not the spelling of the names or the expressions. Use `dump` for the canonical form.
