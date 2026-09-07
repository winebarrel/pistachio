package format_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/format"
)

func TestFormat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:  "expand a one-line definition list",
			input: `create table public.items (id integer not null, name text);`,
			expected: `create table public.items (
    id integer not null,
    name text
);
`,
		},
		{
			name: "reindent a definition list",
			input: `CREATE TABLE public.items (
id integer NOT NULL,
      name text,
  CONSTRAINT items_pkey PRIMARY KEY (id)
);
`,
			expected: `CREATE TABLE public.items (
    id integer NOT NULL,
    name text,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
`,
		},
		{
			name: "count the nested parentheses of a multi-line CHECK",
			input: `CREATE TABLE public.items (
    v integer,
    CONSTRAINT ck CHECK (
 v > 0
 AND v < 10
    )
);
`,
			expected: `CREATE TABLE public.items (
    v integer,
    CONSTRAINT ck CHECK (
        v > 0
        AND v < 10
    )
);
`,
		},
		{
			name: "move a leading comma to the end of the line",
			input: `CREATE TABLE public.items (
    id integer
  , name text
  , price numeric(10,2)
);
`,
			expected: `CREATE TABLE public.items (
    id integer,
    name text,
    price numeric(10,2)
);
`,
		},
		{
			name:  "keep a one-line statement on one line",
			input: `CREATE INDEX  idx_items_name   ON public.items USING btree ( name ) ;`,
			expected: `CREATE INDEX idx_items_name ON public.items USING btree (name);
`,
		},
		{
			name: "keep the line breaks of a statement with no definition list",
			input: `ALTER TABLE public.items
    ADD CONSTRAINT items_price_check
    CHECK (price > 0);
`,
			expected: `ALTER TABLE public.items
    ADD CONSTRAINT items_price_check
    CHECK (price > 0);
`,
		},
		{
			name: "collapse consecutive blank lines",
			input: `CREATE TABLE public.a (id integer);



CREATE TABLE public.b (id integer);
`,
			expected: `CREATE TABLE public.a (
    id integer
);

CREATE TABLE public.b (
    id integer
);
`,
		},
		{
			name: "keep a comment on the line of the definition it follows",
			input: `-- pista:renamed-from public.old_items
CREATE TABLE public.items (
    id integer, -- the key
    -- the display name
    name text
);
`,
			expected: `-- pista:renamed-from public.old_items
CREATE TABLE public.items (
    id integer, -- the key
    -- the display name
    name text
);
`,
		},
		{
			name: "keep a comma off a line that ends in a comment",
			input: `CREATE TABLE public.cmt (
    id int -- no comma here
    , name text
);
`,
			expected: `CREATE TABLE public.cmt (
    id int -- no comma here
    , name text
);
`,
		},
		{
			name:  "expand a composite type and an enum",
			input: `CREATE TYPE public.address AS (street text, city text);CREATE TYPE public.status AS ENUM ('active', 'inactive');`,
			expected: `CREATE TYPE public.address AS (
    street text,
    city text
);
CREATE TYPE public.status AS ENUM (
    'active',
    'inactive'
);
`,
		},
		{
			name:  "remove the quotes an identifier does not need",
			input: `CREATE TABLE "public"."items" ("id" integer, "note" text, "Value" text);`,
			expected: `CREATE TABLE public.items (
    id integer,
    note text,
    "Value" text
);
`,
		},
		{
			name:  "keep the quotes a reserved or type_func_name keyword needs",
			input: `CREATE TABLE public."left" ("name" text, "select" text);`,
			expected: `CREATE TABLE public."left" (
    name text,
    "select" text
);
`,
		},
		{
			name:  "keep the quotes a function name needs",
			input: `CREATE INDEX i ON public.t ((    "precision"(v)));`,
			expected: `CREATE INDEX i ON public.t (("precision"(v)));
`,
		},
		{
			name: "hang an indented block off the line that opened it",
			input: `ALTER TABLE public.items
        ADD CONSTRAINT ck CHECK (
price > 0
);
`,
			expected: `ALTER TABLE public.items
        ADD CONSTRAINT ck CHECK (
            price > 0
        );
`,
		},
		{
			name: "leave a view body with a subquery alone",
			input: `CREATE OR REPLACE VIEW public.v AS
 SELECT t.id,
    ( SELECT max(x.amount) AS max
           FROM public.t x
          WHERE x.id <> t.id) AS peak
   FROM public.t t;
`,
			expected: `CREATE OR REPLACE VIEW public.v AS
 SELECT t.id,
    ( SELECT max(x.amount) AS max
           FROM public.t x
          WHERE x.id <> t.id) AS peak
   FROM public.t t;
`,
		},
		{
			name:  "format a statement pistachio does not read",
			input: `GRANT   SELECT   ON  public.items   TO  "readonly" ;`,
			expected: `GRANT SELECT ON public.items TO readonly;
`,
		},
		{
			name: "leave a view body alone",
			input: `CREATE OR REPLACE VIEW public.v AS
SELECT users.id,
    users.name
   FROM users;
`,
			expected: `CREATE OR REPLACE VIEW public.v AS
SELECT users.id,
    users.name
   FROM users;
`,
		},
		{
			name: "indent the clauses of a routine",
			input: `CREATE OR REPLACE FUNCTION public.normalize(e text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT lower(e) $$;
CREATE FUNCTION public.bump(
a integer,
   b integer DEFAULT 1
) RETURNS integer
  LANGUAGE plpgsql
AS $$
BEGIN
  RETURN a + b;
END;
$$;
`,
			expected: `CREATE OR REPLACE FUNCTION public.normalize(e text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT lower(e) $$;
CREATE FUNCTION public.bump(
    a integer,
    b integer DEFAULT 1
) RETURNS integer
    LANGUAGE plpgsql
    AS $$
BEGIN
  RETURN a + b;
END;
$$;
`,
		},
		{
			name: "leave a routine body alone",
			input: `CREATE OR REPLACE FUNCTION public.f(e text) RETURNS text
    LANGUAGE sql
    AS $$
        SELECT   lower(e)
$$;
`,
			expected: `CREATE OR REPLACE FUNCTION public.f(e text) RETURNS text
    LANGUAGE sql
    AS $$
        SELECT   lower(e)
$$;
`,
		},
		{
			name:     "keep a statement that carries no definition list",
			input:    `CREATE TABLE public.items_2026 PARTITION OF public.items FOR VALUES FROM (1) TO (2);`,
			expected: "CREATE TABLE public.items_2026 PARTITION OF public.items FOR VALUES FROM (1) TO (2);\n",
		},
		{
			name:  "write an empty definition list on two lines",
			input: `CREATE TABLE public.boxes () INHERITS (public.shapes);`,
			expected: `CREATE TABLE public.boxes (
)
INHERITS (public.shapes);
`,
		},
		{
			name:  "break before the clauses that follow a definition list",
			input: `CREATE TABLE public.events (id bigint, at timestamp with time zone) PARTITION BY RANGE (at);`,
			expected: `CREATE TABLE public.events (
    id bigint,
    at timestamp with time zone
)
PARTITION BY RANGE (at);
`,
		},
		{
			name: "close up an array type and a cast",
			input: `CREATE TABLE public.t (
    tags text [ ],
    m text [ 3 ] [ 3 ],
    v integer DEFAULT ( 1 ) :: integer
);
CREATE INDEX i ON public.t ((v :: bigint));
`,
			expected: `CREATE TABLE public.t (
    tags text[],
    m text[3][3],
    v integer DEFAULT (1)::integer
);
CREATE INDEX i ON public.t ((v::bigint));
`,
		},
		{
			name:  "unquote what it can when one identifier in the statement resists",
			input: `CREATE INDEX i ON public.t (("precision"("note")));`,
			expected: `CREATE INDEX i ON public.t (("precision"(note)));
`,
		},
		{
			name:  "read the definition list of a CREATE TABLE IF NOT EXISTS",
			input: `CREATE TABLE IF NOT EXISTS public.t2 ("id" int, v text)`,
			expected: `CREATE TABLE IF NOT EXISTS public.t2 (
    id int,
    v text
)
`,
		},
		{
			name:  "remove the quotes of an identifier that holds a dollar sign",
			input: `CREATE TABLE public.t3 ("a$b" int);`,
			expected: `CREATE TABLE public.t3 (
    a$b int
);
`,
		},
		{
			name:  "leave the column options of a typed table inline",
			input: `CREATE TABLE public.oft OF public.ty ( id WITH OPTIONS NOT NULL );`,
			expected: `CREATE TABLE public.oft OF public.ty (id WITH OPTIONS NOT NULL);
`,
		},
		{
			name: "leave the body of a materialized view alone",
			input: `CREATE MATERIALIZED VIEW public.mv AS
 SELECT a.id,
    ( SELECT 1) AS x
   FROM public.a a;
`,
			expected: `CREATE MATERIALIZED VIEW public.mv AS
 SELECT a.id,
    ( SELECT 1) AS x
   FROM public.a a;
`,
		},
		{
			name:  "break before the storage parameters and the tablespace",
			input: `CREATE UNLOGGED TABLE public.cache ( k text, v jsonb ) WITH ( fillfactor = 70 ) TABLESPACE fast;`,
			expected: `CREATE UNLOGGED TABLE public.cache (
    k text,
    v jsonb
)
WITH (fillfactor = 70) TABLESPACE fast;
`,
		},
		{
			name:  "start a statement that follows a block comment on its own line",
			input: `/* note */ CREATE TABLE public.b1 ( id int ); -- tail`,
			expected: `/* note */
CREATE TABLE public.b1 (
    id int
); -- tail
`,
		},
		{
			name:     "replace a tab that indents a definition",
			input:    "CREATE TABLE public.tabs (\n\tid int,\n\t\tv text\n);\n",
			expected: "CREATE TABLE public.tabs (\n    id int,\n    v text\n);\n",
		},
		{
			name:     "write CRLF input back with newlines",
			input:    "CREATE TABLE public.a (\r\n  id int\r\n);\r\n",
			expected: "CREATE TABLE public.a (\n    id int\n);\n",
		},
		{
			name: "leave a routine body written as a string alone",
			input: `CREATE FUNCTION public.f(e text) RETURNS text LANGUAGE sql
AS ' SELECT lower ( e ) ';
`,
			expected: `CREATE FUNCTION public.f(e text) RETURNS text LANGUAGE sql
    AS ' SELECT lower ( e ) ';
`,
		},
		{
			name:     "strip trailing whitespace",
			input:    "CREATE INDEX i ON public.t (v);   \n",
			expected: "CREATE INDEX i ON public.t (v);\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := format.Format(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, out)

			again, err := format.Format(out)
			require.NoError(t, err)
			assert.Equal(t, out, again, "format is not idempotent")
		})
	}
}

func TestFormat_ParseError(t *testing.T) {
	_, err := format.Format("CREATE TABLE (;")
	assert.Error(t, err)
}

func TestFormat_ScanError(t *testing.T) {
	tests := map[string]string{
		"an unterminated string":       "SELECT 'abc",
		"an unterminated dollar quote": "SELECT $$x",
		"an unterminated comment":      "/* x",
	}

	for name, sql := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := format.Format(sql)
			assert.ErrorContains(t, err, "failed to scan SQL")
		})
	}
}

func TestFormat_Empty(t *testing.T) {
	out, err := format.Format("")
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestFormat_CommentOnly(t *testing.T) {
	out, err := format.Format("-- just a comment\n")
	require.NoError(t, err)
	assert.Equal(t, "-- just a comment\n", out)
}
