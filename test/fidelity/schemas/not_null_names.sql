-- On PostgreSQL 18 a NOT NULL constraint keeps its automatic name when the
-- table or the column is renamed. pista dump writes such a name, so the
-- restored constraint has it too.
CREATE TABLE public.items (
    id integer NOT NULL,
    code text NOT NULL
);

ALTER TABLE public.items RENAME TO goods;

ALTER TABLE public.goods RENAME COLUMN code TO sku;

CREATE TABLE public.users (
    email text CONSTRAINT email_not_null NOT NULL
);
