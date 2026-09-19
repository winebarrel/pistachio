-- A domain with a check constraint, used by a table column. Every step
-- reshapes the domain while the dependent column stays in place.
CREATE DOMAIN public.email AS text
    CONSTRAINT email_format CHECK (VALUE ~ '@'::text);

CREATE TABLE public.users (
    id integer NOT NULL,
    addr public.email NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
