CREATE DOMAIN public.email AS text
    DEFAULT 'nobody@example.com'::text
    NOT NULL
    CONSTRAINT email_format CHECK ((VALUE ~ '@'::text));

CREATE TABLE public.users (
    id integer NOT NULL,
    addr public.email NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
