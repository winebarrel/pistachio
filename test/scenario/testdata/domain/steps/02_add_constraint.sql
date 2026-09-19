CREATE DOMAIN public.email AS text
    DEFAULT 'nobody@example.com'::text
    NOT NULL
    CONSTRAINT email_format CHECK ((VALUE ~ '@'::text))
    CONSTRAINT email_length CHECK ((length(VALUE) <= 320));

CREATE TABLE public.users (
    id integer NOT NULL,
    addr public.email NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
