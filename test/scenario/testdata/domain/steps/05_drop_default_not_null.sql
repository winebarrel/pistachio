CREATE DOMAIN public.email AS text
    CONSTRAINT email_format CHECK ((VALUE ~ '^[^@]+@[^@]+$'::text))
    CONSTRAINT email_length CHECK ((length(VALUE) <= 320))
    CONSTRAINT email_local CHECK ((length(split_part(VALUE, '@'::text, 1)) > 0));

CREATE TABLE public.users (
    id integer NOT NULL,
    addr public.email NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
