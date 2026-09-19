-- pista:renamed-from public.email
CREATE DOMAIN public.email_address AS text
    CONSTRAINT email_format CHECK ((VALUE ~ '^[^@]+@[^@]+$'::text))
    CONSTRAINT email_length CHECK ((length(VALUE) <= 320))
    CONSTRAINT email_local CHECK ((length(split_part(VALUE, '@'::text, 1)) > 0));

CREATE TABLE public.users (
    id integer NOT NULL,
    addr public.email_address NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
