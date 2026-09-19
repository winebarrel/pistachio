-- pista:renamed-from public.email
CREATE DOMAIN public.email_address AS text
    CONSTRAINT email_format CHECK ((VALUE ~ '^[^@]+@[^@]+$'::text));

CREATE TABLE public.users (
    id integer NOT NULL,
    addr public.email_address NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
