CREATE TABLE public.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    body text,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
