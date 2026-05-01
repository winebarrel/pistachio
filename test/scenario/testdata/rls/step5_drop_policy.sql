CREATE TABLE public.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    body text,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY owner_select ON public.documents FOR ALL USING (owner = session_user);
