CREATE TABLE public.notes (
    id integer NOT NULL,
    body text,
    CONSTRAINT notes_pkey PRIMARY KEY (id)
);
CREATE VIEW public.recent_notes AS SELECT notes.id, notes.body FROM public.notes;
COMMENT ON TABLE public.notes IS 'user notes';
COMMENT ON COLUMN public.notes.body IS 'the text';
COMMENT ON VIEW public.recent_notes IS 'a view';
