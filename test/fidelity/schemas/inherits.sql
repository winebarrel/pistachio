-- No comment on an inherited column: such a comment is dropped at parse time,
-- since an INHERITS child carries only the columns it declares itself.
-- TODO.md records it.
--
-- No child of more than one parent: only the first is read, so the dump names
-- that one alone. TODO.md records it.
CREATE TABLE public.shapes (
    id integer NOT NULL,
    name text,
    CONSTRAINT shapes_name_check CHECK (name <> '')
);
-- A child that declares nothing of its own.
CREATE TABLE public.circles () INHERITS (public.shapes);
-- A child that redeclares an inherited column, which PostgreSQL merges.
CREATE TABLE public.squares (name text NOT NULL) INHERITS (public.shapes);
-- A child with its own column, its own index and its own NOT VALID check.
CREATE TABLE public.boxes (w integer NOT NULL, h integer) INHERITS (public.shapes);
ALTER TABLE public.boxes ADD CONSTRAINT boxes_w_check CHECK (w > 0) NOT VALID;
CREATE INDEX boxes_h_idx ON public.boxes USING btree (h);
