-- pista:renamed-from addr
CREATE TYPE public.address AS (
    -- pista:renamed-from street
    road varchar(200),
    city text
);
