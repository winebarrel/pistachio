-- pista:renamed-from addr
CREATE TYPE public.address AS (
    -- pista:renamed-from street
    road varchar(200),
    city text
);
COMMENT ON TYPE public.address IS 'an address';
COMMENT ON COLUMN public.address.city IS 'city';
