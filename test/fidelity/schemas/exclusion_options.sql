CREATE TABLE public.slots (
    id integer NOT NULL,
    room integer NOT NULL,
    during int4range NOT NULL,
    active boolean DEFAULT true NOT NULL,
    CONSTRAINT slots_pkey PRIMARY KEY (id),
    CONSTRAINT slots_room_excl EXCLUDE USING gist (during WITH &&) WHERE (active) DEFERRABLE INITIALLY DEFERRED
);
