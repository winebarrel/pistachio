CREATE TABLE public.bookings (
    id integer NOT NULL,
    during tsrange,
    CONSTRAINT bookings_pkey PRIMARY KEY (id),
    CONSTRAINT bookings_during_excl EXCLUDE USING gist (during WITH &&)
);
