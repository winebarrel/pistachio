-- Drop: enum (status), domain (pos_int), table (posts), view (active_users), matview (user_stats)
-- Drop column: users.email, users.age

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
