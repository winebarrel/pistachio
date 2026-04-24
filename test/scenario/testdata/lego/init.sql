-- public.lego_colors
CREATE TABLE public.lego_colors (
    id serial NOT NULL,
    name character varying(255) NOT NULL,
    rgb character varying(6) NOT NULL,
    is_trans character(1) NOT NULL,
    CONSTRAINT lego_colors_pkey PRIMARY KEY (id)
);

-- public.lego_inventories
CREATE TABLE public.lego_inventories (
    id serial NOT NULL,
    version integer NOT NULL,
    set_num character varying(255) NOT NULL,
    CONSTRAINT lego_inventories_pkey PRIMARY KEY (id)
);

-- public.lego_inventory_parts
CREATE TABLE public.lego_inventory_parts (
    inventory_id integer NOT NULL,
    part_num character varying(255) NOT NULL,
    color_id integer NOT NULL,
    quantity integer NOT NULL,
    is_spare boolean NOT NULL
);

-- public.lego_inventory_sets
CREATE TABLE public.lego_inventory_sets (
    inventory_id integer NOT NULL,
    set_num character varying(255) NOT NULL,
    quantity integer NOT NULL
);

-- public.lego_part_categories
CREATE TABLE public.lego_part_categories (
    id serial NOT NULL,
    name character varying(255) NOT NULL,
    CONSTRAINT lego_part_categories_pkey PRIMARY KEY (id)
);

-- public.lego_parts
CREATE TABLE public.lego_parts (
    part_num character varying(255) NOT NULL,
    name text NOT NULL,
    part_cat_id integer NOT NULL,
    CONSTRAINT lego_parts_pkey PRIMARY KEY (part_num)
);

-- public.lego_sets
CREATE TABLE public.lego_sets (
    set_num character varying(255) NOT NULL,
    name character varying(255) NOT NULL,
    year integer,
    theme_id integer,
    num_parts integer,
    CONSTRAINT lego_sets_pkey PRIMARY KEY (set_num)
);

-- public.lego_themes
CREATE TABLE public.lego_themes (
    id serial NOT NULL,
    name character varying(255) NOT NULL,
    parent_id integer,
    CONSTRAINT lego_themes_pkey PRIMARY KEY (id)
);
