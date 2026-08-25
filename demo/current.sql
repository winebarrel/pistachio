-- ============================================================
--  pistachio demo: blog/CMS schema (current state)
--  This file is loaded into the 'demo' database at image build.
-- ============================================================

-- Enum: post status
CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived');

-- Composite type: author's links
CREATE TYPE social_links AS (
    website text,
    github  text
);

-- Domain: simple email validation
CREATE DOMAIN email_address AS text
    CHECK (VALUE ~ '^[^@]+@[^@]+\.[^@]+$');

-- Sequence: public-facing post reference numbers
CREATE SEQUENCE post_ref_seq START 1000;

-- ---------- functions and procedures ----------
--  Managed declaratively; the demo has $PISTA_MANAGE_ROUTINE set.

CREATE FUNCTION set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE FUNCTION notify_comment() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('comments', NEW.post_id::text);
    RETURN NULL;
END;
$$;

CREATE FUNCTION slugify(t text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT lower(replace(t, ' ', '-')) $$;

COMMENT ON FUNCTION slugify(text) IS 'Tag name -> URL slug';

CREATE FUNCTION post_excerpt(body text, len integer DEFAULT 100) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT left(body, len) $$;

CREATE PROCEDURE refresh_tag_usage()
    LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW tag_usage;
END;
$$;

-- ---------- users ----------
CREATE TABLE users (
    id           bigserial PRIMARY KEY,
    email        email_address NOT NULL UNIQUE,
    display_name text NOT NULL,
    links        social_links,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------- posts ----------
--  foreign key, check constraint, generated column,
--  partial index, row-level security
CREATE TABLE posts (
    id           bigserial PRIMARY KEY,
    author_id    bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title        text NOT NULL,
    body         text NOT NULL,
    status       post_status NOT NULL DEFAULT 'draft',
    word_count   integer GENERATED ALWAYS AS (array_length(string_to_array(body, ' '), 1)) STORED,
    published_at timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX posts_author_id_idx ON posts (author_id);
CREATE INDEX posts_published_idx ON posts (published_at DESC) WHERE status = 'published';

ALTER TABLE posts ENABLE ROW LEVEL SECURITY;

CREATE POLICY posts_visible ON posts
    FOR SELECT
    USING (
        status = 'published'
        OR author_id = current_setting('app.user_id', true)::bigint
    );

COMMENT ON TABLE posts IS 'Blog posts';
COMMENT ON COLUMN posts.status IS 'draft -> published -> archived';

-- ---------- comments ----------
--  trigger on a table, plus a second trigger to switch off later
CREATE TABLE comments (
    id         bigserial PRIMARY KEY,
    post_id    bigint NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    author_id  bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX comments_post_id_idx ON comments (post_id);

CREATE TRIGGER comments_set_updated_at BEFORE UPDATE ON comments
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER comments_notify AFTER INSERT ON comments
    FOR EACH ROW EXECUTE FUNCTION notify_comment();

-- ---------- tags ----------
CREATE TABLE tags (
    id   bigserial PRIMARY KEY,
    name text NOT NULL UNIQUE
);

-- ---------- post_tags (composite PK, multiple FKs) ----------
CREATE TABLE post_tags (
    post_id bigint NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    tag_id  bigint NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);

-- ---------- post_schedules (identity column, exclusion constraint) ----------
CREATE TABLE post_schedules (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    post_id bigint NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    during  tstzrange NOT NULL,
    EXCLUDE USING gist (during WITH &&)
);

-- ---------- post_views (partitioned table) ----------
CREATE TABLE post_views (
    post_id   bigint NOT NULL,
    viewed_on date   NOT NULL,
    views     integer NOT NULL DEFAULT 0,
    PRIMARY KEY (viewed_on, post_id)
) PARTITION BY RANGE (viewed_on);

CREATE TABLE post_views_2025 PARTITION OF post_views
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- ---------- view ----------
CREATE VIEW published_posts AS
    SELECT p.id, p.title, p.body, p.published_at, u.display_name AS author
    FROM posts p
    JOIN users u ON u.id = p.author_id
    WHERE p.status = 'published';

-- ---------- materialized view ----------
CREATE MATERIALIZED VIEW tag_usage AS
    SELECT t.id AS tag_id, t.name, count(pt.post_id) AS post_count
    FROM tags t
    LEFT JOIN post_tags pt ON pt.tag_id = t.id
    GROUP BY t.id, t.name;

CREATE UNIQUE INDEX tag_usage_tag_id_idx ON tag_usage (tag_id);
