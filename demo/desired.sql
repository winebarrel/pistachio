-- ============================================================
--  pistachio demo: blog/CMS schema (desired state)
--  Edit this file freely, then run:  pista plan desired.sql
-- ============================================================

-- Added 'pinned' to the enum
CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived', 'pinned');

CREATE TYPE social_links AS (
    website text,
    github  text
);

CREATE DOMAIN email_address AS text
    CHECK (VALUE ~ '^[^@]+@[^@]+\.[^@]+$');

-- Sequence: post_ref_seq, increment bumped to 10
CREATE SEQUENCE post_ref_seq START 1000 INCREMENT BY 10;

-- ---------- trigger functions ----------
--  Not managed by pistachio; created only when missing.

-- pista:execute-first SELECT to_regprocedure('public.set_updated_at()') IS NULL
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

-- pista:execute-first SELECT to_regprocedure('public.notify_comment()') IS NULL
CREATE OR REPLACE FUNCTION notify_comment() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('comments', NEW.post_id::text);
    RETURN NULL;
END;
$$;

-- ---------- users ----------
--  +avatar_url
CREATE TABLE users (
    id           bigserial PRIMARY KEY,
    email        email_address NOT NULL UNIQUE,
    display_name text NOT NULL,
    links        social_links,
    avatar_url   text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------- posts ----------
--  -word_count, +updated_at, +posts_created_idx, +posts_modifiable policy,
--  +posts_set_updated_at trigger, reworded status comment
CREATE TABLE posts (
    id           bigserial PRIMARY KEY,
    author_id    bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title        text NOT NULL,
    body         text NOT NULL,
    status       post_status NOT NULL DEFAULT 'draft',
    published_at timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX posts_author_id_idx ON posts (author_id);
CREATE INDEX posts_published_idx ON posts (published_at DESC) WHERE status = 'published';
CREATE INDEX posts_created_idx   ON posts (created_at  DESC);

ALTER TABLE posts ENABLE ROW LEVEL SECURITY;

CREATE POLICY posts_visible ON posts
    FOR SELECT
    USING (
        status IN ('published', 'pinned')
        OR author_id = current_setting('app.user_id', true)::bigint
    );

CREATE POLICY posts_modifiable ON posts
    FOR UPDATE
    USING (author_id = current_setting('app.user_id', true)::bigint);

CREATE TRIGGER posts_set_updated_at BEFORE UPDATE ON posts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE posts IS 'Blog posts';
COMMENT ON COLUMN posts.status IS 'draft -> published -> archived, or pinned';

-- ---------- comments ----------
--  comments_set_updated_at now fires on INSERT too,
--  comments_notify is switched off, +table comment
CREATE TABLE comments (
    id         bigserial PRIMARY KEY,
    post_id    bigint NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    author_id  bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX comments_post_id_idx ON comments (post_id);

CREATE TRIGGER comments_set_updated_at BEFORE INSERT OR UPDATE ON comments
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER comments_notify AFTER INSERT ON comments
    FOR EACH ROW EXECUTE FUNCTION notify_comment();

ALTER TABLE comments DISABLE TRIGGER comments_notify;

COMMENT ON TABLE comments IS 'Reader comments on a post';

-- ---------- tags ----------
--  +slug
CREATE TABLE tags (
    id   bigserial PRIMARY KEY,
    name text NOT NULL UNIQUE,
    slug text NOT NULL UNIQUE
);

-- ---------- post_tags ----------
CREATE TABLE post_tags (
    post_id bigint NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    tag_id  bigint NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);

-- ---------- post_schedules ----------
CREATE TABLE post_schedules (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    post_id bigint NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    during  tstzrange NOT NULL,
    EXCLUDE USING gist (during WITH &&)
);

-- ---------- post_views ----------
--  +post_views_2026 partition
CREATE TABLE post_views (
    post_id   bigint NOT NULL,
    viewed_on date   NOT NULL,
    views     integer NOT NULL DEFAULT 0,
    PRIMARY KEY (viewed_on, post_id)
) PARTITION BY RANGE (viewed_on);

CREATE TABLE post_views_2025 PARTITION OF post_views
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE post_views_2026 PARTITION OF post_views
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- ---------- view ----------
--  include updated_at (appended at the end so CREATE OR REPLACE works)
CREATE VIEW published_posts AS
    SELECT p.id, p.title, p.body, p.published_at, u.display_name AS author, p.updated_at
    FROM posts p
    JOIN users u ON u.id = p.author_id
    WHERE p.status = 'published';

-- ---------- materialized view ----------
--  +tag_usage_post_count_idx
CREATE MATERIALIZED VIEW tag_usage AS
    SELECT t.id AS tag_id, t.name, count(pt.post_id) AS post_count
    FROM tags t
    LEFT JOIN post_tags pt ON pt.tag_id = t.id
    GROUP BY t.id, t.name;

CREATE UNIQUE INDEX tag_usage_tag_id_idx ON tag_usage (tag_id);
CREATE INDEX tag_usage_post_count_idx ON tag_usage (post_count DESC);
