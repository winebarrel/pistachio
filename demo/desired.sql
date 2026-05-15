-- ============================================================
--  pistachio demo: blog/CMS schema (desired state)
--  Edit this file freely, then run:  pista plan desired.sql
-- ============================================================

-- Added 'pinned' to the enum
CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived', 'pinned');

CREATE DOMAIN email_address AS text
    CHECK (VALUE ~ '^[^@]+@[^@]+\.[^@]+$');

-- ---------- users ----------
--  +avatar_url
CREATE TABLE users (
    id           bigserial PRIMARY KEY,
    email        email_address NOT NULL UNIQUE,
    display_name text NOT NULL,
    avatar_url   text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------- posts ----------
--  -word_count, +updated_at, +posts_created_idx, +posts_modifiable policy
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

-- ---------- comments ----------
CREATE TABLE comments (
    id         bigserial PRIMARY KEY,
    post_id    bigint NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    author_id  bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX comments_post_id_idx ON comments (post_id);

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

-- ---------- view ----------
--  include updated_at (appended at the end so CREATE OR REPLACE works)
CREATE VIEW published_posts AS
    SELECT p.id, p.title, p.body, p.published_at, u.display_name AS author, p.updated_at
    FROM posts p
    JOIN users u ON u.id = p.author_id
    WHERE p.status = 'published'::post_status;
