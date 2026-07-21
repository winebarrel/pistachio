-- ============================================================
--  pistachio demo: blog/CMS schema (current state)
--  This file is loaded into the 'demo' database at image build.
-- ============================================================

-- Enum: post status
CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived');

-- Domain: simple email validation
CREATE DOMAIN email_address AS text
    CHECK (VALUE ~ '^[^@]+@[^@]+\.[^@]+$');

-- Sequence: public-facing post reference numbers
CREATE SEQUENCE post_ref_seq START 1000;

-- ---------- users ----------
CREATE TABLE users (
    id           bigserial PRIMARY KEY,
    email        email_address NOT NULL UNIQUE,
    display_name text NOT NULL,
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

-- ---------- view ----------
CREATE VIEW published_posts AS
    SELECT p.id, p.title, p.body, p.published_at, u.display_name AS author
    FROM posts p
    JOIN users u ON u.id = p.author_id
    WHERE p.status = 'published';
