package format_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/format"
)

func TestStripRenamedFrom(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "every place a directive goes",
			input: `-- pista:renamed-from public.old_users
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    -- pista:renamed-from users_name_key
    CONSTRAINT users_display_name_key UNIQUE (display_name)
);

-- pista:renamed-from idx_users_name
CREATE INDEX idx_users_display_name ON public.users (display_name);

CREATE TYPE public.status AS ENUM (
    'active',
    -- pista:renamed-from 'inactive'
    'disabled'
);

-- pista:renamed-from public.address
CREATE TYPE public.postal_address AS (
    -- pista:renamed-from street
    road text
);
`,
			expected: `CREATE TABLE public.users (
    id integer NOT NULL,
    display_name text NOT NULL,
    CONSTRAINT users_display_name_key UNIQUE (display_name)
);

CREATE INDEX idx_users_display_name ON public.users (display_name);

CREATE TYPE public.status AS ENUM (
    'active',
    'disabled'
);

CREATE TYPE public.postal_address AS (
    road text
);
`,
		},
		{
			name: "other comments and directives stay",
			input: `-- the users
-- pista:execute
-- pista:renamed-from old_users
CREATE TABLE users (id integer);
`,
			expected: `-- the users
-- pista:execute
CREATE TABLE users (id integer);
`,
		},
		{
			name: "spacing and no argument",
			input: `  --pista:renamed-from   old_users   
	--   pista:renamed-from
CREATE TABLE users (id integer);
`,
			expected: `CREATE TABLE users (id integer);
`,
		},
		{
			name:     "a comment after code on the same line stays",
			input:    "CREATE TABLE users (id integer); -- pista:renamed-from old_users\n",
			expected: "CREATE TABLE users (id integer); -- pista:renamed-from old_users\n",
		},
		{
			name:     "a longer directive name stays",
			input:    "-- pista:renamed-fromx old_users\nCREATE TABLE users (id integer);\n",
			expected: "-- pista:renamed-fromx old_users\nCREATE TABLE users (id integer);\n",
		},
		{
			name: "text inside a string or a body stays",
			input: `CREATE FUNCTION f() RETURNS text LANGUAGE sql AS $$
-- pista:renamed-from g
SELECT 'x'
$$;
COMMENT ON TABLE users IS '
-- pista:renamed-from old_users
';
`,
			expected: `CREATE FUNCTION f() RETURNS text LANGUAGE sql AS $$
-- pista:renamed-from g
SELECT 'x'
$$;
COMMENT ON TABLE users IS '
-- pista:renamed-from old_users
';
`,
		},
		{
			name: "consecutive directives and one before a closing parenthesis",
			input: `CREATE TABLE users (
	id integer
	-- pista:renamed-from old_id
	-- pista:renamed-from older_id
);
`,
			expected: `CREATE TABLE users (
	id integer
);
`,
		},
		{
			name: "a block comment stays",
			input: `/* -- pista:renamed-from old_users */
/*
-- pista:renamed-from old_users
*/ -- pista:renamed-from old_users
CREATE TABLE users (id integer);
`,
			expected: `/* -- pista:renamed-from old_users */
/*
-- pista:renamed-from old_users
*/ -- pista:renamed-from old_users
CREATE TABLE users (id integer);
`,
		},
		{
			name:     "only directives",
			input:    "-- pista:renamed-from a\n-- pista:renamed-from b\n",
			expected: "",
		},
		{
			name:     "CRLF",
			input:    "-- pista:renamed-from old_users\r\nCREATE TABLE users (id integer);\r\n",
			expected: "CREATE TABLE users (id integer);\r\n",
		},
		{
			name:     "the last line with no newline",
			input:    "CREATE TABLE users (id integer);\n-- pista:renamed-from old_users",
			expected: "CREATE TABLE users (id integer);\n",
		},
		{
			name:     "nothing to strip",
			input:    "CREATE TABLE users (id integer);\n",
			expected: "CREATE TABLE users (id integer);\n",
		},
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := format.StripRenamedFrom(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, out)
		})
	}
}

func TestStripRenamedFrom_ScanError(t *testing.T) {
	_, err := format.StripRenamedFrom("SELECT 'abc")
	assert.Error(t, err)
}
