package parser

import (
	"testing"

	pgquery "github.com/wasilibs/go-pgquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractStmtDirectives(t *testing.T) {
	t.Run("single directive", func(t *testing.T) {
		sql := `-- pist:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active', 'inactive');`
		result, err := pgquery.Parse(sql)
		require.NoError(t, err)
		dirs := ExtractStmtDirectives(sql, result.Stmts)
		assert.Len(t, dirs, 1)
		assert.Equal(t, "public.old_status", dirs[result.Stmts[0].StmtLocation])
	})

	t.Run("multiple directives", func(t *testing.T) {
		sql := `-- pist:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active');
-- pist:renamed-from public.old_users
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pgquery.Parse(sql)
		require.NoError(t, err)
		dirs := ExtractStmtDirectives(sql, result.Stmts)
		assert.Len(t, dirs, 2)
		assert.Equal(t, "public.old_status", dirs[result.Stmts[0].StmtLocation])
		assert.Equal(t, "public.old_users", dirs[result.Stmts[1].StmtLocation])
	})

	t.Run("no directives", func(t *testing.T) {
		sql := `CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pgquery.Parse(sql)
		require.NoError(t, err)
		dirs := ExtractStmtDirectives(sql, result.Stmts)
		assert.Empty(t, dirs)
	})

	t.Run("directive only on second statement", func(t *testing.T) {
		sql := `CREATE TABLE public.users (id integer NOT NULL);
-- pist:renamed-from public.old_posts
CREATE TABLE public.posts (id integer NOT NULL);`
		result, err := pgquery.Parse(sql)
		require.NoError(t, err)
		dirs := ExtractStmtDirectives(sql, result.Stmts)
		assert.Len(t, dirs, 1)
		assert.Equal(t, "public.old_posts", dirs[result.Stmts[1].StmtLocation])
	})

	t.Run("directive with extra whitespace", func(t *testing.T) {
		sql := `  -- pist:renamed-from  public.old_name
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pgquery.Parse(sql)
		require.NoError(t, err)
		dirs := ExtractStmtDirectives(sql, result.Stmts)
		assert.Equal(t, "public.old_name", dirs[result.Stmts[0].StmtLocation])
	})

	t.Run("unqualified name", func(t *testing.T) {
		sql := `-- pist:renamed-from old_name
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pgquery.Parse(sql)
		require.NoError(t, err)
		dirs := ExtractStmtDirectives(sql, result.Stmts)
		assert.Equal(t, "old_name", dirs[result.Stmts[0].StmtLocation])
	})

	t.Run("whitespace-only directive ignored", func(t *testing.T) {
		sql := `-- pist:renamed-from
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pgquery.Parse(sql)
		require.NoError(t, err)
		dirs := ExtractStmtDirectives(sql, result.Stmts)
		assert.Empty(t, dirs)
	})

	t.Run("regular comment ignored", func(t *testing.T) {
		sql := `-- this is a regular comment
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pgquery.Parse(sql)
		require.NoError(t, err)
		dirs := ExtractStmtDirectives(sql, result.Stmts)
		assert.Empty(t, dirs)
	})
}

func TestExtractColumnDirectives(t *testing.T) {
	t.Run("single column directive", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    -- pist:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
		dirs := ExtractColumnDirectives(sql)
		assert.Len(t, dirs, 1)
		assert.Equal(t, "name", dirs["display_name"])
	})

	t.Run("multiple column directives", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    -- pist:renamed-from uid
    id integer NOT NULL,
    -- pist:renamed-from name
    display_name text NOT NULL
);`
		dirs := ExtractColumnDirectives(sql)
		assert.Len(t, dirs, 2)
		assert.Equal(t, "uid", dirs["id"])
		assert.Equal(t, "name", dirs["display_name"])
	})

	t.Run("no directives", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL
);`
		dirs := ExtractColumnDirectives(sql)
		assert.Empty(t, dirs)
	})

	t.Run("quoted column name", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    -- pist:renamed-from "Old Name"
    "New Name" text NOT NULL
);`
		dirs := ExtractColumnDirectives(sql)
		assert.Equal(t, "Old Name", dirs["New Name"])
	})

	t.Run("constraint line skipped", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    -- pist:renamed-from old_col
    new_col text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
		dirs := ExtractColumnDirectives(sql)
		assert.Len(t, dirs, 1)
		assert.Equal(t, "old_col", dirs["new_col"])
	})
}

func TestExtractInlineDirectives_Constraint(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    code text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pist:renamed-from users_code_key
    CONSTRAINT users_code_unique UNIQUE (code)
);`
	dirs := ExtractInlineDirectives(sql)
	assert.Empty(t, dirs.Columns)
	assert.Len(t, dirs.Constraints, 1)
	assert.Equal(t, "users_code_key", dirs.Constraints["users_code_unique"])
}

func TestExtractInlineDirectives_Mixed(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    -- pist:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pist:renamed-from old_unique
    CONSTRAINT new_unique UNIQUE (display_name)
);`
	dirs := ExtractInlineDirectives(sql)
	assert.Len(t, dirs.Columns, 1)
	assert.Equal(t, "name", dirs.Columns["display_name"])
	assert.Len(t, dirs.Constraints, 1)
	assert.Equal(t, "old_unique", dirs.Constraints["new_unique"])
}

func TestExtractConstraintName(t *testing.T) {
	assert.Equal(t, "users_pkey", extractConstraintName("CONSTRAINT users_pkey PRIMARY KEY (id)"))
	assert.Equal(t, "My Con", extractConstraintName(`CONSTRAINT "My Con" UNIQUE (code)`))
	assert.Equal(t, "", extractConstraintName("id integer NOT NULL"))
	assert.Equal(t, "", extractConstraintName(""))
	// Unquoted names are lowercased
	assert.Equal(t, "users_pkey", extractConstraintName("CONSTRAINT Users_Pkey PRIMARY KEY (id)"))
}

func TestSplitQualifiedName_SpacesAroundDot(t *testing.T) {
	parts := splitQualifiedName("public . old_table")
	assert.Equal(t, []string{"public", "old_table"}, parts)
}

func TestNormalizeDirectiveValue(t *testing.T) {
	assert.Equal(t, "public.old_name", normalizeDirectiveValue("public.old_name"))
	assert.Equal(t, `"Old Name"`, normalizeDirectiveValue(`"Old Name"`))
	assert.Equal(t, `"My Schema"."Old Name"`, normalizeDirectiveValue(`"My Schema"."Old Name"`))
	assert.Equal(t, `public."Old Name"`, normalizeDirectiveValue(`public."Old Name"`))
	assert.Equal(t, `"has""quote"`, normalizeDirectiveValue(`"has""quote"`))
	assert.Equal(t, "simple", normalizeDirectiveValue("simple"))
}

func TestNormalizeUnqualifiedDirective(t *testing.T) {
	assert.Equal(t, "old_name", normalizeUnqualifiedDirective("old_name"))
	assert.Equal(t, "Old Name", normalizeUnqualifiedDirective(`"Old Name"`))
	assert.Equal(t, `has"quote`, normalizeUnqualifiedDirective(`"has""quote"`))
	// Schema-qualified: take last part only
	assert.Equal(t, "old_idx", normalizeUnqualifiedDirective("public.old_idx"))
	assert.Equal(t, "Old Name", normalizeUnqualifiedDirective(`public."Old Name"`))
	// Unquoted names are lowercased
	assert.Equal(t, "oldcolumn", normalizeUnqualifiedDirective("OldColumn"))
}

func TestQualifyRenameFrom(t *testing.T) {
	assert.Equal(t, "public.old_name", QualifyRenameFrom("old_name", "public"))
	assert.Equal(t, "public.old_name", QualifyRenameFrom("public.old_name", "public"))
	assert.Equal(t, "myschema.old_name", QualifyRenameFrom("myschema.old_name", "public"))
	assert.Equal(t, `public."Old Name"`, QualifyRenameFrom(`"Old Name"`, "public"))
	// Quoted identifier containing a dot should be treated as single name
	assert.Equal(t, `public."a.b"`, QualifyRenameFrom(`"a.b"`, "public"))
}

func TestExtractStmtDirectives_QuotedName(t *testing.T) {
	sql := `-- pist:renamed-from "My Schema"."Old Name"
CREATE TABLE public.users (id integer NOT NULL);`
	result, err := pgquery.Parse(sql)
	require.NoError(t, err)
	dirs := ExtractStmtDirectives(sql, result.Stmts)
	assert.Equal(t, `"My Schema"."Old Name"`, dirs[result.Stmts[0].StmtLocation])
}

func TestScanQuotedIdent(t *testing.T) {
	name, ok := scanQuotedIdent(`"My Name" text NOT NULL`)
	assert.True(t, ok)
	assert.Equal(t, "My Name", name)

	name, ok = scanQuotedIdent(`"has""quote" text`)
	assert.True(t, ok)
	assert.Equal(t, `has"quote`, name)

	_, ok = scanQuotedIdent(`not_quoted`)
	assert.False(t, ok)

	_, ok = scanQuotedIdent(`"unterminated`)
	assert.False(t, ok)

	_, ok = scanQuotedIdent(``)
	assert.False(t, ok)
}

func TestExtractColumnName(t *testing.T) {
	assert.Equal(t, "id", extractColumnName("id integer NOT NULL,"))
	assert.Equal(t, "name", extractColumnName("name text NOT NULL"))
	assert.Equal(t, "My Col", extractColumnName(`"My Col" text NOT NULL,`))
	assert.Equal(t, "", extractColumnName("CONSTRAINT users_pkey PRIMARY KEY (id)"))
	assert.Equal(t, "", extractColumnName(""))
	// Unquoted names are lowercased
	assert.Equal(t, "displayname", extractColumnName("DisplayName text NOT NULL"))
}
