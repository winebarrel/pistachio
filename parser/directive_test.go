package parser

import (
	"bytes"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractStmtDirectives(t *testing.T) {
	t.Run("single directive", func(t *testing.T) {
		sql := `-- pista:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active', 'inactive');`
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)
		dirs := extractStmtDirectives(sql, result.Stmts)
		assert.Len(t, dirs, 1)
		assert.Equal(t, "public.old_status", dirs[result.Stmts[0].StmtLocation])
	})

	t.Run("multiple directives", func(t *testing.T) {
		sql := `-- pista:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active');
-- pista:renamed-from public.old_users
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)
		dirs := extractStmtDirectives(sql, result.Stmts)
		assert.Len(t, dirs, 2)
		assert.Equal(t, "public.old_status", dirs[result.Stmts[0].StmtLocation])
		assert.Equal(t, "public.old_users", dirs[result.Stmts[1].StmtLocation])
	})

	t.Run("no directives", func(t *testing.T) {
		sql := `CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)
		dirs := extractStmtDirectives(sql, result.Stmts)
		assert.Empty(t, dirs)
	})

	t.Run("directive only on second statement", func(t *testing.T) {
		sql := `CREATE TABLE public.users (id integer NOT NULL);
-- pista:renamed-from public.old_posts
CREATE TABLE public.posts (id integer NOT NULL);`
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)
		dirs := extractStmtDirectives(sql, result.Stmts)
		assert.Len(t, dirs, 1)
		assert.Equal(t, "public.old_posts", dirs[result.Stmts[1].StmtLocation])
	})

	t.Run("directive with extra whitespace", func(t *testing.T) {
		sql := `  -- pista:renamed-from  public.old_name
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)
		dirs := extractStmtDirectives(sql, result.Stmts)
		assert.Equal(t, "public.old_name", dirs[result.Stmts[0].StmtLocation])
	})

	t.Run("unqualified name", func(t *testing.T) {
		sql := `-- pista:renamed-from old_name
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)
		dirs := extractStmtDirectives(sql, result.Stmts)
		assert.Equal(t, "old_name", dirs[result.Stmts[0].StmtLocation])
	})

	t.Run("whitespace-only directive ignored", func(t *testing.T) {
		sql := `-- pista:renamed-from
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)
		dirs := extractStmtDirectives(sql, result.Stmts)
		assert.Empty(t, dirs)
	})

	t.Run("regular comment ignored", func(t *testing.T) {
		sql := `-- this is a regular comment
CREATE TABLE public.users (id integer NOT NULL);`
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)
		dirs := extractStmtDirectives(sql, result.Stmts)
		assert.Empty(t, dirs)
	})
}

func TestExtractInlineDirectives_Columns(t *testing.T) {
	t.Run("single column directive", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
		dirs := extractInlineDirectives(sql).Columns
		assert.Len(t, dirs, 1)
		assert.Equal(t, "name", dirs["display_name"])
	})

	t.Run("multiple column directives", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    -- pista:renamed-from uid
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL
);`
		dirs := extractInlineDirectives(sql).Columns
		assert.Len(t, dirs, 2)
		assert.Equal(t, "uid", dirs["id"])
		assert.Equal(t, "name", dirs["display_name"])
	})

	t.Run("no directives", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL
);`
		dirs := extractInlineDirectives(sql).Columns
		assert.Empty(t, dirs)
	})

	t.Run("quoted column name", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    -- pista:renamed-from "Old Name"
    "New Name" text NOT NULL
);`
		dirs := extractInlineDirectives(sql).Columns
		assert.Equal(t, "Old Name", dirs["New Name"])
	})

	t.Run("constraint line skipped", func(t *testing.T) {
		sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from old_col
    new_col text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
		dirs := extractInlineDirectives(sql).Columns
		assert.Len(t, dirs, 1)
		assert.Equal(t, "old_col", dirs["new_col"])
	})
}

func TestExtractInlineDirectives_Constraint(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    code text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pista:renamed-from users_code_key
    CONSTRAINT users_code_unique UNIQUE (code)
);`
	dirs := extractInlineDirectives(sql)
	assert.Empty(t, dirs.Columns)
	assert.Len(t, dirs.Constraints, 1)
	assert.Equal(t, "users_code_key", dirs.Constraints["users_code_unique"])
}

func TestExtractInlineDirectives_Mixed(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pista:renamed-from old_unique
    CONSTRAINT new_unique UNIQUE (display_name)
);`
	dirs := extractInlineDirectives(sql)
	assert.Len(t, dirs.Columns, 1)
	assert.Equal(t, "name", dirs.Columns["display_name"])
	assert.Len(t, dirs.Constraints, 1)
	assert.Equal(t, "old_unique", dirs.Constraints["new_unique"])
}

func TestExtractConstraintName(t *testing.T) {
	assert.Equal(t, "users_pkey", extractConstraintName("CONSTRAINT users_pkey PRIMARY KEY (id)"))
	assert.Equal(t, "My Con", extractConstraintName(`CONSTRAINT "My Con" UNIQUE (code)`))
	assert.Empty(t, extractConstraintName("id integer NOT NULL"))
	assert.Empty(t, extractConstraintName(""))
	// Unquoted names are lowercased
	assert.Equal(t, "users_pkey", extractConstraintName("CONSTRAINT Users_Pkey PRIMARY KEY (id)"))
	// An unterminated quote yields no name rather than a truncated one
	assert.Empty(t, extractConstraintName(`CONSTRAINT "unterminated UNIQUE (code)`))
	assert.Empty(t, extractConstraintName("CONSTRAINT "))
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
	assert.Equal(t, "public.old_name", qualifyRenameFrom("old_name", "public"))
	assert.Equal(t, "public.old_name", qualifyRenameFrom("public.old_name", "public"))
	assert.Equal(t, "myschema.old_name", qualifyRenameFrom("myschema.old_name", "public"))
	assert.Equal(t, `public."Old Name"`, qualifyRenameFrom(`"Old Name"`, "public"))
	// Quoted identifier containing a dot should be treated as single name
	assert.Equal(t, `public."a.b"`, qualifyRenameFrom(`"a.b"`, "public"))
}

func TestExtractStmtDirectives_QuotedName(t *testing.T) {
	sql := `-- pista:renamed-from "My Schema"."Old Name"
CREATE TABLE public.users (id integer NOT NULL);`
	result, err := pg_query.Parse(sql)
	require.NoError(t, err)
	dirs := extractStmtDirectives(sql, result.Stmts)
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
	assert.Empty(t, extractColumnName("CONSTRAINT users_pkey PRIMARY KEY (id)"))
	assert.Empty(t, extractColumnName(""))
	// Unquoted names are lowercased
	assert.Equal(t, "displayname", extractColumnName("DisplayName text NOT NULL"))
	// An unterminated quote yields no name rather than a truncated one
	assert.Empty(t, extractColumnName(`"unterminated text NOT NULL`))
}

func TestExtractExecuteDirectives_WithCheckSQL(t *testing.T) {
	sql := `-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'my_func')
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, skip, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].SQL, "CREATE OR REPLACE FUNCTION")
	assert.Equal(t, "SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'my_func')", stmts[0].CheckSQL)
	assert.Len(t, skip, 1)
}

func TestExtractExecuteDirectives_CheckSQLTrailingSemicolon(t *testing.T) {
	sql := `-- pista:execute SELECT true;
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, _, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	// Trailing semicolon should be stripped from check SQL
	assert.Equal(t, "SELECT true", stmts[0].CheckSQL)
}

func TestExtractExecuteDirectives_WithoutCheckSQL(t *testing.T) {
	sql := `-- pista:execute
GRANT SELECT ON public.users TO readonly_role;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, skip, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].SQL, "GRANT select")
	assert.Empty(t, stmts[0].CheckSQL)
	assert.Len(t, skip, 1)
}

func TestExtractExecuteDirectives_MixedWithManaged(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL);
-- pista:execute
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, skip, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].SQL, "CREATE OR REPLACE FUNCTION")
	assert.Len(t, skip, 1)
	// The CREATE TABLE should NOT be in skip
	assert.False(t, skip[result.Stmts[0].StmtLocation])
}

func TestExtractExecuteDirectives_Multiple(t *testing.T) {
	sql := `-- pista:execute
CREATE OR REPLACE FUNCTION public.func1() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
-- pista:execute SELECT true
CREATE OR REPLACE FUNCTION public.func2() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, skip, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	assert.Empty(t, stmts[0].CheckSQL)
	assert.Equal(t, "SELECT true", stmts[1].CheckSQL)
	assert.Len(t, skip, 2)
}

func TestExtractExecuteDirectives_None(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL);`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, skip, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Empty(t, skip)
}

func TestExtractExecuteDirectives_First(t *testing.T) {
	sql := `-- pista:execute-first
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, skip, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.True(t, stmts[0].First)
	assert.Contains(t, stmts[0].SQL, "CREATE OR REPLACE FUNCTION")
	assert.Empty(t, stmts[0].CheckSQL)
	assert.Len(t, skip, 1)
}

func TestExtractExecuteDirectives_FirstWithCheckSQL(t *testing.T) {
	sql := `-- pista:execute-first SELECT to_regprocedure('public.my_func()') IS NULL
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, _, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.True(t, stmts[0].First)
	assert.Equal(t, "SELECT to_regprocedure('public.my_func()') IS NULL", stmts[0].CheckSQL)
}

// The execute-first directive name shares a prefix with execute, so the
// plain-execute pattern must not claim it (and vice versa).
func TestExtractExecuteDirectives_FirstNotMatchedAsExecute(t *testing.T) {
	sql := `-- pista:execute-first
CREATE OR REPLACE FUNCTION public.f1() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
-- pista:execute
CREATE OR REPLACE FUNCTION public.f2() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, _, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	assert.True(t, stmts[0].First)
	assert.Contains(t, stmts[0].SQL, "public.f1")
	assert.False(t, stmts[1].First)
	assert.Contains(t, stmts[1].SQL, "public.f2")
}

func TestExtractExecuteDirectives_FirstCheckSQLTrailingSemicolon(t *testing.T) {
	sql := `-- pista:execute-first SELECT true;
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, _, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.True(t, stmts[0].First)
	assert.Equal(t, "SELECT true", stmts[0].CheckSQL)
}

// Both directives on one statement is a contradiction: the statement cannot
// run on both sides of the managed DDL, so it is rejected rather than resolved.
func TestExtractExecuteDirectives_BothDirectivesIsError(t *testing.T) {
	for _, sql := range []string{
		`-- pista:execute SELECT 1 = 1
-- pista:execute-first SELECT 2 = 2
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`,
		`-- pista:execute-first SELECT 2 = 2
-- pista:execute SELECT 1 = 1
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`,
	} {
		result, err := pg_query.Parse(sql)
		require.NoError(t, err)

		_, _, err = extractExecuteDirectives(sql, result.Stmts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot both apply to one statement")
	}
}

// Repeating execute-first keeps the last-match-wins rule within the kind.
func TestExtractExecuteDirectives_FirstRepeated(t *testing.T) {
	sql := `-- pista:execute-first SELECT 1 = 1
-- pista:execute-first SELECT 2 = 2
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, _, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.True(t, stmts[0].First)
	assert.Equal(t, "SELECT 2 = 2", stmts[0].CheckSQL)
}

func TestExtractExecuteDirectives_FirstMixedWithManaged(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL);
-- pista:execute-first
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	stmts, skip, err := extractExecuteDirectives(sql, result.Stmts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.True(t, stmts[0].First)
	assert.Len(t, skip, 1)
	assert.False(t, skip[result.Stmts[0].StmtLocation])
}

func TestFormatExecuteStmt_First(t *testing.T) {
	es := &ExecuteStmt{SQL: "CREATE FUNCTION f();", First: true}
	assert.Equal(t, "-- pista:execute-first\nCREATE FUNCTION f();", FormatExecuteStmt(es))
}

func TestFormatExecuteStmt_FirstWithCheck(t *testing.T) {
	es := &ExecuteStmt{SQL: "CREATE FUNCTION f();", CheckSQL: "SELECT true", First: true}
	assert.Equal(t, "-- pista:execute-first SELECT true\nCREATE FUNCTION f();", FormatExecuteStmt(es))
}

func TestValidateDirectives_ExecuteFirstIsKnown(t *testing.T) {
	require.NoError(t, validateDirectives("-- pista:execute-first\nSELECT 1;"))
	require.NoError(t, validateDirectives("-- pista:execute-first SELECT true\nSELECT 1;"))
	require.Error(t, validateDirectives("-- pista:execute-last\nSELECT 1;"))
}

func TestExtractConcurrentlyDirectives(t *testing.T) {
	sql := `-- pista:concurrently
CREATE INDEX idx_name ON public.users USING btree (name);
CREATE INDEX idx_email ON public.users USING btree (email);`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	directives := extractConcurrentlyDirectives(sql, result.Stmts)
	assert.True(t, directives[result.Stmts[0].StmtLocation])
	assert.False(t, directives[result.Stmts[1].StmtLocation])
}

func TestExtractConcurrentlyDirectives_withRenamedFrom(t *testing.T) {
	sql := `-- pista:renamed-from idx_old
-- pista:concurrently
CREATE INDEX idx_name ON public.users USING btree (name);`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	directives := extractConcurrentlyDirectives(sql, result.Stmts)
	assert.True(t, directives[result.Stmts[0].StmtLocation])
}

func TestExtractConcurrentlyDirectives_noDirective(t *testing.T) {
	sql := `CREATE INDEX idx_name ON public.users USING btree (name);`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	directives := extractConcurrentlyDirectives(sql, result.Stmts)
	assert.Empty(t, directives)
}

func TestExtractBulkAlterDirectives(t *testing.T) {
	sql := `-- pista:bulk-alter
CREATE TABLE public.users (id integer NOT NULL);
CREATE TABLE public.posts (id integer NOT NULL);`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	directives := extractBulkAlterDirectives(sql, result.Stmts)
	assert.True(t, directives[result.Stmts[0].StmtLocation])
	assert.False(t, directives[result.Stmts[1].StmtLocation])
}

func TestExtractBulkAlterDirectives_withRenamedFrom(t *testing.T) {
	sql := `-- pista:renamed-from public.old_users
-- pista:bulk-alter
CREATE TABLE public.users (id integer NOT NULL);`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	directives := extractBulkAlterDirectives(sql, result.Stmts)
	assert.True(t, directives[result.Stmts[0].StmtLocation])
}

func TestExtractBulkAlterDirectives_noDirective(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL);`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	directives := extractBulkAlterDirectives(sql, result.Stmts)
	assert.Empty(t, directives)
}

func TestValidateDirectives_Valid(t *testing.T) {
	assert.NoError(t, validateDirectives("-- pista:renamed-from old"))
	assert.NoError(t, validateDirectives("-- pista:execute SELECT true"))
	assert.NoError(t, validateDirectives("-- pista:execute"))
	assert.NoError(t, validateDirectives("-- pista:concurrently"))
	assert.NoError(t, validateDirectives("-- pista:bulk-alter"))
	assert.NoError(t, validateDirectives("-- pista:ignore"))
	assert.NoError(t, validateDirectives("SELECT 1; -- no directives"))
}

func TestValidateDirectives_IgnoreWithArgs(t *testing.T) {
	err := validateDirectives("-- pista:ignore extra")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-- pista:ignore does not accept arguments")
}

func TestValidateDirectives_BulkAlterWithArgs(t *testing.T) {
	err := validateDirectives("-- pista:bulk-alter extra")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-- pista:bulk-alter does not accept arguments")
}

func TestValidateDirectives_ConcurrentlyWithArgs(t *testing.T) {
	err := validateDirectives("-- pista:concurrently extra")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-- pista:concurrently does not accept arguments")
}

func TestValidateDirectives_UnknownDirective(t *testing.T) {
	err := validateDirectives("-- pista:exec")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown directive: -- pista:exec")
}

func TestValidateDirectives_Typo(t *testing.T) {
	err := validateDirectives("-- pista:rename-from old")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown directive: -- pista:rename-from")
}

func TestValidateDirectives_UnknownFoobar(t *testing.T) {
	err := validateDirectives("-- pista:foobar something")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown directive: -- pista:foobar")
}

func TestValidateDirectives_SpaceAfterColon(t *testing.T) {
	err := validateDirectives("-- pista: exec")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown directive: -- pista:exec")
}

func TestValidateDirectives_MissingName(t *testing.T) {
	err := validateDirectives("-- pista:")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing directive name")
}

func TestFormatExecuteStmt_WithCheck(t *testing.T) {
	es := &ExecuteStmt{SQL: "CREATE FUNCTION f();", CheckSQL: "SELECT true"}
	assert.Equal(t, "-- pista:execute SELECT true\nCREATE FUNCTION f();", FormatExecuteStmt(es))
}

func TestFormatExecuteStmt_WithoutSemicolon(t *testing.T) {
	// Deparse output has no trailing semicolon; FormatExecuteStmt should add one
	es := &ExecuteStmt{SQL: "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql", CheckSQL: ""}
	assert.Equal(t, "-- pista:execute\nCREATE FUNCTION f() RETURNS void LANGUAGE plpgsql;", FormatExecuteStmt(es))
}

func TestFormatExecuteStmt_WithoutCheck(t *testing.T) {
	es := &ExecuteStmt{SQL: "GRANT SELECT ON t TO r;", CheckSQL: ""}
	assert.Equal(t, "-- pista:execute\nGRANT SELECT ON t TO r;", FormatExecuteStmt(es))
}

// pg_query leaves StmtLen at 0 for a final statement with no semicolon, so
// every directive scan has to read that statement's region to the end of the
// input. Each case is the SQL a file would end with, run once per ending; all
// three must agree.
func TestDirectivesOnTrailingStatementWithoutSemicolon(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		check     func(t *testing.T, r *ParseResult)
		noWarning bool
	}{
		{
			name: "renamed-from",
			sql: `CREATE TABLE public.a (id integer);
-- pista:renamed-from public.old_b
CREATE TABLE public.b (id integer)`,
			check: func(t *testing.T, r *ParseResult) {
				t.Helper()
				b := r.Tables.Get("public.b")
				require.NotNil(t, b.RenameFrom)
				assert.Equal(t, "public.old_b", *b.RenameFrom)
			},
		},
		{
			name: "ignore",
			sql: `CREATE TABLE public.a (id integer);
-- pista:ignore
CREATE TABLE public.b (id integer)`,
			check: func(t *testing.T, r *ParseResult) {
				t.Helper()
				assert.True(t, r.Tables.Get("public.b").Ignore)
			},
		},
		{
			name: "concurrently",
			sql: `CREATE TABLE public.a (id integer);
-- pista:concurrently
CREATE INDEX idx ON public.a (id)`,
			check: func(t *testing.T, r *ParseResult) {
				t.Helper()
				assert.True(t, r.Tables.Get("public.a").Indexes.Get("idx").Concurrently)
			},
		},
		{
			name: "bulk-alter",
			sql: `-- pista:bulk-alter
CREATE TABLE public.a (id integer)`,
			check: func(t *testing.T, r *ParseResult) {
				t.Helper()
				assert.True(t, r.Tables.Get("public.a").BulkAlter)
			},
		},
		{
			name: "execute",
			sql: `CREATE TABLE public.a (id integer);
-- pista:execute
GRANT SELECT ON public.a TO someone`,
			check: func(t *testing.T, r *ParseResult) {
				t.Helper()
				require.Len(t, r.ExecuteStmts, 1)
				assert.Contains(t, r.ExecuteStmts[0].SQL, "GRANT")
			},
			// The statement is run at apply, so it must not also be
			// reported as one the parser dropped.
			noWarning: true,
		},
		{
			name: "column renamed-from",
			sql: `CREATE TABLE public.b (
    id integer,
    -- pista:renamed-from old_name
    name text
)`,
			check: func(t *testing.T, r *ParseResult) {
				t.Helper()
				col := r.Tables.Get("public.b").Columns.Get("name")
				require.NotNil(t, col.RenameFrom)
				assert.Equal(t, "old_name", *col.RenameFrom)
			},
		},
		{
			name: "enum value renamed-from",
			sql: `CREATE TYPE public.e AS ENUM (
    -- pista:renamed-from old_a
    'a'
)`,
			check: func(t *testing.T, r *ParseResult) {
				t.Helper()
				assert.Equal(t, map[string]string{"a": "old_a"}, r.Enums.Get("public.e").ValueRenameFrom)
			},
		},
		{
			name: "composite attribute renamed-from",
			sql: `CREATE TYPE public.ct AS (
    -- pista:renamed-from old_x
    x integer
)`,
			check: func(t *testing.T, r *ParseResult) {
				t.Helper()
				attr := r.CompositeTypes.Get("public.ct").Attributes[0]
				require.NotNil(t, attr.RenameFrom)
				assert.Equal(t, "old_x", *attr.RenameFrom)
			},
		},
	}

	// A file usually ends with a newline, so the statement text and the
	// region can end on either side of it.
	endings := []struct{ name, suffix string }{
		{"without semicolon", ""},
		{"without semicolon, trailing newline", "\n"},
		{"with semicolon", ";\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, ending := range endings {
				t.Run(ending.name, func(t *testing.T) {
					var warnings bytes.Buffer
					defer SetWarnWriter(&warnings)()

					r, err := parseSQLWithSchema(tt.sql+ending.suffix, "public", nil)
					require.NoError(t, err)
					tt.check(t, r)
					if tt.noWarning {
						assert.Empty(t, warnings.String())
					}
				})
			}
		})
	}
}

func TestFindLeadingCommentEnd(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want int
	}{
		{"empty", "", 0},
		{"statement only", "CREATE TABLE t (id int);", 0},
		{"indented statement", "    CREATE TABLE t (id int);", 4},
		{"whitespace only", " \t\n", 3},
		{"line comment", "-- c\nCREATE", 5},
		{"line comment without newline", "-- c", 4},
		{"blank line between comments", "-- a\n\n-- b\nCREATE", 11},
		{"block comment", "/* c */\nCREATE", 8},
		{"block comment on the statement line", "/* c */ CREATE", 8},
		{"multi-line block comment", "/* a\n   b */\nCREATE", 13},
		{"nested block comment", "/* a /* b */ c */\nCREATE", 18},
		{"unterminated block comment", "/* a\n-- b\nCREATE", 16},
		{"comment forms mixed", "-- a\n/* b */\n-- c\nCREATE", 18},
		{"block comment holding a line comment", "/* -- a */\nCREATE", 11},
		{"line comment holding a block comment", "-- /* a\nCREATE", 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, findLeadingCommentEnd(tt.s))
		})
	}
}

func TestBlankBlockComments(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want string
	}{
		{"no block comment", "-- a\n", "-- a\n"},
		{"block comment", "/* a */\n", "       \n"},
		{"multi-line block comment", "/* a\nb */\n", "    \n    \n"},
		{"nested block comment", "/* a /* b */ c */", "                 "},
		{"unterminated block comment", "/* a\nb", "    \n "},
		{"line comment kept", "-- a\n/* b */\n-- c\n", "-- a\n       \n-- c\n"},
		{"block comment opened inside a line comment", "-- /* a\n-- b\n", "-- /* a\n-- b\n"},
		{"line comment last, no newline", "/* a */\n-- b", "       \n-- b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := blankBlockComments(tt.s)
			assert.Equal(t, tt.want, got)
			assert.Len(t, got, len(tt.s), "blanking must not change the length")
		})
	}
}

// A directive after a block comment belongs to the statement, and one inside a
// block comment is commented out.
func TestDirectivesAroundBlockComments(t *testing.T) {
	t.Run("after a block comment", func(t *testing.T) {
		r, err := parseSQLWithSchema(`/* the table this replaces */
-- pista:renamed-from public.old_b
CREATE TABLE public.b (id integer);`, "public", nil)
		require.NoError(t, err)
		b := r.Tables.Get("public.b")
		require.NotNil(t, b.RenameFrom)
		assert.Equal(t, "public.old_b", *b.RenameFrom)
	})

	t.Run("inside a block comment", func(t *testing.T) {
		r, err := parseSQLWithSchema(`/*
-- pista:renamed-from public.old_b
*/
CREATE TABLE public.b (id integer);`, "public", nil)
		require.NoError(t, err)
		assert.Nil(t, r.Tables.Get("public.b").RenameFrom)
	})

	t.Run("schema commented out with a block comment", func(t *testing.T) {
		r, err := parseSQLWithSchema(`/*
-- pista:ignore
CREATE TABLE public.b (id integer);
*/
CREATE TABLE public.c (id integer);`, "public", nil)
		require.NoError(t, err)
		assert.Nil(t, r.Tables.Get("public.b"))
		assert.False(t, r.Tables.Get("public.c").Ignore)
	})

	t.Run("check SQL after a block comment", func(t *testing.T) {
		r, err := parseSQLWithSchema(`CREATE TABLE public.a (id integer);
/* why */
-- pista:execute-first SELECT count(*) = 0 FROM public.a
GRANT SELECT ON public.a TO someone;`, "public", nil)
		require.NoError(t, err)
		require.Len(t, r.ExecuteStmts, 1)
		assert.True(t, r.ExecuteStmts[0].First)
		assert.Equal(t, "SELECT count(*) = 0 FROM public.a", r.ExecuteStmts[0].CheckSQL)
	})
}
