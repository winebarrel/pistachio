package dsql_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/dsql"
	"github.com/winebarrel/pistachio/model"
)

func TestNormalizeCurrent(t *testing.T) {
	cols := orderedmap.New[string, *model.Column]()
	cols.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	cols.Set("name", &model.Column{Name: "name", TypeName: "text", Compression: "lz4"})

	cons := orderedmap.New[string, *model.Constraint]()
	cons.Set("users_pkey", &model.Constraint{
		Name:       "users_pkey",
		Type:       'p',
		Definition: "PRIMARY KEY (id) INCLUDE (name, email)",
	})
	cons.Set("users_name_key", &model.Constraint{
		Name:       "users_name_key",
		Type:       'u',
		Definition: "UNIQUE (name)",
	})
	cons.Set("users_name_check", &model.Constraint{
		Name:       "users_name_check",
		Type:       'c',
		Definition: "CHECK ((name <> ''::text))",
	})

	idxs := orderedmap.New[string, *model.Index]()
	idxs.Set("public.users_name_idx", &model.Index{
		Schema:     "public",
		Name:       "users_name_idx",
		Table:      "users",
		Definition: "CREATE INDEX users_name_idx ON public.users USING btree_index (name)",
	})
	idxs.Set("public.users_lower_idx", &model.Index{
		Schema:     "public",
		Name:       "users_lower_idx",
		Table:      "users",
		Definition: "CREATE UNIQUE INDEX users_lower_idx ON public.users USING btree_index (lower(name)) WHERE (id > 0)",
	})

	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.users", &model.Table{
		Schema:      "public",
		Name:        "users",
		Columns:     cols,
		Constraints: cons,
		Indexes:     idxs,
	})

	require.NoError(t, dsql.NormalizeCurrent(tables))

	users := tables.Get("public.users")
	assert.Empty(t, users.Columns.Get("name").Compression)
	assert.Equal(t, "PRIMARY KEY (id)", users.Constraints.Get("users_pkey").Definition)
	assert.Equal(t, "UNIQUE (name)", users.Constraints.Get("users_name_key").Definition)
	assert.Equal(t, "CHECK ((name <> ''::text))", users.Constraints.Get("users_name_check").Definition)
	assert.Equal(t, "CREATE INDEX users_name_idx ON public.users USING btree (name)", users.Indexes.Get("public.users_name_idx").Definition)
	assert.Equal(t, "CREATE UNIQUE INDEX users_lower_idx ON public.users USING btree (lower(name)) WHERE id > 0", users.Indexes.Get("public.users_lower_idx").Definition)
}

func TestNormalizeCurrentNoIndexesOrConstraints(t *testing.T) {
	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.t", &model.Table{
		Schema:      "public",
		Name:        "t",
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		Indexes:     orderedmap.New[string, *model.Index](),
	})
	require.NoError(t, dsql.NormalizeCurrent(tables))
}

func TestNormalizeCurrentBadDefinition(t *testing.T) {
	cons := orderedmap.New[string, *model.Constraint]()
	cons.Set("t_pkey", &model.Constraint{Name: "t_pkey", Type: 'p', Definition: "PRIMARY KEY ("})
	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.t", &model.Table{
		Schema:      "public",
		Name:        "t",
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: cons,
		Indexes:     orderedmap.New[string, *model.Index](),
	})
	require.Error(t, dsql.NormalizeCurrent(tables))

	idxs := orderedmap.New[string, *model.Index]()
	idxs.Set("public.i", &model.Index{Schema: "public", Name: "i", Table: "t", Definition: "CREATE INDEX i ON"})
	tables.Set("public.t", &model.Table{
		Schema:      "public",
		Name:        "t",
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		Indexes:     idxs,
	})
	require.Error(t, dsql.NormalizeCurrent(tables))
}

func TestSplit(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "add column with a default",
			input:    []string{"ALTER TABLE public.users ADD COLUMN status text DEFAULT 'active'::text;"},
			expected: []string{"ALTER TABLE public.users ADD COLUMN status text;", "ALTER TABLE public.users ALTER COLUMN status SET DEFAULT 'active'::text;"},
		},
		{
			name:     "add column with a default and a collation",
			input:    []string{`ALTER TABLE public.users ADD COLUMN "Status" text COLLATE "C" DEFAULT now()::text;`},
			expected: []string{`ALTER TABLE public.users ADD COLUMN "Status" text COLLATE "C";`, `ALTER TABLE public.users ALTER COLUMN "Status" SET DEFAULT now()::text;`},
		},
		{
			name:     "add column with a default and NOT NULL is left for Finish",
			input:    []string{"ALTER TABLE public.users ADD COLUMN status text DEFAULT 'active'::text NOT NULL;"},
			expected: []string{"ALTER TABLE public.users ADD COLUMN status text DEFAULT 'active'::text NOT NULL;"},
		},
		{
			name:     "add column without a default",
			input:    []string{"ALTER TABLE public.users ADD COLUMN status text;"},
			expected: []string{"ALTER TABLE public.users ADD COLUMN status text;"},
		},
		{
			name:  "add a unique constraint",
			input: []string{"ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE (email);"},
			expected: []string{
				"CREATE UNIQUE INDEX users_email_key ON public.users (email);",
				"ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE USING INDEX users_email_key;",
			},
		},
		{
			name:  "add a deferrable unique constraint on two columns",
			input: []string{`ALTER TABLE public.users ADD CONSTRAINT "Users_key" UNIQUE (email, "Name") DEFERRABLE INITIALLY DEFERRED;`},
			expected: []string{
				`CREATE UNIQUE INDEX "Users_key" ON public.users (email, "Name");`,
				`ALTER TABLE public.users ADD CONSTRAINT "Users_key" UNIQUE USING INDEX "Users_key" DEFERRABLE INITIALLY DEFERRED;`,
			},
		},
		{
			name:     "a unique constraint over an existing index is left alone",
			input:    []string{"ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE USING INDEX users_email_idx;"},
			expected: []string{"ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE USING INDEX users_email_idx;"},
		},
		{
			name:     "other statements are left alone",
			input:    []string{"CREATE TABLE public.t (\n    id integer\n);", "ALTER TABLE public.t ALTER COLUMN id SET DEFAULT 1;"},
			expected: []string{"CREATE TABLE public.t (\n    id integer\n);", "ALTER TABLE public.t ALTER COLUMN id SET DEFAULT 1;"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := dsql.Split(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestSplitParseError(t *testing.T) {
	_, err := dsql.Split([]string{"ALTER TABLE"})
	require.Error(t, err)
}

func TestFinish(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "an index loses USING btree and gains ASYNC",
			input:    []string{"CREATE INDEX users_name_idx ON public.users USING btree (name);"},
			expected: []string{"CREATE INDEX ASYNC users_name_idx ON public.users (name);"},
		},
		{
			name:     "a unique index",
			input:    []string{"CREATE UNIQUE INDEX users_name_idx ON public.users USING btree (lower(name)) WHERE id > 0;"},
			expected: []string{"CREATE UNIQUE INDEX ASYNC users_name_idx ON public.users (lower(name)) WHERE id > 0;"},
		},
		{
			name:     "an index without USING",
			input:    []string{"CREATE UNIQUE INDEX users_email_key ON public.users (email);"},
			expected: []string{"CREATE UNIQUE INDEX ASYNC users_email_key ON public.users (email);"},
		},
		{
			name:     "another access method is kept for DSQL to refuse",
			input:    []string{"CREATE INDEX users_tags_idx ON public.users USING gin (tags);"},
			expected: []string{"CREATE INDEX ASYNC users_tags_idx ON public.users USING gin (tags);"},
		},
		{
			name:     "an identity column in CREATE TABLE gains CACHE 1",
			input:    []string{"CREATE TABLE public.t (\n    id bigint GENERATED ALWAYS AS IDENTITY,\n    v text\n);"},
			expected: []string{"CREATE TABLE public.t (\n    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),\n    v text\n);"},
		},
		{
			name:     "an identity column with other options",
			input:    []string{"CREATE TABLE public.t (\n    id bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 100),\n    v text\n);"},
			expected: []string{"CREATE TABLE public.t (\n    id bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 100 CACHE 1),\n    v text\n);"},
		},
		{
			name:     "an identity column with a cache keeps it",
			input:    []string{"CREATE TABLE public.t (\n    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 65536)\n);"},
			expected: []string{"CREATE TABLE public.t (\n    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 65536)\n);"},
		},
		{
			name:     "an identity added to a column",
			input:    []string{"ALTER TABLE public.t ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY;"},
			expected: []string{"ALTER TABLE public.t ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (CACHE 1);"},
		},
		{
			name:     "other statements are left alone",
			input:    []string{"ALTER TABLE public.t ALTER COLUMN v SET DEFAULT 'x'::text;", "DROP INDEX public.i;", "COMMENT ON TABLE public.t IS 'x';"},
			expected: []string{"ALTER TABLE public.t ALTER COLUMN v SET DEFAULT 'x'::text;", "DROP INDEX public.i;", "COMMENT ON TABLE public.t IS 'x';"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := dsql.Finish(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestFinishRefuses(t *testing.T) {
	tests := []struct {
		name  string
		input string
		err   string
	}{
		{"CREATE INDEX CONCURRENTLY", "CREATE INDEX CONCURRENTLY i ON public.t USING btree (c);", "CONCURRENTLY"},
		{"DROP INDEX CONCURRENTLY", "DROP INDEX CONCURRENTLY public.i;", "CONCURRENTLY"},
		{"DROP COLUMN", "ALTER TABLE public.t DROP COLUMN c;", "DROP COLUMN"},
		{"SET NOT NULL", "ALTER TABLE public.t ALTER COLUMN c SET NOT NULL;", "SET NOT NULL"},
		{"type change", "ALTER TABLE public.t ALTER COLUMN c TYPE bigint;", "type"},
		{"NOT NULL column", "ALTER TABLE public.t ADD COLUMN c integer NOT NULL;", "NOT NULL"},
		{"NOT NULL column with a default", "ALTER TABLE public.t ADD COLUMN c integer DEFAULT 1 NOT NULL;", "NOT NULL"},
		{"primary key", "ALTER TABLE public.t ADD CONSTRAINT t_pkey PRIMARY KEY (id);", "PRIMARY KEY"},
		{"check", "ALTER TABLE public.t ADD CONSTRAINT t_c_check CHECK (c > 0);", "CHECK"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := dsql.Finish([]string{tt.input})
			require.Error(t, err)
			require.ErrorContains(t, err, tt.err)
			require.ErrorContains(t, err, tt.input)
		})
	}
}

func TestFinishParseError(t *testing.T) {
	_, err := dsql.Finish([]string{"CREATE INDEX"})
	require.Error(t, err)
}

func TestAddIdentityCache(t *testing.T) {
	sql := "-- public.t\nCREATE TABLE public.t (\n    id bigint GENERATED ALWAYS AS IDENTITY,\n    n bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 2),\n    v text,\n    CONSTRAINT t_pkey PRIMARY KEY (id)\n);\n\n-- public.u\nCREATE TABLE public.u (\n    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 65536)\n);"
	expected := "-- public.t\nCREATE TABLE public.t (\n    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),\n    n bigint GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 2 CACHE 1),\n    v text,\n    CONSTRAINT t_pkey PRIMARY KEY (id)\n);\n\n-- public.u\nCREATE TABLE public.u (\n    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 65536)\n);"

	actual, err := dsql.AddIdentityCache(sql)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	_, err = dsql.AddIdentityCache("CREATE TABLE (")
	require.Error(t, err)
}

func TestUnwaitedUniqueIndex(t *testing.T) {
	name, ok, err := dsql.UnwaitedUniqueIndex([]string{
		"CREATE UNIQUE INDEX ASYNC users_email_key ON public.users (email);",
		"ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE USING INDEX users_email_key;",
	})
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "users_email_key", name)

	// An index that already exists needs no wait.
	_, ok, err = dsql.UnwaitedUniqueIndex([]string{
		"CREATE INDEX ASYNC users_name_idx ON public.users (name);",
		"ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE USING INDEX users_email_idx;",
	})
	require.NoError(t, err)
	assert.False(t, ok)

	_, _, err = dsql.UnwaitedUniqueIndex([]string{"CREATE UNIQUE INDEX ASYNC ("})
	require.Error(t, err)
}

type fakeRow struct {
	value any
	err   error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	switch d := dest[0].(type) {
	case *string:
		*d = r.value.(string)
	case *bool:
		*d = r.value.(bool)
	}
	return nil
}

type fakeConn struct {
	execs   []string
	queries []string
	rows    []fakeRow
	execErr error
}

func (c *fakeConn) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	c.execs = append(c.execs, sql)
	return pgconn.CommandTag{}, c.execErr
}

func (c *fakeConn) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	c.queries = append(c.queries, sql)
	row := c.rows[0]
	c.rows = c.rows[1:]
	return row
}

func TestExec(t *testing.T) {
	ctx := context.Background()

	t.Run("an index build is waited for", func(t *testing.T) {
		conn := &fakeConn{rows: []fakeRow{{value: "job1"}, {value: true}}}
		require.NoError(t, dsql.Exec(ctx, conn, "CREATE INDEX ASYNC i ON public.t (c);"))
		assert.Equal(t, []string{"CREATE INDEX ASYNC i ON public.t (c);", "CALL sys.wait_for_job('job1')"}, conn.queries)
		assert.Empty(t, conn.execs)
	})

	t.Run("a unique index build is waited for", func(t *testing.T) {
		conn := &fakeConn{rows: []fakeRow{{value: "job1"}, {value: true}}}
		require.NoError(t, dsql.Exec(ctx, conn, "CREATE UNIQUE INDEX ASYNC i ON public.t (c);"))
		assert.Len(t, conn.queries, 2)
	})

	t.Run("a failed build is an error", func(t *testing.T) {
		conn := &fakeConn{rows: []fakeRow{{value: "job1"}, {value: false}}}
		err := dsql.Exec(ctx, conn, "CREATE INDEX ASYNC i ON public.t (c);")
		require.ErrorContains(t, err, "job1")
	})

	t.Run("the statement fails", func(t *testing.T) {
		conn := &fakeConn{rows: []fakeRow{{err: errors.New("boom")}}}
		require.ErrorContains(t, dsql.Exec(ctx, conn, "CREATE INDEX ASYNC i ON public.t (c);"), "boom")
	})

	t.Run("the wait fails", func(t *testing.T) {
		conn := &fakeConn{rows: []fakeRow{{value: "job1"}, {err: errors.New("boom")}}}
		require.ErrorContains(t, dsql.Exec(ctx, conn, "CREATE INDEX ASYNC i ON public.t (c);"), "boom")
	})

	t.Run("other statements run as they are", func(t *testing.T) {
		conn := &fakeConn{}
		require.NoError(t, dsql.Exec(ctx, conn, "DROP INDEX public.i;"))
		assert.Equal(t, []string{"DROP INDEX public.i;"}, conn.execs)
		assert.Empty(t, conn.queries)

		conn = &fakeConn{execErr: errors.New("boom")}
		require.ErrorContains(t, dsql.Exec(ctx, conn, "DROP INDEX public.i;"), "boom")
	})
}
