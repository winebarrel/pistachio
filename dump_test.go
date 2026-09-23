package pistachio

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/format"
	"github.com/winebarrel/pistachio/internal/testutil"
	"github.com/winebarrel/pistachio/model"
)

type dumpTestCase struct {
	Init string `yaml:"init"`
	Dump string `yaml:"dump"`
	// MinPG skips the fixture on a server older than this major version, for
	// syntax the server does not accept yet.
	MinPG              int      `yaml:"min_pg,omitempty"`
	DumpPG16           string   `yaml:"dump_pg16,omitempty"`
	DumpPG17           string   `yaml:"dump_pg17,omitempty"`
	DumpPG18           string   `yaml:"dump_pg18,omitempty"`
	OmitSchema         bool     `yaml:"omit_schema"`
	Include            []string `yaml:"include,omitempty"`
	Exclude            []string `yaml:"exclude,omitempty"`
	Enable             []string `yaml:"enable,omitempty"`
	Disable            []string `yaml:"disable,omitempty"`
	ManageRoutine      bool     `yaml:"manage_routine,omitempty"`
	ManageStorageParam bool     `yaml:"manage_storage_param,omitempty"`
	SkipPartitionChild bool     `yaml:"skip_partition_child,omitempty"`
	// Explain sets --explain, so the dump carries the size of each table and
	// index. An init that wants the estimates runs ANALYZE; write {{today}}
	// in the expected dump for the date it prints.
	Explain bool `yaml:"explain,omitempty"`
}

func (tc *dumpTestCase) expectedDump(major int) string {
	switch major {
	case 16:
		if tc.DumpPG16 != "" {
			return tc.DumpPG16
		}
	case 17:
		if tc.DumpPG17 != "" {
			return tc.DumpPG17
		}
	case 18:
		if tc.DumpPG18 != "" {
			return tc.DumpPG18
		}
	}
	return tc.Dump
}

func TestDump_InvalidConnString(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	_, err := client.Dump(ctx, &DumpOptions{})
	require.Error(t, err)
}

func TestDump_UnreachableHost(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{
		ConnString: "postgres://postgres@192.0.2.1:5432/postgres?connect_timeout=1",
		Schemas:    []string{"public"},
	})

	_, err := client.Dump(ctx, &DumpOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect")
}

func TestDump_EmptySchemas(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{},
	})

	_, err := client.Dump(ctx, &DumpOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one schema must be specified")
}

func TestDump_CanceledContext(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	_, err := client.Dump(canceledCtx, &DumpOptions{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDump_Count(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	assert.Equal(t, []string{"public"}, got.Count.Schemas)
	assert.Equal(t, 1, got.Count.Tables)
	assert.Equal(t, 1, got.Count.Views)
	assert.Equal(t, 1, got.Count.Enums)
	assert.Equal(t, 1, got.Count.Domains)
	assert.Equal(t, 0, got.Count.Sequences)
	assert.Equal(t, "schema public", got.Count.SchemaLabel())
	assert.Equal(t, "1 table, 1 view, 1 enum, 1 domain, 0 composite types, 0 sequences", got.Count.Summary())
}

func TestDump_NoReadOnly(t *testing.T) {
	// With NoReadOnly the connection is opened read-write. Dump never writes,
	// so the output is the same; this just exercises the plumbing.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{NoReadOnly: true})
	require.NoError(t, err)
	assert.Equal(t, 1, got.Count.Tables)
}

// TestDump_NoFormat covers the flag and the property behind it: the model
// renders what the formatter would write, so turning the formatter off changes
// nothing. A rule that broke this would put dump and pista fmt at odds.
func TestDump_NoFormat(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE TABLE public.users (
    id integer NOT NULL,
    tags text[],
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    state public.status NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id, state)
) PARTITION BY LIST (state);
CREATE TABLE public.users_active PARTITION OF public.users FOR VALUES IN ('active');
CREATE VIEW public.v AS SELECT u.id, (SELECT max(x.id) FROM public.users x WHERE x.id <> u.id) AS peak FROM public.users u;`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	formatted, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)

	raw, err := client.Dump(ctx, &DumpOptions{NoFormat: true})
	require.NoError(t, err)

	assert.Equal(t, formatted.String(), raw.String())
	assert.Equal(t, formatted.Files(), raw.Files())
}

// A model built by hand can render SQL that does not parse, here a sequence
// with no type. The dump returns that SQL as rendered instead of failing.
func TestDumpResult_String_UnparsableKeptAsRendered(t *testing.T) {
	seqs := orderedmap.New[string, *model.Sequence]()
	seqs.Set("public.s", &model.Sequence{Schema: "public", Name: "s"})
	result := &DumpResult{Sequences: seqs}

	assert.Equal(t, model.SequencesToSQL(seqs), result.String())
}

func TestDump_Count_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	assert.Equal(t, 0, got.Count.Tables)
	assert.Equal(t, 0, got.Count.Views)
	assert.Equal(t, 0, got.Count.Enums)
	assert.Equal(t, 0, got.Count.Domains)
	assert.Equal(t, "0 tables, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences", got.Count.Summary())
}

func TestDump_Count_Filtered(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Exclude:    []string{"posts"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, got.Count.Tables)
	assert.Equal(t, "1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences", got.Count.Summary())
}

func TestDumpResult_Files_Tables(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Len(t, files, 2)
	assert.Contains(t, files, "public.users.sql")
	assert.Contains(t, files, "public.posts.sql")
	assert.Contains(t, files["public.users.sql"], "CREATE TABLE public.users")
	assert.Contains(t, files["public.posts.sql"], "CREATE TABLE public.posts")
}

func TestDumpResult_Files_Views(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Len(t, files, 2)
	assert.Contains(t, files, "public.users.sql")
	assert.Contains(t, files, "public.active_users.sql")
	assert.Contains(t, files["public.active_users.sql"], "CREATE OR REPLACE VIEW")
}

func TestDumpResult_Files_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Empty(t, files)
}

func TestDumpResult_Files_Enums(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "public.status.sql")
	assert.Contains(t, files["public.status.sql"], "CREATE TYPE public.status AS ENUM")
}

func TestDumpResult_Files_Domains(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "public.pos_int.sql")
	assert.Contains(t, files["public.pos_int.sql"], "CREATE DOMAIN public.pos_int")
}

func TestDumpResult_Files_CompositeTypes(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.addr AS (street text, city text);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "public.addr.sql")
	assert.Contains(t, files["public.addr.sql"], "CREATE TYPE public.addr AS (")
}

func TestDumpResult_Files_Sequences(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE SEQUENCE public.order_seq;`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "public.order_seq.sql")
	assert.Contains(t, files["public.order_seq.sql"], "CREATE SEQUENCE public.order_seq")
}

func TestDumpResult_Files_SpecialCharacters(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public."My Table" (
    id integer NOT NULL
);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "public.My_Table.sql")
}

func TestDumpResult_OmitSchema_Files(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "users.sql")
	assert.NotContains(t, files, "public.users.sql")
	assert.Contains(t, files["users.sql"], "CREATE TABLE users")
}

// TestDumpResult_OmitSchema_HelperKeyConsistency verifies that the internal
// helpers used by String()/Files() build their map with unqualified keys when
// OmitSchema is true, matching the schema-stripped values they store.
// Previously the helpers stripped Schema from the value but kept the
// schema-qualified name as the map key; invisible through String()/Files()
// (both iterate values) but a self-inconsistent intermediate that would
// leak to any future caller iterating by key.
func TestDumpResult_OmitSchema_HelperKeyConsistency(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{OmitSchema: true})
	require.NoError(t, err)

	for k, v := range got.tables().All() {
		assert.Equal(t, model.Ident(v.Name), k, "table key should match unqualified Ident(Name)")
		assert.Empty(t, v.Schema, "table value Schema should be empty")
	}
	for k, v := range got.views().All() {
		assert.Equal(t, model.Ident(v.Name), k, "view key should match unqualified Ident(Name)")
		assert.Empty(t, v.Schema, "view value Schema should be empty")
	}
	for k, v := range got.enums().All() {
		assert.Equal(t, model.Ident(v.Name), k, "enum key should match unqualified Ident(Name)")
		assert.Empty(t, v.Schema, "enum value Schema should be empty")
	}
	for k, v := range got.domains().All() {
		assert.Equal(t, model.Ident(v.Name), k, "domain key should match unqualified Ident(Name)")
		assert.Empty(t, v.Schema, "domain value Schema should be empty")
	}
}

func TestDump_Domain_OmitSchema_PlanNoDiff(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);
CREATE DOMAIN public.email AS varchar(255) NOT NULL DEFAULT ''::varchar;
CREATE TABLE public.users (
    id pos_int NOT NULL,
    email email NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// Dump with omit-schema, then plan should have no diff
	got, err := client.Dump(ctx, &DumpOptions{OmitSchema: true})
	require.NoError(t, err)

	tmpFile := filepath.Join(t.TempDir(), "schema.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte(got.String()), 0o644))

	plan, err := client.Plan(ctx, &PlanOptions{Files: []string{tmpFile}})
	require.NoError(t, err)
	assert.Empty(t, plan.SQL)
}

func TestDumpResult_OmitSchema_Enum_Files(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "status.sql")
	assert.NotContains(t, files, "public.status.sql")
	assert.Contains(t, files["status.sql"], "CREATE TYPE status AS ENUM")
}

func TestDump_OmitSchema_MultipleSchemas(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{
		ConnString: "postgres://postgres@localhost/postgres",
		Schemas:    []string{"public", "other"},
	})

	_, err := client.Dump(ctx, &DumpOptions{OmitSchema: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--omit-schema cannot be used with multiple schemas")
}

func TestDump_OmitSchema_PartitionParentInAnotherSchema(t *testing.T) {
	ctx := context.Background()
	// The dumped schema holds only the child; its parent stays in another one.
	// Stripping the schema off the parent would point the statement at a table
	// that does not exist, so the reference keeps it.
	connStr := setupSchemaDB(t, ctx, "other", `
		CREATE TABLE other.parent (
			id integer NOT NULL,
			d date NOT NULL
		) PARTITION BY RANGE (d);
	`)
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, `
		CREATE TABLE public.child PARTITION OF other.parent
			FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
	`)

	client := NewClient(&Options{
		ConnString: connStr,
		Schemas:    []string{"public"},
	})
	result, err := client.Dump(ctx, &DumpOptions{OmitSchema: true})
	require.NoError(t, err)

	assert.Contains(t, result.String(), "CREATE TABLE child PARTITION OF other.parent")
}

func TestDumpResult_OmitSchema_ViewDefinition(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	// Use a non-default schema so pg_get_viewdef includes the schema prefix
	// in the view definition. With public, pg_get_viewdef omits it.
	_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS app CASCADE")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE SCHEMA app")
	require.NoError(t, err)
	defer conn.Exec(ctx, "DROP SCHEMA app CASCADE") //nolint:errcheck

	_, err = conn.Exec(ctx, `
CREATE TABLE app.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW app.active_users AS SELECT id, name FROM app.users WHERE name IS NOT NULL;`)
	require.NoError(t, err)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"app"},
	})

	got, err := client.Dump(ctx, &DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	// View name should not have schema prefix
	assert.Contains(t, s, "CREATE OR REPLACE VIEW active_users")
	assert.NotContains(t, s, "app.active_users")
	// Note: pg_get_viewdef includes schema prefix for non-search_path schemas
	// in the view definition (FROM clause). This is PostgreSQL behavior and
	// --omit-schema currently does not rewrite view definition internals.
	assert.Contains(t, s, "FROM app.users")
}

func TestDumpResult_Files_DuplicateFileName(t *testing.T) {
	// When a table and view produce the same file name, the second should be renamed
	tables := orderedmap.New[string, *model.Table]()
	tbl := &model.Table{
		Schema:      "",
		Name:        "users",
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Indexes:     orderedmap.New[string, *model.Index](),
	}
	tbl.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	tables.Set("users", tbl)

	views := orderedmap.New[string, *model.View]()
	views.Set("users", &model.View{Schema: "", Name: "users", Definition: "SELECT 1"})

	result := &DumpResult{
		Tables: tables,
		Views:  views,
	}
	files := result.Files()
	assert.Len(t, files, 2)
	assert.Contains(t, files, "users.sql")
	assert.Contains(t, files, "users_2.sql")
}

func TestDumpResult_Files_DuplicateFileNameCaseInsensitive(t *testing.T) {
	// "Users" and "users" should be treated as duplicate file names
	tables := orderedmap.New[string, *model.Table]()
	t1 := &model.Table{
		Schema:      "",
		Name:        "users",
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Indexes:     orderedmap.New[string, *model.Index](),
	}
	t1.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	t2 := &model.Table{
		Schema:      "",
		Name:        "Users",
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Indexes:     orderedmap.New[string, *model.Index](),
	}
	t2.Columns.Set("id", &model.Column{Name: "id", TypeName: "bigint", NotNull: true})
	tables.Set("users", t1)
	tables.Set("Users", t2)

	result := &DumpResult{
		Tables: tables,
		Views:  orderedmap.New[string, *model.View](),
	}
	files := result.Files()
	assert.Contains(t, files, "users.sql")
	assert.Contains(t, files, "Users_2.sql")
	assert.Len(t, files, 2)
}

func TestDump(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	pgMajor := testutil.ServerMajorVersion(t, ctx, conn)

	files, err := filepath.Glob("testdata/dump/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")
		t.Run(name, func(t *testing.T) {
			tc := loadYAML[dumpTestCase](t, file)
			if tc.MinPG > 0 && pgMajor < tc.MinPG {
				t.Skipf("requires PostgreSQL %d or later", tc.MinPG)
			}
			testutil.SetupDB(t, ctx, conn, tc.Init)
			client := NewClient(&Options{
				ConnString:         conn.Config().ConnString(),
				Schemas:            []string{"public"},
				Include:            tc.Include,
				Exclude:            tc.Exclude,
				Enable:             tc.Enable,
				Disable:            tc.Disable,
				ManageRoutine:      tc.ManageRoutine,
				ManageStorageParam: tc.ManageStorageParam,
				SkipPartitionChild: tc.SkipPartitionChild,
			})
			got, err := client.Dump(ctx, &DumpOptions{
				OmitSchema: tc.OmitSchema,
				Explain:    tc.Explain,
			})
			require.NoError(t, err)
			expected := strings.TrimSpace(expandToday(tc.expectedDump(pgMajor)))
			assert.Equal(t, expected, strings.TrimSpace(got.String()))

			// dump writes through the formatter, so its output has to be what
			// the formatter leaves alone. A rule that moved it would be a
			// difference between dump and pista fmt.
			formatted, err := format.Format(expected + "\n")
			require.NoError(t, err)
			assert.Equal(t, expected, strings.TrimSpace(formatted), "the dump is not what pista fmt writes")
		})
	}
}

// --split writes one file per routine. Two overloads share a schema-qualified
// name, so the second one takes the same _2 suffix any other collision does.
func TestDumpResult_Files_Routines(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE FUNCTION public.f(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
CREATE FUNCTION public.f(a text) RETURNS text LANGUAGE sql AS $$ SELECT a $$;
CREATE PROCEDURE public.p() LANGUAGE sql AS $$ SELECT $$;`)

	client := NewClient(&Options{
		ConnString:    conn.Config().ConnString(),
		Schemas:       []string{"public"},
		ManageRoutine: true,
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)

	files := got.Files()
	assert.Len(t, files, 3)
	assert.Contains(t, files["public.f.sql"], "CREATE OR REPLACE FUNCTION public.f(a integer)")
	assert.Contains(t, files["public.f_2.sql"], "CREATE OR REPLACE FUNCTION public.f(a text)")
	assert.Contains(t, files["public.p.sql"], "CREATE OR REPLACE PROCEDURE public.p()")
}

// Without --manage-routine the dump holds no routines, so --split writes no
// file for one.
func TestDumpResult_Files_RoutinesUnmanaged(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
CREATE TABLE public.users (id integer);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)

	files := got.Files()
	assert.Len(t, files, 1)
	assert.Contains(t, files, "public.users.sql")
}

// Document carries every object kind the dump holds, so a JSON dump reports
// what a SQL one does.
func TestDumpResult_Document(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);
CREATE TYPE public.addr AS (street text, city text);
CREATE SEQUENCE public.counter;
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	result, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)

	doc := result.Document()
	assert.Equal(t, 1, doc.Tables.Len())
	assert.Equal(t, 1, doc.Views.Len())
	assert.Equal(t, 1, doc.Enums.Len())
	assert.Equal(t, 1, doc.Domains.Len())
	assert.Equal(t, 1, doc.CompositeTypes.Len())
	assert.Equal(t, 1, doc.Sequences.Len())
	assert.Equal(t, 0, doc.Routines.Len())

	// A database holds no execute statements.
	assert.Empty(t, doc.ExecuteStmts)
}

// --omit-schema reaches the document, the way it reaches the SQL.
func TestDumpResult_Document_OmitSchema(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer);`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	result, err := client.Dump(ctx, &DumpOptions{OmitSchema: true})
	require.NoError(t, err)

	doc := result.Document()
	table, ok := doc.Tables.GetOk("users")
	require.True(t, ok, "the key loses the schema")
	assert.Empty(t, table.Schema)
}

// A dump that read nothing still carries every object kind, as an empty map.
func TestDumpResult_Document_Empty(t *testing.T) {
	doc := (&DumpResult{}).Document()

	assert.Equal(t, 0, doc.Tables.Len())
	assert.Equal(t, 0, doc.Views.Len())
	assert.Equal(t, 0, doc.Enums.Len())
	assert.Equal(t, 0, doc.Domains.Len())
	assert.Equal(t, 0, doc.CompositeTypes.Len())
	assert.Equal(t, 0, doc.Sequences.Len())
	assert.Equal(t, 0, doc.Routines.Len())
}

// Two schemas share one document, each object under its qualified name.
func TestDumpResult_Document_MultipleSchemas(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer);`)
	connStr := setupSchemaDB(t, ctx, "other", `CREATE TABLE other.items (id integer);`)

	client := NewClient(&Options{ConnString: connStr, Schemas: []string{"public", "other"}})

	result, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)

	doc := result.Document()
	assert.ElementsMatch(t, []string{"public.users", "other.items"}, doc.Tables.CollectKeys())
}
