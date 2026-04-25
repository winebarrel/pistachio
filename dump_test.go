package pistachio_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
	"github.com/winebarrel/pistachio/model"
)

type dumpTestCase struct {
	Init string `yaml:"init"`
	Dump string `yaml:"dump"`
}

func TestDump_InvalidConnString(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.Error(t, err)
}

func TestDump_UnreachableHost(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "postgres://postgres@192.0.2.1:5432/postgres?connect_timeout=1",
		Schemas:    []string{"public"},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect")
}

func TestDump_EmptySchemas(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.Error(t, err)
}

func TestDump_CanceledContext(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	_, err := client.Dump(canceledCtx, &pistachio.DumpOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch tables")
}

func TestDumpResult_String_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Equal(t, "", got.String())
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "public.pos_int.sql")
	assert.Contains(t, files["public.pos_int.sql"], "CREATE DOMAIN public.pos_int")
}

func TestDumpResult_Files_SpecialCharacters(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public."My Table" (
    id integer NOT NULL
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "public.My_Table.sql")
}

func TestDumpResult_OmitSchema_String(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "CREATE TABLE users")
	assert.NotContains(t, s, "public.users")
	assert.Contains(t, s, "CREATE OR REPLACE VIEW active_users")
	assert.NotContains(t, s, "public.active_users")
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "users.sql")
	assert.NotContains(t, files, "public.users.sql")
	assert.Contains(t, files["users.sql"], "CREATE TABLE users")
}

func TestDumpResult_OmitSchema_ForeignKey(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
ALTER TABLE public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "ALTER TABLE ONLY orders")
	assert.NotContains(t, s, "public.orders")
}

func TestDumpResult_OmitSchema_Comment(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.users IS 'User accounts';`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "COMMENT ON TABLE users")
	assert.NotContains(t, s, "public.users")
}

func TestDumpResult_OmitSchema_Enum(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
COMMENT ON TYPE public.status IS 'User status';`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "CREATE TYPE status AS ENUM")
	assert.NotContains(t, s, "public.status")
	assert.Contains(t, s, "COMMENT ON TYPE status")
}

func TestDumpResult_OmitSchema_Domain(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "CREATE DOMAIN pos_int AS integer")
	assert.NotContains(t, s, "public.pos_int")
}

func TestDump_Domain_DefaultCollationExcluded(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE DOMAIN public.email AS varchar(255) NOT NULL DEFAULT ''::varchar;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "CREATE DOMAIN public.email")
	assert.NotContains(t, s, "COLLATE")
	assert.NotContains(t, s, "default")
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// Dump with omit-schema, then plan should have no diff
	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)

	tmpFile := filepath.Join(t.TempDir(), "schema.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte(got.String()), 0o644))

	plan, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{tmpFile}})
	require.NoError(t, err)
	assert.Empty(t, plan.SQL)
}

func TestDumpResult_OmitSchema_Enum_Files(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "status.sql")
	assert.NotContains(t, files, "public.status.sql")
	assert.Contains(t, files["status.sql"], "CREATE TYPE status AS ENUM")
}

func TestDump_OmitSchema_MultipleSchemas(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "postgres://postgres@localhost/postgres",
		Schemas:    []string{"public", "other"},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--omit-schema cannot be used with multiple schemas")
}

func TestDumpResult_OmitSchema_Index(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_id ON public.users (id);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "CREATE INDEX idx_users_id ON users")
	assert.NotContains(t, s, "ON public.users")
}

func TestDumpResult_OmitSchema_MaterializedView(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;
CREATE INDEX idx_user_stats_cnt ON public.user_stats (cnt);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "CREATE MATERIALIZED VIEW user_stats")
	assert.Contains(t, s, "CREATE INDEX idx_user_stats_cnt ON user_stats")
	assert.NotContains(t, s, "ON public.user_stats")
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"app"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
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

	result := &pistachio.DumpResult{
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

	result := &pistachio.DumpResult{
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

	files, err := filepath.Glob("testdata/dump/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")
		t.Run(name, func(t *testing.T) {
			tc := loadYAML[dumpTestCase](t, file)
			testutil.SetupDB(t, ctx, conn, tc.Init)
			client := pistachio.NewClient(&pistachio.Options{
				ConnString: conn.Config().ConnString(),
				Schemas:    []string{"public"},
			})
			got, err := client.Dump(ctx, &pistachio.DumpOptions{})
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Dump), strings.TrimSpace(got.String()))
		})
	}
}
