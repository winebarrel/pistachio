package command_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/cmd/command"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestDump_Run(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	err := cmd.Run(ctx, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "-- Dump of schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)")
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
	assertConnectedCommentFirst(t, buf.String(), conn.Config())
}

func TestDump_Run_Split(t *testing.T) {
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

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	splitDir := filepath.Join(t.TempDir(), "split_output")
	var buf bytes.Buffer
	cmd := &command.Dump{Options: options, Split: splitDir}
	err := cmd.Run(ctx, &buf)
	require.NoError(t, err)

	usersData, err := os.ReadFile(filepath.Join(splitDir, "public.users.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(usersData), "CREATE TABLE public.users")

	postsData, err := os.ReadFile(filepath.Join(splitDir, "public.posts.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(postsData), "CREATE TABLE public.posts")

	out := buf.String()
	assert.Contains(t, out, "-- Dump of schema public (2 tables, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)")
	assert.Contains(t, out, fmt.Sprintf("-- Wrote 2 file(s) to %s", splitDir))
	assert.NotContains(t, out, "public.users.sql")
	assert.NotContains(t, out, "public.posts.sql")
}

func TestDump_Run_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	err := cmd.Run(ctx, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "-- Dump of schema public (0 tables, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)")
}

func TestDump_Run_Split_WithView(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	splitDir := filepath.Join(t.TempDir(), "split_output")
	var buf bytes.Buffer
	cmd := &command.Dump{Options: options, Split: splitDir}
	err := cmd.Run(ctx, &buf)
	require.NoError(t, err)

	usersData, err := os.ReadFile(filepath.Join(splitDir, "public.users.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(usersData), "CREATE TABLE public.users")

	viewData, err := os.ReadFile(filepath.Join(splitDir, "public.active_users.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(viewData), "CREATE OR REPLACE VIEW")
}

func TestDump_Run_Split_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	splitDir := filepath.Join(t.TempDir(), "split_output")
	var buf bytes.Buffer
	cmd := &command.Dump{Options: options, Split: splitDir}
	err := cmd.Run(ctx, &buf)
	require.NoError(t, err)

	entries, err := os.ReadDir(splitDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
	out := buf.String()
	assert.Contains(t, out, "-- Dump of schema public (0 tables, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)")
	assert.Contains(t, out, fmt.Sprintf("-- Wrote 0 file(s) to %s", splitDir))
}

func TestDump_Run_Split_SpecialCharacters(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public."My Table" (
    id integer NOT NULL
);`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	splitDir := filepath.Join(t.TempDir(), "split_output")
	var buf bytes.Buffer
	cmd := &command.Dump{Options: options, Split: splitDir}
	err := cmd.Run(ctx, &buf)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(splitDir, "public.My_Table.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(data), `CREATE TABLE public."My Table"`)
}

func TestDump_Run_Split_MkdirError(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	// Use a regular file as the parent so MkdirAll fails on every
	// platform, unlike /dev/null which is a plain path on Windows.
	notADir := filepath.Join(t.TempDir(), "not_a_dir")
	require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o644))

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options, Split: filepath.Join(notADir, "invalid")}
	err := cmd.Run(ctx, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create directory")
	// MkdirAll runs before the header, so nothing should land on stdout.
	assert.Empty(t, buf.String())
}

func TestDump_Run_Split_WriteError(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	// Force os.WriteFile to fail by pre-creating a directory where the
	// dump file would go. Writing to a directory path fails on every
	// platform (EISDIR / access denied), including as root, unlike a
	// read-only parent whose permission bits root and Windows ignore.
	splitDir := filepath.Join(t.TempDir(), "split_output")
	require.NoError(t, os.MkdirAll(filepath.Join(splitDir, "public.users.sql"), 0o755))

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options, Split: splitDir}
	err := cmd.Run(ctx, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write")
	// On the write-failure path the header has already been emitted
	// (it precedes the file loop) but the footer must not appear,
	// so partial output is unambiguous from a successful run.
	out := buf.String()
	assert.Contains(t, out, "-- Dump of schema public")
	assert.NotContains(t, out, "-- Wrote")
}

func TestDump_Run_Error(t *testing.T) {
	ctx := context.Background()
	options := pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	err := cmd.Run(ctx, &buf)
	require.Error(t, err)
}

func TestDump_Run_JSON(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TYPE public.st AS ENUM ('active', 'archived');
CREATE TABLE public.users (
    id bigint GENERATED ALWAYS AS IDENTITY,
    email text NOT NULL,
    state public.st NOT NULL DEFAULT 'active',
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_email ON public.users (email);
CREATE VIEW public.live AS SELECT id FROM public.users;`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	cmd.JSON = true
	require.NoError(t, cmd.Run(ctx, &buf))

	// No SQL comment can precede the document.
	assert.True(t, strings.HasPrefix(buf.String(), "{\n"), "the document starts the output")

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	table := result["tables"].(map[string]any)["public.users"].(map[string]any)
	assert.Equal(t, "users", table["name"])

	columns := table["columns"].(map[string]any)
	assert.Equal(t, "always", columns["id"].(map[string]any)["identity"])
	assert.Equal(t, true, columns["email"].(map[string]any)["not_null"])

	assert.Contains(t, table["indexes"], "idx_users_email")
	assert.Contains(t, result["views"], "public.live")
	assert.Contains(t, result["enums"], "public.st")

	// A database holds no execute statements, and the key is written all the
	// same, so the document is the shape parse writes.
	assert.Equal(t, []any{}, result["execute_stmts"])

	// The catalog fills what a schema file cannot say.
	assert.NotEqual(t, float64(0), table["oid"], "the catalog reports an OID")
	assert.Equal(t, "extended", columns["email"].(map[string]any)["type_storage"])
}

// The document dump writes is the one the published schema describes, which is
// why the schema's name carries no command.
func TestDump_Run_JSON_MatchesJSONSchema(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE DOMAIN public.email AS text NOT NULL CHECK (VALUE ~ '@');
CREATE TYPE public.addr AS (street text, city text);
CREATE SEQUENCE public.counter START 5 CYCLE;
CREATE TABLE public.users (
    id bigint GENERATED BY DEFAULT AS IDENTITY,
    addr public.email,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_users ON public.users AS RESTRICTIVE FOR SELECT USING (id > 0);
CREATE MATERIALIZED VIEW public.recent AS SELECT id FROM public.users;`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	cmd.JSON = true
	require.NoError(t, cmd.Run(ctx, &buf))

	assert.NoError(t, validateAgainstJSONSchema(t, buf.Bytes()))
}

func TestDump_Run_JSON_OmitSchema(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer);`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	cmd.JSON = true
	cmd.OmitSchema = true
	require.NoError(t, cmd.Run(ctx, &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	table := result["tables"].(map[string]any)["users"].(map[string]any)
	assert.Empty(t, table["schema"], "--omit-schema reaches the document")
}

// The filters and --manage-routine decide what the dump holds, so they decide
// what the document holds.
func TestDump_Run_JSON_FiltersAndRoutines(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer);
CREATE TABLE public.tmp_scratch (id integer);
CREATE FUNCTION public.noop() RETURNS void LANGUAGE sql AS 'SELECT';`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	read := func(t *testing.T, cmd *command.Dump) map[string]any {
		t.Helper()
		var buf bytes.Buffer
		require.NoError(t, cmd.Run(ctx, &buf))
		var result map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

		return result
	}

	plain := &command.Dump{Options: options}
	plain.JSON = true
	result := read(t, plain)
	assert.Contains(t, result["tables"], "public.tmp_scratch")
	assert.Empty(t, result["routines"], "routines are unmanaged without --manage-routine")

	filtered := &command.Dump{Options: options}
	filtered.JSON = true
	filtered.Exclude = []string{"tmp_*"}
	filtered.ManageRoutine = true
	result = read(t, filtered)
	assert.NotContains(t, result["tables"], "public.tmp_scratch", "--exclude reaches the document")
	assert.Contains(t, result["tables"], "public.users")
	assert.Contains(t, result["routines"], "public.noop()")
}

// A schema holding nothing still writes every object kind, and the document is
// one the schema accepts.
func TestDump_Run_JSON_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	cmd.JSON = true
	require.NoError(t, cmd.Run(ctx, &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
	for _, key := range []string{
		"tables", "views", "enums", "domains", "composite_types", "sequences", "routines",
	} {
		assert.Equal(t, map[string]any{}, result[key], key)
	}
	assert.Equal(t, []any{}, result["execute_stmts"])

	assert.NoError(t, validateAgainstJSONSchema(t, buf.Bytes()))
}

// Storage parameters are unmanaged without --manage-storage-param, and so are
// routines without --manage-routine. Both are written as an empty object, so
// one document does not spell the same fact two ways.
func TestDump_Run_JSON_UnmanagedAreEmptyObjects(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.docs (id integer) WITH (fillfactor = 70);
CREATE MATERIALIZED VIEW public.recent AS SELECT id FROM public.docs;
CREATE FUNCTION public.noop() RETURNS void LANGUAGE sql AS 'SELECT';`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	cmd.JSON = true
	require.NoError(t, cmd.Run(ctx, &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	table := result["tables"].(map[string]any)["public.docs"].(map[string]any)
	assert.Equal(t, map[string]any{}, table["storage_params"], "the table's fillfactor was not read")

	view := result["views"].(map[string]any)["public.recent"].(map[string]any)
	assert.Equal(t, map[string]any{}, view["storage_params"])

	assert.Equal(t, map[string]any{}, result["routines"])

	// With the flag the parameters are there, so the empty object above is the
	// unmanaged case rather than a table that carries none.
	buf.Reset()
	managed := &command.Dump{Options: options}
	managed.JSON = true
	managed.ManageStorageParam = true
	require.NoError(t, managed.Run(ctx, &buf))

	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
	table = result["tables"].(map[string]any)["public.docs"].(map[string]any)
	assert.Equal(t, "70", table["storage_params"].(map[string]any)["fillfactor"])
}

// The catalog reports a column's storage for every column, while the parser
// fills it only where the file writes SET STORAGE. The guide says so, and this
// is the case it describes.
func TestDump_Run_JSON_StorageTypeIsCatalogWide(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.docs (id integer, body text, note text);
ALTER TABLE public.docs ALTER COLUMN body SET STORAGE EXTERNAL;`)

	options := pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	}

	var buf bytes.Buffer
	cmd := &command.Dump{Options: options}
	cmd.JSON = true
	require.NoError(t, cmd.Run(ctx, &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
	columns := result["tables"].(map[string]any)["public.docs"].(map[string]any)["columns"].(map[string]any)

	// Every column carries one, written or not.
	assert.Equal(t, "plain", columns["id"].(map[string]any)["storage_type"])
	assert.Equal(t, "external", columns["body"].(map[string]any)["storage_type"])
	assert.Equal(t, "extended", columns["note"].(map[string]any)["storage_type"])
}
