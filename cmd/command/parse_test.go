package command_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/cmd/command"
	pistaschema "github.com/winebarrel/pistachio/internal/jsonschema"
)

const parseSchemaSQL = `
create table items (
    id bigint generated always as identity,
    name text not null,
    constraint items_pkey primary key (id)
);
create index idx_items_name on items (name);
create type status as enum ('active', 'archived');
`

func parseClient() *pistachio.Client {
	return pistachio.NewClient(&pistachio.Options{Schemas: []string{"public"}})
}

func TestParse_Run(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", parseSchemaSQL)

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{path}}
	require.NoError(t, cmd.Run(parseClient(), &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	tables, ok := result["tables"].(map[string]any)
	require.True(t, ok)
	items, ok := tables["public.items"].(map[string]any)
	require.True(t, ok, "unqualified names are qualified with the default schema")

	columns := items["columns"].(map[string]any)
	id := columns["id"].(map[string]any)
	assert.Equal(t, "bigint", id["type"])
	assert.Equal(t, "always", id["identity"])
	name := columns["name"].(map[string]any)
	assert.Equal(t, true, name["not_null"])

	constraints := items["constraints"].(map[string]any)
	pkey := constraints["items_pkey"].(map[string]any)
	assert.Equal(t, "primary_key", pkey["type"])

	indexes := items["indexes"].(map[string]any)
	assert.Len(t, indexes, 1)

	enums := result["enums"].(map[string]any)
	status := enums["public.status"].(map[string]any)
	assert.Equal(t, []any{"active", "archived"}, status["values"])

	// An object kind the files do not declare is an empty map rather than
	// absent.
	assert.Equal(t, map[string]any{}, result["views"])
	assert.Equal(t, map[string]any{}, result["domains"])
}

// Every field is written whatever it holds, so one object of a kind carries
// the same keys as the next.
func TestParse_Run_WritesEveryField(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", "create table t (note text);")

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{path}}
	require.NoError(t, cmd.Run(parseClient(), &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	table := result["tables"].(map[string]any)["public.t"].(map[string]any)
	for _, key := range []string{
		"oid", "schema", "name", "rename_from", "bulk_alter", "ignore",
		"table_space", "storage_params", "unlogged", "partitioned",
		"partition_def", "partition_of", "partition_bound", "row_security",
		"force_row_security", "columns", "constraints", "foreign_keys",
		"indexes", "policies", "triggers", "comment",
	} {
		assert.Contains(t, table, key)
	}

	col := table["columns"].(map[string]any)["note"].(map[string]any)
	for _, key := range []string{
		"name", "rename_from", "type", "serial_sequence", "not_null",
		"not_null_name", "default", "identity", "identity_sequence",
		"generated", "collation", "storage_type", "type_storage",
		"compression", "comment",
	} {
		assert.Contains(t, col, key)
	}

	// The top level carries every object kind, and the execute statements.
	for _, key := range []string{
		"tables", "views", "enums", "domains", "composite_types", "sequences",
		"routines", "execute_stmts",
	} {
		assert.Contains(t, result, key)
	}
	assert.Equal(t, []any{}, result["execute_stmts"])
}

// A check expression carries characters encoding/json v1 escapes. The command
// writes with v2, which leaves them alone.
func TestParse_Run_NoHTMLEscape(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", "create table t (amt numeric, check (amt > 0));")

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{path}}
	require.NoError(t, cmd.Run(parseClient(), &buf))

	assert.Contains(t, buf.String(), "CHECK (amt > 0)")
	assert.NotContains(t, buf.String(), "\\u003e")
}

func TestParse_Run_Directives(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", `
-- pista:renamed-from public.old_items
create table items (id integer);

-- pista:ignore
create table legacy (id integer);

-- pista:execute select 1
update items set id = id;
`)

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{path}}
	require.NoError(t, cmd.Run(parseClient(), &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	tables := result["tables"].(map[string]any)
	items := tables["public.items"].(map[string]any)
	assert.Equal(t, "public.old_items", items["rename_from"])

	legacy := tables["public.legacy"].(map[string]any)
	assert.Equal(t, true, legacy["ignore"])

	stmts := result["execute_stmts"].([]any)
	require.Len(t, stmts, 1)
	stmt := stmts[0].(map[string]any)
	assert.Equal(t, "UPDATE items SET id = id", stmt["sql"])
	assert.Equal(t, "select 1", stmt["check_sql"])
}

func TestParse_Run_AllObjectTypes(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", `
create view recent as select 1 as n;
create domain email as text check (value ~ '@');
create type address as (street text, city text);
create sequence seq start 5;
create function noop() returns void language sql as 'select';
`)

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{path}}
	require.NoError(t, cmd.Run(parseClient(), &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Contains(t, result["views"], "public.recent")
	assert.Contains(t, result["domains"], "public.email")
	assert.Contains(t, result["composite_types"], "public.address")
	assert.Contains(t, result["sequences"], "public.seq")
	assert.Contains(t, result["routines"], "public.noop()")
}

// errWriter fails on the first write, standing in for a closed pipe.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("broken pipe")
}

func TestParse_Run_WriteError(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", "create table t (id integer);")

	cmd := &command.Parse{Files: []string{path}}
	err := cmd.Run(parseClient(), errWriter{})
	require.Error(t, err)
}

func TestParse_Run_ParseError(t *testing.T) {
	path := writeSQLFile(t, "broken.sql", "CREATE TABLE (;")

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{path}}
	err := cmd.Run(parseClient(), &buf)
	require.Error(t, err)
	assert.Empty(t, buf.String())
}

func TestParse_Run_SeveralFiles(t *testing.T) {
	one := writeSQLFile(t, "one.sql", "create table t1 (id integer);")
	two := writeSQLFile(t, "two.sql", "create table t2 (id integer);")

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{one, two}}
	require.NoError(t, cmd.Run(parseClient(), &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
	tables := result["tables"].(map[string]any)
	assert.Contains(t, tables, "public.t1")
	assert.Contains(t, tables, "public.t2")
}

// TestParse_Run_MatchesJSONSchema validates what the command itself writes,
// rather than a document rebuilt beside it, against the published schema.
func TestParse_Run_MatchesJSONSchema(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", parseSchemaSQL)

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{path}}
	require.NoError(t, cmd.Run(parseClient(), &buf))

	f, err := os.Open(filepath.Join("..", "..", filepath.FromSlash(pistaschema.Path)))
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	doc, err := jsonschema.UnmarshalJSON(f)
	require.NoError(t, err)

	c := jsonschema.NewCompiler()
	require.NoError(t, c.AddResource(pistaschema.ID, doc))
	schema, err := c.Compile(pistaschema.ID)
	require.NoError(t, err)

	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.NoError(t, schema.Validate(inst))
}

const renamedEnumSQL = `
CREATE TYPE status AS ENUM (
    -- pista:renamed-from 'a1'
    'alpha',
    -- pista:renamed-from 'b1'
    'bravo',
    -- pista:renamed-from 'c1'
    'charlie',
    -- pista:renamed-from 'd1'
    'delta',
    -- pista:renamed-from 'e1'
    'echo',
    -- pista:renamed-from 'f1'
    'foxtrot'
);
`

// TestParse_Run_IsDeterministic pins the output of one schema to one set of
// bytes. An enum's value_rename_from is a Go map, whose order a run does not
// otherwise repeat, and a document that differs run to run cannot be committed
// or diffed.
func TestParse_Run_IsDeterministic(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", renamedEnumSQL)

	var first string
	for i := range 8 {
		var buf bytes.Buffer
		cmd := &command.Parse{Files: []string{path}}
		require.NoError(t, cmd.Run(parseClient(), &buf))

		if i == 0 {
			first = buf.String()
			require.Contains(t, first, `"value_rename_from"`)

			continue
		}

		assert.Equal(t, first, buf.String(), "run %d differs", i)
	}
}

// Deterministic sorts the keys of a Go map, so value_rename_from comes out in
// that order while the values keep the one the file writes.
func TestParse_Run_ValueRenameFromIsSorted(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", `
CREATE TYPE s AS ENUM (
    -- pista:renamed-from 'z1'
    'zulu',
    -- pista:renamed-from 'y1'
    'yankee',
    -- pista:renamed-from 'a1'
    'alpha'
);
`)

	var buf bytes.Buffer
	cmd := &command.Parse{Files: []string{path}}
	require.NoError(t, cmd.Run(parseClient(), &buf))

	var result map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
	enum := result["enums"].(map[string]any)["public.s"].(map[string]any)
	assert.Equal(t, []any{"zulu", "yankee", "alpha"}, enum["values"], "the file's order")

	// Unmarshaling into a map loses the order, so read the written bytes.
	written := buf.String()
	renames := written[strings.Index(written, `"value_rename_from"`):]
	assert.Less(t, strings.Index(renames, `"alpha"`), strings.Index(renames, `"yankee"`))
	assert.Less(t, strings.Index(renames, `"yankee"`), strings.Index(renames, `"zulu"`))
}
