package command_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/cmd/command"
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

	// Objects the files do not declare still appear as empty maps, so a
	// consumer can index them without checking for presence.
	assert.Equal(t, map[string]any{}, result["views"])
	assert.Equal(t, map[string]any{}, result["domains"])

	// Internal fields stay out of the output.
	assert.NotContains(t, buf.String(), "OID")
	assert.NotContains(t, buf.String(), "oid")
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
