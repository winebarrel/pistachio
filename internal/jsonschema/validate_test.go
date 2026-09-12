package jsonschema_test

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pistaschema "github.com/winebarrel/pistachio/internal/jsonschema"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

// compileSchema compiles the schema as it is committed, so the test measures
// the published file rather than what the generator would write now. The
// drift test is what ties the two together.
func compileSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()

	path := repoFile(t, pistaschema.Path)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	doc, err := jsonschema.UnmarshalJSON(f)
	require.NoError(t, err)

	c := jsonschema.NewCompiler()
	require.NoError(t, c.AddResource(pistaschema.ID, doc))

	schema, err := c.Compile(pistaschema.ID)
	require.NoError(t, err)

	return schema
}

// parseDocument parses the files and returns the document as an any tree, the
// way a consumer reads it. The marshaling matches command.Parse: json/v2 with
// the model's marshalers, so the field presence and the shape are what the
// command writes.
func parseDocument(t *testing.T, files ...string) any {
	t.Helper()

	result, err := parser.ParseSQLFilesWithSchema(files, "public")
	require.NoError(t, err)

	b, err := json.Marshal(result, model.JSONMarshalers)
	require.NoError(t, err)

	var doc any
	require.NoError(t, json.Unmarshal(b, &doc))

	return doc
}

// TestSchema_ValidatesFidelityCorpus runs every schema the fidelity check
// holds through the parser and the published schema. The corpus covers the
// constructs pistachio manages, so a field the schema describes wrongly is
// caught here rather than by a consumer.
func TestSchema_ValidatesFidelityCorpus(t *testing.T) {
	schema := compileSchema(t)

	files, err := filepath.Glob(repoFile(t, "test/fidelity/schemas/*.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "the fidelity schemas are the corpus")

	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			assert.NoError(t, schema.Validate(parseDocument(t, file)))
		})
	}
}

// TestSchema_ValidatesEmptyDocument covers the document a run over a file that
// declares nothing produces, where every object map is empty.
func TestSchema_ValidatesEmptyDocument(t *testing.T) {
	schema := compileSchema(t)

	path := filepath.Join(t.TempDir(), "empty.sql")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	assert.NoError(t, schema.Validate(parseDocument(t, path)))
}

const richSQL = `
create type status as enum ('active', 'archived');
create domain email as text not null check (value ~ '@');
create type address as (street text, city text);
create sequence counter start 5 cycle;

create table users (
    id bigint generated always as identity,
    addr email,
    state status not null default 'active',
    norm text generated always as (lower(addr)) stored,
    body text storage external compression pglz,
    constraint users_pkey primary key (id),
    constraint users_state_check check (state <> 'archived') not valid
);

create index concurrently idx_users_state on users (state);

create table posts (
    id bigint,
    user_id bigint,
    constraint posts_user_fkey foreign key (user_id) references users (id) deferrable initially deferred
);

alter table users enable row level security;
create policy p_users on users as restrictive for select to public using (id > 0);

create function f() returns trigger language plpgsql as $$ begin return new; end $$;
create trigger trg before insert on users for each row execute function f();

create materialized view recent as select id from users;
create view live with (security_barrier = true) as select id from users with cascaded check option;

-- pista:execute select 1
update users set id = id;
`

// TestSchema_ValidatesEveryObjectKind covers the fields the fidelity corpus
// does not reach in one document: a restrictive policy, a deferred foreign
// key, a NOT VALID check, a generated column, a storage and compression
// setting, a cycling sequence, a check option, and an execute directive.
func TestSchema_ValidatesEveryObjectKind(t *testing.T) {
	schema := compileSchema(t)

	path := filepath.Join(t.TempDir(), "rich.sql")
	require.NoError(t, os.WriteFile(path, []byte(richSQL), 0o644))

	doc := parseDocument(t, path)
	require.NoError(t, schema.Validate(doc))

	// The document really does carry every kind, so the pass above is not
	// vacuous.
	top := doc.(map[string]any)
	for _, key := range []string{"tables", "views", "enums", "domains", "composite_types", "sequences"} {
		assert.NotEmpty(t, top[key], key)
	}
	assert.NotEmpty(t, top["execute_stmts"])
}

// TestSchema_RejectsMalformed is what keeps the schema from passing everything:
// a document broken in a way a consumer would care about has to fail.
func TestSchema_RejectsMalformed(t *testing.T) {
	schema := compileSchema(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	require.NoError(t, os.WriteFile(path, []byte(
		"create table t (id bigint not null);\ncreate type status as enum ('a');\n"), 0o644))

	valid := parseDocument(t, path)
	require.NoError(t, schema.Validate(valid))

	table := func(doc any) map[string]any {
		return doc.(map[string]any)["tables"].(map[string]any)["public.t"].(map[string]any)
	}
	column := func(doc any) map[string]any {
		return table(doc)["columns"].([]any)[0].(map[string]any)
	}

	for name, break_ := range map[string]func(doc any){
		"a boolean written as a string": func(doc any) { column(doc)["not_null"] = "yes" },
		"a string written as a number":  func(doc any) { column(doc)["type"] = 1 },
		"a word outside the enum":       func(doc any) { column(doc)["identity"] = "sometimes" },
		"an unknown field":              func(doc any) { column(doc)["bogus"] = true },
		"a missing field":               func(doc any) { delete(column(doc), "not_null") },
		"a missing object kind":         func(doc any) { delete(doc.(map[string]any), "views") },
		"an object where a map belongs": func(doc any) { doc.(map[string]any)["tables"] = []any{} },
		"a map where an array belongs":  func(doc any) { table(doc)["columns"] = map[string]any{} },
		"a null where a string belongs": func(doc any) { table(doc)["name"] = nil },
	} {
		t.Run(name, func(t *testing.T) {
			doc := parseDocument(t, path)
			break_(doc)
			assert.Error(t, schema.Validate(doc))
		})
	}
}

// A pointer field reaching the reader as null is the normal case, not a
// malformed one.
func TestSchema_AcceptsNullForAnUnsetPointer(t *testing.T) {
	schema := compileSchema(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	require.NoError(t, os.WriteFile(path, []byte("create table t (id bigint);\n"), 0o644))

	doc := parseDocument(t, path)
	column := doc.(map[string]any)["tables"].(map[string]any)["public.t"].(map[string]any)["columns"].([]any)[0].(map[string]any)
	require.Nil(t, column["default"], "an unset default reaches the reader as null")

	assert.NoError(t, schema.Validate(doc))
}
