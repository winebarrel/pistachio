package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

func parseDerived(t *testing.T, sql string, schemas ...string) *parser.ParseResult {
	t.Helper()

	if len(schemas) == 0 {
		schemas = []string{"public"}
	}
	result, err := parser.ParseSQLSourcesWithSchema([]parser.Source{{Name: "schema.sql", SQL: sql}}, schemas[0])
	require.NoError(t, err)
	require.NoError(t, result.FillDerived(schemas))
	return result
}

func newTables(tables ...*model.Table) *orderedmap.Map[string, *model.Table] {
	m := orderedmap.New[string, *model.Table]()
	for _, t := range tables {
		m.Set(t.FQTN(), t)
	}
	return m
}

func newColumns(columns ...*model.Column) *orderedmap.Map[string, *model.Column] {
	m := orderedmap.New[string, *model.Column]()
	for _, c := range columns {
		m.Set(c.Name, c)
	}
	return m
}

func newIndexes(indexes ...*model.Index) *orderedmap.Map[string, *model.Index] {
	m := orderedmap.New[string, *model.Index]()
	for _, i := range indexes {
		m.Set(i.Name, i)
	}
	return m
}

func newForeignKeys(fks ...*model.ForeignKey) *orderedmap.Map[string, *model.ForeignKey] {
	m := orderedmap.New[string, *model.ForeignKey]()
	for _, fk := range fks {
		m.Set(fk.Name, fk)
	}
	return m
}

func TestFillDerived_ColumnBaseType(t *testing.T) {
	result := parseDerived(t, `
CREATE DOMAIN public.ts AS timestamp(3);
CREATE DOMAIN public.ts2 AS public.ts;
CREATE DOMAIN public.ints AS int[];
CREATE DOMAIN other."My Dom" AS varchar(10);
CREATE TYPE public.st AS ENUM ('a');
CREATE TABLE public.t (
  a timestamp,
  b timestamp(3) without time zone,
  c ts,
  d public.ts2,
  e bigserial,
  f serial,
  g smallserial,
  h int[],
  i numeric(10,2)[],
  j interval day to second(3),
  k st,
  l public.ints,
  m other."My Dom",
  n unknown_ext_type
);`)

	table := result.Tables.Get("public.t")
	require.NotNil(t, table)

	cases := []struct {
		name     string
		baseType string
		isArray  bool
	}{
		{"a", "timestamp without time zone", false},
		{"b", "timestamp without time zone", false},
		{"c", "timestamp without time zone", false},
		{"d", "timestamp without time zone", false},
		{"e", "bigint", false},
		{"f", "integer", false},
		{"g", "smallint", false},
		{"h", "integer", true},
		{"i", "numeric", true},
		{"j", "interval day to second", false},
		{"k", "st", false},
		{"l", "integer", true},
		{"m", "character varying", false},
		{"n", "unknown_ext_type", false},
	}
	for _, c := range cases {
		col := table.Columns.Get(c.name)
		require.NotNil(t, col, c.name)
		assert.Equal(t, c.baseType, col.BaseType, c.name)
		assert.Equal(t, c.isArray, col.IsArray, c.name)
	}
}

// An unqualified domain name resolves against the schemas in order, the way
// the catalog leaves a name unqualified when the search path reaches it.
func TestFillDerived_DomainSearchesSchemas(t *testing.T) {
	result := parseDerived(t, `
CREATE DOMAIN app.code AS char(3);
CREATE TABLE app.t (c code);`, "app", "public")

	col := result.Tables.Get("app.t").Columns.Get("c")
	assert.Equal(t, "character", col.BaseType)
}

// A domain that names itself through another does not loop.
func TestFillDerived_DomainCycle(t *testing.T) {
	result := parseDerived(t, `
CREATE DOMAIN public.a AS public.b;
CREATE DOMAIN public.b AS public.a;
CREATE TABLE public.t (c public.a);`)

	col := result.Tables.Get("public.t").Columns.Get("c")
	assert.NotEmpty(t, col.BaseType)
}

func TestFillDerived_Index(t *testing.T) {
	result := parseDerived(t, `
CREATE TABLE public.t (a int, b text, c int, d int);
CREATE UNIQUE INDEX t_full_idx ON public.t USING btree (a DESC, lower(b)) INCLUDE (c, d) WHERE a > 0;
CREATE INDEX t_plain_idx ON public.t (a, c);
CREATE INDEX t_gin_idx ON public.t USING gin (to_tsvector('simple', b));`)

	indexes := result.Tables.Get("public.t").Indexes

	full := indexes.Get("t_full_idx")
	assert.Equal(t, []*string{new("a"), nil}, full.Columns)
	assert.Equal(t, []string{"c", "d"}, full.Include)
	assert.True(t, full.Unique)
	assert.Equal(t, "btree", full.Method)
	assert.True(t, full.Partial)

	plain := indexes.Get("t_plain_idx")
	assert.Equal(t, []*string{new("a"), new("c")}, plain.Columns)
	assert.Equal(t, []string{}, plain.Include)
	assert.False(t, plain.Unique)
	assert.Equal(t, "btree", plain.Method)
	assert.False(t, plain.Partial)

	gin := indexes.Get("t_gin_idx")
	assert.Equal(t, []*string{nil}, gin.Columns)
	assert.Equal(t, "gin", gin.Method)
}

func TestFillDerived_MaterializedViewIndex(t *testing.T) {
	result := parseDerived(t, `
CREATE TABLE public.t (a int);
CREATE MATERIALIZED VIEW public.mv AS SELECT a FROM public.t;
CREATE UNIQUE INDEX mv_a_idx ON public.mv (a);`)

	idx := result.Views.Get("public.mv").Indexes.Get("mv_a_idx")
	assert.Equal(t, []*string{new("a")}, idx.Columns)
	assert.True(t, idx.Unique)
}

func TestFillDerived_ForeignKey(t *testing.T) {
	result := parseDerived(t, `
CREATE TABLE public.users (id bigint NOT NULL, org bigint NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id), CONSTRAINT users_org_key UNIQUE (org, id));
CREATE TABLE public.posts (
  id bigint,
  user_id bigint,
  org bigint,
  CONSTRAINT posts_user_fkey FOREIGN KEY (user_id) REFERENCES users ON DELETE CASCADE,
  CONSTRAINT posts_org_fkey FOREIGN KEY (org, user_id) REFERENCES public.users (org, id) MATCH FULL ON UPDATE SET NULL ON DELETE RESTRICT
);`)

	fks := result.Tables.Get("public.posts").ForeignKeys

	user := fks.Get("posts_user_fkey")
	assert.Equal(t, []string{"id"}, user.RefColumns)
	assert.Equal(t, "cascade", user.OnDelete)
	assert.Equal(t, "no action", user.OnUpdate)
	assert.Equal(t, "simple", user.Match)

	org := fks.Get("posts_org_fkey")
	assert.Equal(t, []string{"org", "id"}, org.RefColumns)
	assert.Equal(t, "restrict", org.OnDelete)
	assert.Equal(t, "set null", org.OnUpdate)
	assert.Equal(t, "full", org.Match)
}

// The fields are derived from what the model already holds, so a document the
// catalog side builds gets them the same way.
func TestFillDerived_HandBuiltModel(t *testing.T) {
	table := &model.Table{Schema: "public", Name: "t"}
	table.Columns = newColumns(&model.Column{Name: "a", TypeName: "character varying(20)[]"})
	table.Indexes = newIndexes(&model.Index{Schema: "public", Name: "t_a_idx", Table: "t", Definition: "CREATE INDEX t_a_idx ON ONLY public.t USING hash (a)"})
	table.ForeignKeys = newForeignKeys(&model.ForeignKey{
		Name: "t_a_fkey", Definition: "FOREIGN KEY (a) REFERENCES other.\"Users\"(\"Name\") ON UPDATE CASCADE ON DELETE SET DEFAULT",
	})

	result := &parser.ParseResult{Tables: newTables(table)}
	require.NoError(t, result.FillDerived([]string{"public"}))

	col := table.Columns.Get("a")
	assert.Equal(t, "character varying", col.BaseType)
	assert.True(t, col.IsArray)

	idx := table.Indexes.Get("t_a_idx")
	assert.Equal(t, []*string{new("a")}, idx.Columns)
	assert.Equal(t, "hash", idx.Method)

	fk := table.ForeignKeys.Get("t_a_fkey")
	assert.Equal(t, []string{"Name"}, fk.RefColumns)
	assert.Equal(t, "set default", fk.OnDelete)
	assert.Equal(t, "cascade", fk.OnUpdate)
}

func TestFillDerived_BadDefinition(t *testing.T) {
	table := &model.Table{Schema: "public", Name: "t"}
	table.Indexes = newIndexes(&model.Index{Schema: "public", Name: "i", Table: "t", Definition: "not sql"})

	result := &parser.ParseResult{Tables: newTables(table)}
	require.ErrorContains(t, result.FillDerived([]string{"public"}), "index public.i")

	table.Indexes = nil
	table.ForeignKeys = newForeignKeys(&model.ForeignKey{Name: "f", Definition: "not sql"})
	assert.ErrorContains(t, result.FillDerived([]string{"public"}), "foreign key f")
}
