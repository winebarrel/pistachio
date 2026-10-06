package document_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/document"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

func newDocument(t *testing.T, sql string, schemas ...string) *document.Document {
	t.Helper()

	if len(schemas) == 0 {
		schemas = []string{"public"}
	}
	result, err := parser.ParseSQLSourcesWithSchema([]parser.Source{{Name: "schema.sql", SQL: sql}}, schemas[0])
	require.NoError(t, err)
	return document.New(result, schemas)
}

func columnsByName(table *document.Table) map[string]*document.Column {
	columns := map[string]*document.Column{}
	for _, c := range table.Columns {
		columns[c.Name] = c
	}
	return columns
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

func TestNew_ColumnBaseType(t *testing.T) {
	doc := newDocument(t, `
CREATE DOMAIN public.ts AS timestamp(3);
CREATE DOMAIN public.ts2 AS public.ts;
CREATE DOMAIN public.ints AS int[];
CREATE DOMAIN other."My Dom" AS varchar(10);
CREATE TYPE public.st AS ENUM ('a');
CREATE TYPE public."t(1)" AS ENUM ('a');
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
  n unknown_ext_type,
  o ts[],
  p public."t(1)",
  q time(3) with time zone,
  r bit varying(5),
  s double precision,
  u int[][],
  v character(3),
  w text
);`)

	table := doc.Tables.Get("public.t")
	require.NotNil(t, table)
	columns := columnsByName(table)

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
		{"o", "timestamp without time zone", true},
		{"p", `public."t(1)"`, false},
		{"q", "time with time zone", false},
		{"r", "bit varying", false},
		{"s", "double precision", false},
		{"u", "integer", true},
		{"v", "character", false},
		{"w", "text", false},
	}
	for _, c := range cases {
		col := columns[c.name]
		require.NotNil(t, col, c.name)
		assert.Equal(t, c.baseType, col.BaseType, c.name)
		assert.Equal(t, c.isArray, col.IsArray, c.name)
	}

	// The columns keep the order the table declares them in.
	assert.Equal(t, "a", table.Columns[0].Name)
	assert.Equal(t, "w", table.Columns[len(table.Columns)-1].Name)
}

// An unqualified domain name is looked up in the schemas in order and then in
// public, the search path that plan, apply and dump set.
func TestNew_DomainSearchPath(t *testing.T) {
	const sql = `
CREATE DOMAIN app.code AS char(3);
CREATE DOMAIN public.code AS text;
CREATE DOMAIN public.note AS varchar(10);
CREATE DOMAIN other.flag AS boolean;
CREATE TABLE app.t (c code, n note, f flag);`

	columns := columnsByName(newDocument(t, sql, "app").Tables.Get("app.t"))
	assert.Equal(t, "character", columns["c"].BaseType, "the first schema wins over public")
	assert.Equal(t, "character varying", columns["n"].BaseType, "public is searched last")
	assert.Equal(t, "flag", columns["f"].BaseType, "a schema off the search path is not searched")

	columns = columnsByName(newDocument(t, sql, "public", "app").Tables.Get("app.t"))
	assert.Equal(t, "text", columns["c"].BaseType, "schemas are searched in order")
}

// A domain that refers to itself through another does not loop.
func TestNew_DomainCycle(t *testing.T) {
	doc := newDocument(t, `
CREATE DOMAIN public.a AS public.b;
CREATE DOMAIN public.b AS public.a;
CREATE TABLE public.t (c public.a);`)

	assert.Equal(t, "public.a", doc.Tables.Get("public.t").Columns[0].BaseType)
}

func TestNew_Index(t *testing.T) {
	doc := newDocument(t, `
CREATE TABLE public.t (a int, b text, c int, d int);
CREATE UNIQUE INDEX t_full_idx ON public.t USING btree (a DESC, lower(b)) INCLUDE (c, d) WHERE a > 0;
CREATE INDEX t_plain_idx ON public.t (a, c);
CREATE INDEX t_gin_idx ON public.t USING gin (to_tsvector('simple', b));
CREATE INDEX t_ops_idx ON public.t (b text_pattern_ops DESC NULLS FIRST, b COLLATE "C");`)

	indexes := doc.Tables.Get("public.t").Indexes

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

	// An operator class, a sort order or a collation does not make a column
	// an expression.
	ops := indexes.Get("t_ops_idx")
	assert.Equal(t, []*string{new("b"), new("b")}, ops.Columns)
}

func TestNew_MaterializedViewIndex(t *testing.T) {
	doc := newDocument(t, `
CREATE TABLE public.t (a int);
CREATE MATERIALIZED VIEW public.mv AS SELECT a FROM public.t;
CREATE UNIQUE INDEX mv_a_idx ON public.mv (a);`)

	idx := doc.Views.Get("public.mv").Indexes.Get("mv_a_idx")
	assert.Equal(t, []*string{new("a")}, idx.Columns)
	assert.True(t, idx.Unique)
}

func TestNew_ForeignKey(t *testing.T) {
	doc := newDocument(t, `
CREATE TABLE public.users (id bigint NOT NULL, org bigint NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id), CONSTRAINT users_org_key UNIQUE (org, id));
CREATE TABLE public.posts (
  id bigint,
  user_id bigint,
  org bigint,
  CONSTRAINT posts_user_fkey FOREIGN KEY (user_id) REFERENCES users ON DELETE CASCADE,
  CONSTRAINT posts_org_fkey FOREIGN KEY (org, user_id) REFERENCES public.users (org, id) MATCH FULL ON UPDATE SET NULL ON DELETE RESTRICT,
  CONSTRAINT posts_lazy_fkey FOREIGN KEY (id) REFERENCES public.users (id) ON UPDATE SET DEFAULT DEFERRABLE INITIALLY DEFERRED
);`)

	fks := doc.Tables.Get("public.posts").ForeignKeys

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

	lazy := fks.Get("posts_lazy_fkey")
	assert.Equal(t, []string{"id"}, lazy.RefColumns)
	assert.Equal(t, "no action", lazy.OnDelete)
	assert.Equal(t, "set default", lazy.OnUpdate)
}

// The document reads only the model, so a model the catalog builds gets the
// same values.
func TestNew_CatalogModel(t *testing.T) {
	table := &model.Table{Schema: "public", Name: "t"}
	table.Columns = newColumns(&model.Column{Name: "a", TypeName: "character varying(20)[]"})
	table.Indexes = newIndexes(&model.Index{Schema: "public", Name: "t_a_idx", Table: "t", Definition: "CREATE INDEX t_a_idx ON ONLY public.t USING hash (a)"})
	table.ForeignKeys = newForeignKeys(&model.ForeignKey{
		Name: "t_a_fkey", Definition: "FOREIGN KEY (a) REFERENCES other.\"Users\"(\"Name\") MATCH FULL ON UPDATE CASCADE ON DELETE SET DEFAULT NOT VALID",
	})

	doc := document.New(&parser.ParseResult{Tables: newTables(table)}, []string{"public"})
	got := doc.Tables.Get("public.t")

	assert.Equal(t, "character varying", got.Columns[0].BaseType)
	assert.True(t, got.Columns[0].IsArray)

	idx := got.Indexes.Get("t_a_idx")
	assert.Equal(t, []*string{new("a")}, idx.Columns)
	assert.Equal(t, "hash", idx.Method)

	fk := got.ForeignKeys.Get("t_a_fkey")
	assert.Equal(t, []string{"Name"}, fk.RefColumns)
	assert.Equal(t, "set default", fk.OnDelete)
	assert.Equal(t, "cascade", fk.OnUpdate)
	assert.Equal(t, "full", fk.Match)
}

// The model is left as it was: the added fields are only in the document.
func TestNew_LeavesModelAlone(t *testing.T) {
	table := &model.Table{Schema: "public", Name: "t"}
	col := &model.Column{Name: "a", TypeName: "integer[]"}
	table.Columns = newColumns(col)
	result := &parser.ParseResult{Tables: newTables(table)}

	doc := document.New(result, []string{"public"})

	assert.Same(t, table, doc.Tables.Get("public.t").Table)
	assert.Same(t, col, doc.Tables.Get("public.t").Columns[0].Column)
	assert.Equal(t, "integer[]", col.TypeName)
}

// An empty document, or a table with no columns, keeps its nil maps.
func TestNew_Empty(t *testing.T) {
	doc := document.New(&parser.ParseResult{}, []string{"public"})
	assert.Nil(t, doc.Tables)
	assert.Nil(t, doc.Views)

	doc = document.New(&parser.ParseResult{Tables: newTables(&model.Table{Schema: "public", Name: "t"})}, []string{"public"})
	table := doc.Tables.Get("public.t")
	assert.Nil(t, table.Columns)
	assert.Nil(t, table.Indexes)
	assert.Nil(t, table.ForeignKeys)
}

// A definition that pg_query cannot read leaves the added fields empty rather
// than failing the document. PostgreSQL 18 writes such definitions, and the
// parser reads the PostgreSQL 17 grammar.
func TestNew_UnreadableDefinition(t *testing.T) {
	table := &model.Table{Schema: "public", Name: "t"}
	table.Indexes = newIndexes(
		&model.Index{Schema: "public", Name: "i_overlaps", Table: "t", Definition: "CREATE UNIQUE INDEX i_overlaps ON public.t USING gist (a, p WITHOUT OVERLAPS)"},
		&model.Index{Schema: "public", Name: "i_select", Table: "t", Definition: "SELECT 1"},
	)
	table.ForeignKeys = newForeignKeys(
		&model.ForeignKey{Name: "f_not_enforced", Definition: "FOREIGN KEY (a) REFERENCES public.u(b) NOT ENFORCED"},
		&model.ForeignKey{Name: "f_check", Definition: "CHECK (a > 0)"},
		&model.ForeignKey{Name: "f_two", Definition: "CHECK (a > 0), ADD COLUMN b integer"},
	)
	view := &model.View{Schema: "public", Name: "mv", Materialized: true}
	view.Indexes = newIndexes(&model.Index{Schema: "public", Name: "mv_i", Table: "mv", Definition: "not sql"})
	views := orderedmap.New[string, *model.View]()
	views.Set("public.mv", view)

	doc := document.New(&parser.ParseResult{Tables: newTables(table), Views: views}, []string{"public"})
	got := doc.Tables.Get("public.t")

	for name, idx := range got.Indexes.All() {
		assert.Equal(t, []*string{}, idx.Columns, name)
		assert.Equal(t, []string{}, idx.Include, name)
		assert.Empty(t, idx.Method, name)
	}
	for name, fk := range got.ForeignKeys.All() {
		assert.Equal(t, []string{}, fk.RefColumns, name)
		assert.Empty(t, fk.OnDelete, name)
		assert.Empty(t, fk.Match, name)
	}
	assert.Equal(t, []*string{}, doc.Views.Get("public.mv").Indexes.Get("mv_i").Columns)
}

// auto_named marks an index or a constraint that the files declare without a
// name, so that pistachio named it the way PostgreSQL would.
func TestNew_AutoNamed(t *testing.T) {
	doc := newDocument(t, `
CREATE TABLE public.u (id bigint PRIMARY KEY);
CREATE TABLE public.t (
  id bigint,
  a bigint REFERENCES public.u (id),
  b bigint UNIQUE,
  c bigint CHECK (c > 0),
  d bigint,
  e bigint,
  f bigint CONSTRAINT t_f_named CHECK (f > 0),
  PRIMARY KEY (id),
  FOREIGN KEY (d) REFERENCES public.u (id),
  CONSTRAINT t_e_fkey FOREIGN KEY (e) REFERENCES public.u (id)
);
ALTER TABLE public.t ADD FOREIGN KEY (b) REFERENCES public.u (id);
ALTER TABLE public.t ADD CONSTRAINT t_c_fkey FOREIGN KEY (c) REFERENCES public.u (id);
ALTER TABLE public.t ADD UNIQUE (d);
CREATE INDEX ON public.t (e);
CREATE INDEX t_f_idx ON public.t (f);
CREATE MATERIALIZED VIEW public.mv AS SELECT id FROM public.t;
CREATE INDEX ON public.mv (id);
CREATE INDEX mv_named ON public.mv (id);`)

	u := doc.Tables.Get("public.u")
	assert.True(t, u.Constraints.Get("u_pkey").AutoNamed)

	tbl := doc.Tables.Get("public.t")
	constraints := map[string]bool{}
	for key, c := range tbl.Constraints.All() {
		constraints[key] = c.AutoNamed
	}
	assert.Equal(t, map[string]bool{
		"t_pkey": true, "t_b_key": true, "t_c_check": true, "t_f_named": false, "t_d_key": true,
	}, constraints)

	fks := map[string]bool{}
	for key, fk := range tbl.ForeignKeys.All() {
		fks[key] = fk.AutoNamed
	}
	assert.Equal(t, map[string]bool{
		"t_a_fkey": true, "t_d_fkey": true, "t_e_fkey": false, "t_b_fkey": true, "t_c_fkey": false,
	}, fks)

	assert.True(t, tbl.Indexes.Get("t_e_idx").AutoNamed)
	assert.False(t, tbl.Indexes.Get("t_f_idx").AutoNamed)

	mv := doc.Views.Get("public.mv")
	assert.True(t, mv.Indexes.Get("mv_id_idx").AutoNamed)
	assert.False(t, mv.Indexes.Get("mv_named").AutoNamed)
}

// A constraint named after an index with USING INDEX takes the index's name,
// which the file did not write for the constraint.
func TestNew_AutoNamedUsingIndex(t *testing.T) {
	doc := newDocument(t, `
CREATE TABLE public.t (a bigint NOT NULL);
CREATE UNIQUE INDEX t_a_idx ON public.t (a);
ALTER TABLE public.t ADD UNIQUE USING INDEX t_a_idx;`)

	assert.True(t, doc.Tables.Get("public.t").Constraints.Get("t_a_idx").AutoNamed)
}
