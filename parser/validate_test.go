package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func newValidatableTable(name string, cols ...string) *model.Table {
	t := &model.Table{
		Schema:      "public",
		Name:        name,
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Indexes:     orderedmap.New[string, *model.Index](),
	}
	for _, c := range cols {
		t.Columns.Set(c, &model.Column{Name: c})
	}
	return t
}

func tablesMap(ts ...*model.Table) *orderedmap.Map[string, *model.Table] {
	m := orderedmap.New[string, *model.Table]()
	for _, t := range ts {
		m.Set(t.FQTN(), t)
	}
	return m
}

func TestValidateColumnRefs_Valid(t *testing.T) {
	tbl := newValidatableTable("users", "id", "name")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: "CREATE INDEX idx ON public.users (name)",
	})
	tbl.Constraints.Set("uq", &model.Constraint{
		Name:       "uq",
		Type:       model.ConstraintType('u'),
		Definition: "UNIQUE (name)",
	})
	require.NoError(t, validateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_IndexMissingColumn(t *testing.T) {
	tbl := newValidatableTable("users", "id", "display_name")
	tbl.Indexes.Set("idx_users_name", &model.Index{
		Name:       "idx_users_name",
		Definition: "CREATE INDEX idx_users_name ON public.users (name)",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column name referenced in index idx_users_name does not exist on table public.users")
}

func TestValidateColumnRefs_CheckMissingColumn(t *testing.T) {
	tbl := newValidatableTable("products", "id", "quantity")
	tbl.Constraints.Set("products_qty_check", &model.Constraint{
		Name:       "products_qty_check",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK ((qty > 0))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column qty referenced in CHECK constraint products_qty_check does not exist on table public.products")
}

func TestValidateColumnRefs_FKMissingLocalColumn(t *testing.T) {
	tbl := newValidatableTable("orders", "id", "buyer_id")
	tbl.ForeignKeys.Set("fk", &model.ForeignKey{
		Constraint: model.Constraint{
			Name:       "fk",
			Type:       model.ConstraintType('f'),
			Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)",
		},
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column user_id referenced in foreign key fk does not exist on table public.orders")
}

func TestValidateColumnRefs_FKReferencedColumnNotChecked(t *testing.T) {
	// PkAttrs (referenced columns) are intentionally not validated. A made-up
	// referenced column name should not surface as a violation.
	tbl := newValidatableTable("orders", "id", "user_id")
	tbl.ForeignKeys.Set("fk", &model.ForeignKey{
		Constraint: model.Constraint{
			Name:       "fk",
			Type:       model.ConstraintType('f'),
			Definition: "FOREIGN KEY (user_id) REFERENCES public.users(nonexistent_pk)",
		},
	})
	require.NoError(t, validateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_AggregatesMultiple(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Indexes.Set("idx_a", &model.Index{Name: "idx_a", Definition: "CREATE INDEX idx_a ON public.t (a)"})
	tbl.Constraints.Set("c_b", &model.Constraint{Name: "c_b", Type: model.ConstraintType('c'), Definition: "CHECK ((b > 0))"})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "column a referenced in index idx_a")
	assert.Contains(t, msg, "column b referenced in CHECK constraint c_b")
}

func TestValidateColumnRefs_InheritsChildSkipped(t *testing.T) {
	// INHERITS-style children are represented with PartitionOf set and
	// PartitionBound nil. Their column list is inherited from the parent,
	// so validating their definitions against an empty Columns map would
	// produce false positives.
	parent := "public.events"
	tbl := &model.Table{
		Schema:      "public",
		Name:        "events_v2",
		PartitionOf: &parent,
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Indexes:     orderedmap.New[string, *model.Index](),
	}
	tbl.Constraints.Set("pk", &model.Constraint{
		Name:       "pk",
		Type:       model.ConstraintType('p'),
		Definition: "PRIMARY KEY (id)",
	})
	require.NoError(t, validateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_DeduplicatesPerObject(t *testing.T) {
	// CHECK with multiple references to the same missing column must report
	// the column only once.
	tbl := newValidatableTable("t", "id")
	tbl.Constraints.Set("c", &model.Constraint{
		Name:       "c",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK ((qty > 0 AND qty < 100))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	count := strings.Count(err.Error(), "column qty referenced")
	assert.Equal(t, 1, count)
}

func TestValidateColumnRefs_PartitionChildSkipped(t *testing.T) {
	parent := "public.events"
	bound := "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"
	tbl := &model.Table{
		Schema:         "public",
		Name:           "events_2024",
		PartitionOf:    &parent,
		PartitionBound: &bound,
		Columns:        orderedmap.New[string, *model.Column](),
		Constraints:    orderedmap.New[string, *model.Constraint](),
		ForeignKeys:    orderedmap.New[string, *model.ForeignKey](),
		Indexes:        orderedmap.New[string, *model.Index](),
	}
	// Index references a column not in the (empty) inherited child column set.
	tbl.Indexes.Set("idx", &model.Index{Name: "idx", Definition: "CREATE INDEX idx ON public.events_2024 (id)"})
	require.NoError(t, validateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_ExpressionIndexMissingColumn(t *testing.T) {
	// The renamed column reference is inside a function call expression.
	tbl := newValidatableTable("users", "id", "email_addr")
	tbl.Indexes.Set("idx_users_email_lower", &model.Index{
		Name:       "idx_users_email_lower",
		Definition: "CREATE INDEX idx_users_email_lower ON public.users (lower(email))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column email referenced in index idx_users_email_lower")
}

func TestValidateColumnRefs_FKCompositeLocalPartialMiss(t *testing.T) {
	// FK (a, b) REFERENCES ... where a exists but b is missing locally.
	tbl := newValidatableTable("orders", "id", "tenant_id")
	tbl.ForeignKeys.Set("fk_buyer", &model.ForeignKey{
		Constraint: model.Constraint{
			Name:       "fk_buyer",
			Type:       model.ConstraintType('f'),
			Definition: "FOREIGN KEY (tenant_id, user_id) REFERENCES public.users(tenant_id, id)",
		},
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "column user_id referenced in foreign key fk_buyer")
	assert.NotContains(t, msg, "column tenant_id referenced")
}

func TestValidateColumnRefs_SelfReferencingFK(t *testing.T) {
	// Local-side parent_id resolves to the table's own column set; PkAttrs
	// (id) are out of scope. Validation must pass.
	tbl := newValidatableTable("nodes", "id", "parent_id")
	tbl.ForeignKeys.Set("fk_parent", &model.ForeignKey{
		Constraint: model.Constraint{
			Name:       "fk_parent",
			Type:       model.ConstraintType('f'),
			Definition: "FOREIGN KEY (parent_id) REFERENCES public.nodes(id)",
		},
	})
	require.NoError(t, validateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_ExpressionIndexValid(t *testing.T) {
	tbl := newValidatableTable("users", "id", "email")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: "CREATE INDEX idx ON public.users (lower(email))",
	})
	require.NoError(t, validateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_PartialIndexWhereChecked(t *testing.T) {
	tbl := newValidatableTable("users", "id", "active")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: "CREATE INDEX idx ON public.users (id) WHERE deleted_at IS NULL",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column deleted_at referenced in index idx")
}

func TestValidateColumnRefs_ExclusionChecked(t *testing.T) {
	tbl := newValidatableTable("reservations", "id", "room", "time_range")
	tbl.Constraints.Set("no_overlap", &model.Constraint{
		Name:       "no_overlap",
		Type:       model.ConstraintType('x'),
		Definition: "EXCLUDE USING gist (room WITH =, during WITH &&)",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column during referenced in EXCLUDE constraint no_overlap")
}

func TestValidateColumnRefs_QuotedIdentifier(t *testing.T) {
	tbl := newValidatableTable("t", "id", "MyName")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: `CREATE INDEX idx ON public.t ("MyName")`,
	})
	require.NoError(t, validateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_CheckBoolExprChecked(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Constraints.Set("c", &model.Constraint{
		Name:       "c",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK ((a > 0 AND b < 100))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "column a referenced")
	assert.Contains(t, msg, "column b referenced")
}

func TestValidateColumnRefs_CheckTypeCastChecked(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Constraints.Set("c", &model.Constraint{
		Name:       "c",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK ((status::text = 'a'))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column status referenced")
}

func TestValidateColumnRefs_CheckCoalesceChecked(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Constraints.Set("c", &model.Constraint{
		Name:       "c",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK ((COALESCE(qty, 0) > 0))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column qty referenced")
}

func TestValidateColumnRefs_CheckCaseChecked(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Constraints.Set("c", &model.Constraint{
		Name:       "c",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK (((CASE WHEN flag THEN 1 ELSE 0 END) = 1))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column flag referenced")
}

func TestValidateColumnRefs_CheckInListChecked(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Constraints.Set("c", &model.Constraint{
		Name:       "c",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK ((status IN ('a', 'b')))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column status referenced")
}

func TestValidateColumnRefs_CheckAnyArrayChecked(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Constraints.Set("c", &model.Constraint{
		Name:       "c",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK ((status = ANY (ARRAY['a'::text, 'b'::text])))",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column status referenced")
}

func TestValidateColumnRefs_MultiTableScoping(t *testing.T) {
	// One table is valid, another has a missing reference. The error must
	// name only the offending table.
	good := newValidatableTable("good", "id", "name")
	good.Indexes.Set("idx_good", &model.Index{
		Name:       "idx_good",
		Definition: "CREATE INDEX idx_good ON public.good (name)",
	})

	bad := newValidatableTable("bad", "id", "display_name")
	bad.Indexes.Set("idx_bad", &model.Index{
		Name:       "idx_bad",
		Definition: "CREATE INDEX idx_bad ON public.bad (name)",
	})

	err := validateColumnRefs(tablesMap(good, bad))
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "table public.bad")
	assert.NotContains(t, msg, "table public.good")
}

func TestValidateColumnRefs_CompositeKeyPartialMiss(t *testing.T) {
	// In INDEX (a, b) with only b in the desired column set, only "a" must
	// be reported; not "b".
	tbl := newValidatableTable("t", "id", "b")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: "CREATE INDEX idx ON public.t (a, b)",
	})
	err := validateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "column a referenced in index idx")
	assert.NotContains(t, msg, "column b referenced")
}

func TestCollectColumnRefsInIndexDef_HandlesUnparseable(t *testing.T) {
	assert.Nil(t, collectColumnRefsInIndexDef("not valid sql"))
	assert.Nil(t, collectColumnRefsInIndexDef(""))
	assert.Nil(t, collectColumnRefsInIndexDef("SELECT 1"))
}

func TestCollectColumnRefsInConstraintDef_HandlesUnparseable(t *testing.T) {
	assert.Nil(t, collectColumnRefsInConstraintDef("not a constraint"))
	assert.Nil(t, collectColumnRefsInConstraintDef(""))
}

func TestParseSQLWithSchema_RejectsUnresolvedColumnRef(t *testing.T) {
	// End-to-end through ParseSQLWithSchema so the validation call site in
	// parser.go is exercised by the parser package's own tests.
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON users (name);`

	_, err := parseSQLWithSchema(sql, "public")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column name referenced in index idx_users_name does not exist on table public.users")
}

func TestParseSQLWithSchema_AcceptsResolvedColumnRefs(t *testing.T) {
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON users (name);`

	_, err := parseSQLWithSchema(sql, "public")
	require.NoError(t, err)
}

func TestConstraintKindLabel(t *testing.T) {
	assert.Equal(t, "CHECK constraint", constraintKindLabel(model.ConstraintType('c')))
	assert.Equal(t, "PRIMARY KEY constraint", constraintKindLabel(model.ConstraintType('p')))
	assert.Equal(t, "UNIQUE constraint", constraintKindLabel(model.ConstraintType('u')))
	assert.Equal(t, "FOREIGN KEY constraint", constraintKindLabel(model.ConstraintType('f')))
	assert.Equal(t, "EXCLUDE constraint", constraintKindLabel(model.ConstraintType('x')))
	assert.Equal(t, "constraint", constraintKindLabel(model.ConstraintType('?')))
}

// PostgreSQL keeps tables, views, materialized views, sequences, indexes and
// composite types in pg_class, and tables, views, materialized views,
// composite types, domains and enums in pg_type. pistachio tracks each kind
// in a separate map, so a name reused across kinds is only caught here.
func TestValidateNamespaces_Collision(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "table and view",
			sql: `CREATE TABLE x (id integer);
CREATE VIEW x AS SELECT 1 AS n;`,
			want: "duplicate relation name: public.x (table and view)",
		},
		{
			name: "table and materialized view",
			sql: `CREATE TABLE x (id integer);
CREATE MATERIALIZED VIEW x AS SELECT 1 AS n;`,
			want: "duplicate relation name: public.x (table and materialized view)",
		},
		{
			name: "table and sequence",
			sql: `CREATE TABLE x (id integer);
CREATE SEQUENCE x;`,
			want: "duplicate relation name: public.x (table and sequence)",
		},
		{
			name: "table and composite type",
			sql: `CREATE TABLE x (id integer);
CREATE TYPE x AS (n integer);`,
			want: "duplicate relation name: public.x (table and composite type)",
		},
		{
			name: "view and sequence",
			sql: `CREATE VIEW x AS SELECT 1 AS n;
CREATE SEQUENCE x;`,
			want: "duplicate relation name: public.x (view and sequence)",
		},
		{
			name: "sequence and composite type",
			sql: `CREATE SEQUENCE x;
CREATE TYPE x AS (n integer);`,
			want: "duplicate relation name: public.x (sequence and composite type)",
		},
		{
			name: "index and view",
			sql: `CREATE TABLE t (id integer);
CREATE INDEX x ON t (id);
CREATE VIEW x AS SELECT 1 AS n;`,
			want: "duplicate relation name: public.x (index and view)",
		},
		{
			name: "index and composite type",
			sql: `CREATE TABLE t (id integer);
CREATE INDEX x ON t (id);
CREATE TYPE x AS (n integer);`,
			want: "duplicate relation name: public.x (index and composite type)",
		},
		{
			name: "primary key constraint and another table's index",
			sql: `CREATE TABLE t1 (id integer, CONSTRAINT x PRIMARY KEY (id));
CREATE TABLE t2 (id integer);
CREATE INDEX x ON t2 (id);`,
			want: "duplicate relation name: public.x (PRIMARY KEY constraint and index)",
		},
		{
			// The message names the catalogs in registration order, not the
			// order the statements appear in.
			name: "view declared before the table",
			sql: `CREATE VIEW x AS SELECT 1 AS n;
CREATE TABLE x (id integer);`,
			want: "duplicate relation name: public.x (table and view)",
		},
		{
			name: "primary key constraint and view",
			sql: `CREATE TABLE t (id integer, CONSTRAINT x PRIMARY KEY (id));
CREATE VIEW x AS SELECT 1 AS n;`,
			want: "duplicate relation name: public.x (PRIMARY KEY constraint and view)",
		},
		{
			name: "unique constraint and sequence",
			sql: `CREATE TABLE t (id integer, CONSTRAINT x UNIQUE (id));
CREATE SEQUENCE x;`,
			want: "duplicate relation name: public.x (UNIQUE constraint and sequence)",
		},
		{
			name: "exclude constraint and view",
			sql: `CREATE TABLE t (id integer, CONSTRAINT x EXCLUDE (id WITH =));
CREATE VIEW x AS SELECT 1 AS n;`,
			want: "duplicate relation name: public.x (EXCLUDE constraint and view)",
		},
		{
			name: "table and its own primary key constraint",
			sql:  `CREATE TABLE x (id integer, CONSTRAINT x PRIMARY KEY (id));`,
			want: "duplicate relation name: public.x (table and PRIMARY KEY constraint)",
		},
		{
			name: "materialized view index and view",
			sql: `CREATE MATERIALIZED VIEW mv AS SELECT 1 AS n;
CREATE INDEX x ON mv (n);
CREATE VIEW x AS SELECT 1 AS n;`,
			want: "duplicate relation name: public.x (index and view)",
		},
		{
			name: "index name reused on another table",
			sql: `CREATE TABLE t1 (id integer);
CREATE TABLE t2 (id integer);
CREATE INDEX x ON t1 (id);
CREATE INDEX x ON t2 (id);`,
			want: "duplicate index name: public.x",
		},
		{
			name: "table and domain",
			sql: `CREATE TABLE x (id integer);
CREATE DOMAIN x AS integer;`,
			want: "duplicate type name: public.x (table and domain)",
		},
		{
			name: "table and enum",
			sql: `CREATE TABLE x (id integer);
CREATE TYPE x AS ENUM ('a');`,
			want: "duplicate type name: public.x (table and enum)",
		},
		{
			name: "view and enum",
			sql: `CREATE VIEW x AS SELECT 1 AS n;
CREATE TYPE x AS ENUM ('a');`,
			want: "duplicate type name: public.x (view and enum)",
		},
		{
			name: "materialized view and domain",
			sql: `CREATE MATERIALIZED VIEW x AS SELECT 1 AS n;
CREATE DOMAIN x AS integer;`,
			want: "duplicate type name: public.x (materialized view and domain)",
		},
		{
			name: "composite type and domain",
			sql: `CREATE TYPE x AS (n integer);
CREATE DOMAIN x AS integer;`,
			want: "duplicate type name: public.x (composite type and domain)",
		},
		{
			name: "composite type and enum",
			sql: `CREATE TYPE x AS (n integer);
CREATE TYPE x AS ENUM ('a');`,
			want: "duplicate type name: public.x (composite type and enum)",
		},
		{
			name: "domain and enum",
			sql: `CREATE DOMAIN x AS integer;
CREATE TYPE x AS ENUM ('a');`,
			want: "duplicate type name: public.x (domain and enum)",
		},
		{
			name: "collision outside the default schema",
			sql: `CREATE TABLE s1.x (id integer);
CREATE VIEW s1.x AS SELECT 1 AS n;`,
			want: "duplicate relation name: s1.x (table and view)",
		},
		{
			name: "ignored table and view",
			sql: `-- pista:ignore
CREATE TABLE x (id integer);
CREATE VIEW x AS SELECT 1 AS n;`,
			want: "duplicate relation name: public.x (table and view)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSQLWithSchema(tt.sql, "public")
			require.Error(t, err)
			assert.EqualError(t, err, tt.want)
		})
	}
}

// Kinds that share no namespace keep working, so the check does not reject
// schemas PostgreSQL accepts.
func TestValidateNamespaces_NoCollision(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "sequence and domain",
			sql: `CREATE SEQUENCE x;
CREATE DOMAIN x AS integer;`,
		},
		{
			name: "index and domain",
			sql: `CREATE TABLE t (id integer);
CREATE INDEX x ON t (id);
CREATE DOMAIN x AS integer;`,
		},
		{
			name: "check constraint and view",
			sql: `CREATE TABLE t (id integer, CONSTRAINT x CHECK (id > 0));
CREATE VIEW x AS SELECT 1 AS n;`,
		},
		{
			name: "foreign key constraint and view",
			sql: `CREATE TABLE p (id integer, CONSTRAINT p_pkey PRIMARY KEY (id));
CREATE TABLE t (id integer, CONSTRAINT x FOREIGN KEY (id) REFERENCES p (id));
CREATE VIEW x AS SELECT 1 AS n;`,
		},
		{
			name: "same name in different schemas",
			sql: `CREATE TABLE s1.x (id integer);
CREATE VIEW s2.x AS SELECT 1 AS n;`,
		},
		{
			name: "names differing only in case",
			sql: `CREATE TABLE "X" (id integer);
CREATE VIEW x AS SELECT 1 AS n;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSQLWithSchema(tt.sql, "public")
			require.NoError(t, err)
		})
	}
}

// A table occupies both namespaces, so a table / composite type clash hits
// pg_class and pg_type alike. One line per name is enough.
func TestValidateNamespaces_ReportsEachNameOnce(t *testing.T) {
	sql := `CREATE TABLE x (id integer);
CREATE TYPE x AS (n integer);`

	_, err := parseSQLWithSchema(sql, "public")
	require.Error(t, err)
	assert.Equal(t, 1, strings.Count(err.Error(), "duplicate"))
}

func TestValidateNamespaces_AggregatesAcrossNames(t *testing.T) {
	sql := `CREATE TABLE x (id integer);
CREATE VIEW x AS SELECT 1 AS n;
CREATE TABLE y (id integer);
CREATE DOMAIN y AS integer;`

	_, err := parseSQLWithSchema(sql, "public")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate relation name: public.x (table and view)")
	assert.Contains(t, err.Error(), "duplicate type name: public.y (table and domain)")
}
