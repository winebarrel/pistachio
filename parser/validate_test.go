package parser

import (
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
	require.NoError(t, ValidateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_IndexMissingColumn(t *testing.T) {
	tbl := newValidatableTable("users", "id", "display_name")
	tbl.Indexes.Set("idx_users_name", &model.Index{
		Name:       "idx_users_name",
		Definition: "CREATE INDEX idx_users_name ON public.users (name)",
	})
	err := ValidateColumnRefs(tablesMap(tbl))
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
	err := ValidateColumnRefs(tablesMap(tbl))
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
	err := ValidateColumnRefs(tablesMap(tbl))
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
	require.NoError(t, ValidateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_AggregatesMultiple(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Indexes.Set("idx_a", &model.Index{Name: "idx_a", Definition: "CREATE INDEX idx_a ON public.t (a)"})
	tbl.Constraints.Set("c_b", &model.Constraint{Name: "c_b", Type: model.ConstraintType('c'), Definition: "CHECK ((b > 0))"})
	err := ValidateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "column a referenced in index idx_a")
	assert.Contains(t, msg, "column b referenced in CHECK constraint c_b")
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
	require.NoError(t, ValidateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_ExpressionIndexValid(t *testing.T) {
	tbl := newValidatableTable("users", "id", "email")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: "CREATE INDEX idx ON public.users (lower(email))",
	})
	require.NoError(t, ValidateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_PartialIndexWhereChecked(t *testing.T) {
	tbl := newValidatableTable("users", "id", "active")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: "CREATE INDEX idx ON public.users (id) WHERE deleted_at IS NULL",
	})
	err := ValidateColumnRefs(tablesMap(tbl))
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
	err := ValidateColumnRefs(tablesMap(tbl))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column during referenced in EXCLUDE constraint no_overlap")
}

func TestValidateColumnRefs_QuotedIdentifier(t *testing.T) {
	tbl := newValidatableTable("t", "id", "MyName")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: `CREATE INDEX idx ON public.t ("MyName")`,
	})
	require.NoError(t, ValidateColumnRefs(tablesMap(tbl)))
}

func TestValidateColumnRefs_CheckBoolExprChecked(t *testing.T) {
	tbl := newValidatableTable("t", "id")
	tbl.Constraints.Set("c", &model.Constraint{
		Name:       "c",
		Type:       model.ConstraintType('c'),
		Definition: "CHECK ((a > 0 AND b < 100))",
	})
	err := ValidateColumnRefs(tablesMap(tbl))
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
	err := ValidateColumnRefs(tablesMap(tbl))
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
	err := ValidateColumnRefs(tablesMap(tbl))
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
	err := ValidateColumnRefs(tablesMap(tbl))
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
	err := ValidateColumnRefs(tablesMap(tbl))
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
	err := ValidateColumnRefs(tablesMap(tbl))
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

	err := ValidateColumnRefs(tablesMap(good, bad))
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "table public.bad")
	assert.NotContains(t, msg, "table public.good")
}

func TestValidateColumnRefs_CompositeKeyPartialMiss(t *testing.T) {
	// In INDEX (a, b) with only b in the desired column set, only "a" must
	// be reported — not "b".
	tbl := newValidatableTable("t", "id", "b")
	tbl.Indexes.Set("idx", &model.Index{
		Name:       "idx",
		Definition: "CREATE INDEX idx ON public.t (a, b)",
	})
	err := ValidateColumnRefs(tablesMap(tbl))
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

	_, err := ParseSQLWithSchema(sql, "public")
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

	_, err := ParseSQLWithSchema(sql, "public")
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
