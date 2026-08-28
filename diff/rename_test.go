package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func one(old, n string) map[string]string {
	return map[string]string{old: n}
}

func TestCollectColumnRenames(t *testing.T) {
	cols := orderedmap.New[string, *model.Column]()
	cols.Set("display_name", &model.Column{Name: "display_name", RenameFrom: new("name")})
	cols.Set("id", &model.Column{Name: "id"})
	cols.Set("self", &model.Column{Name: "self", RenameFrom: new("self")}) // no-op same-name rename

	renames := collectColumnRenames(cols)
	assert.Equal(t, map[string]string{"name": "display_name"}, renames)
}

func TestRewriteColumnsInIndexDef_PlainColumn(t *testing.T) {
	got, err := rewriteColumnsInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (name)",
		one("name", "display_name"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "(display_name)")
	assert.NotContains(t, got, "(name)")
}

func TestRewriteColumnsInIndexDef_LeavesOtherColumns(t *testing.T) {
	got, err := rewriteColumnsInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (a, b)",
		one("a", "x"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "(x, b)")
}

func TestRewriteColumnsInIndexDef_ExpressionIndex(t *testing.T) {
	got, err := rewriteColumnsInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (lower(email))",
		one("email", "email_addr"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "lower(email_addr)")
}

func TestRewriteColumnsInIndexDef_PartialWhere(t *testing.T) {
	got, err := rewriteColumnsInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (id) WHERE deleted_at IS NULL",
		one("deleted_at", "removed_at"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "removed_at IS NULL")
}

func TestRewriteColumnsInIndexDef_IncludeClause(t *testing.T) {
	got, err := rewriteColumnsInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (sku) INCLUDE (name)",
		one("name", "product_name"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "INCLUDE (product_name)")
}

func TestRewriteColumnsInIndexDef_NoCascadeOnChain(t *testing.T) {
	// Renames a->b alongside b->c must rewrite the index's reference to a (the
	// original) into b; NOT cascade through b->c into c.
	got, err := rewriteColumnsInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (a, b)",
		map[string]string{"a": "b", "b": "c"},
	)
	require.NoError(t, err)
	assert.Contains(t, got, "(b, c)")
	assert.NotContains(t, got, "(c, c)")
}

func TestRewriteColumnsInIndexDef_ParseError(t *testing.T) {
	_, err := rewriteColumnsInIndexDef("not a valid sql", one("a", "b"))
	require.Error(t, err)
}

func TestRewriteColumnsInIndexDef_EmptyInput(t *testing.T) {
	_, err := rewriteColumnsInIndexDef("", one("a", "b"))
	require.Error(t, err)
}

func TestRewriteColumnsInIndexDef_NotIndexStmt(t *testing.T) {
	_, err := rewriteColumnsInIndexDef("SELECT 1", one("a", "b"))
	require.Error(t, err)
}

func TestRewriteColumnsInConstraintDef_Unique(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef("UNIQUE (email)", one("email", "email_addr"))
	require.NoError(t, err)
	assert.Contains(t, got, "(email_addr)")
}

func TestRewriteColumnsInConstraintDef_PrimaryKey(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef("PRIMARY KEY (id)", one("id", "user_id"))
	require.NoError(t, err)
	assert.Contains(t, got, "(user_id)")
}

func TestRewriteColumnsInConstraintDef_Check(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef("CHECK ((qty > 0))", one("qty", "quantity"))
	require.NoError(t, err)
	assert.Contains(t, got, "quantity > 0")
}

func TestRewriteColumnsInConstraintDef_CheckBoolExpr(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef(
		"CHECK ((qty > 0 AND qty < 1000))",
		one("qty", "quantity"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "quantity > 0")
	assert.Contains(t, got, "quantity < 1000")
}

func TestRewriteColumnsInConstraintDef_CheckNullTest(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef("CHECK ((deleted_at IS NULL))", one("deleted_at", "removed_at"))
	require.NoError(t, err)
	assert.Contains(t, got, "removed_at IS NULL")
}

func TestRewriteColumnsInConstraintDef_FKLocalAttrs(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef(
		"FOREIGN KEY (user_id) REFERENCES users(id)",
		one("user_id", "buyer_id"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "(buyer_id)")
	// PkAttrs (referenced columns) must NOT be rewritten
	assert.Contains(t, got, "users (id)")
}

func TestRewriteColumnsInConstraintDef_FKReferencedNotRewritten(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef(
		"FOREIGN KEY (user_id) REFERENCES users(id)",
		one("id", "user_pk"),
	)
	require.NoError(t, err)
	// PkAttrs side: "id" should stay as-is even though it matches oldName
	assert.Contains(t, got, "users (id)")
}

func TestRewriteColumnsInConstraintDef_Exclusion(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef(
		"EXCLUDE USING gist (room WITH =, during WITH &&)",
		one("during", "time_range"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "time_range WITH")
}

func TestRewriteColumnsInConstraintDef_UniqueInclude(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef(
		"UNIQUE (email) INCLUDE (name)",
		one("name", "display_name"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "INCLUDE (display_name)")
}

func TestRewriteColumnsInConstraintDef_ParseError(t *testing.T) {
	_, err := rewriteColumnsInConstraintDef("not a valid def", one("a", "b"))
	require.Error(t, err)
}

func TestRewriteColumnsInConstraintDef_EmptyInput(t *testing.T) {
	_, err := rewriteColumnsInConstraintDef("", one("a", "b"))
	require.Error(t, err)
}

func TestRewriteColumnsInConstraintDef_CheckTypeCast(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef("CHECK ((col::text = 'x'))", one("col", "value"))
	require.NoError(t, err)
	assert.Contains(t, got, "value")
	assert.NotContains(t, got, "(col")
}

func TestRewriteColumnsInConstraintDef_CheckCoalesce(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef("CHECK ((COALESCE(col, 0) > 0))", one("col", "value"))
	require.NoError(t, err)
	assert.Contains(t, got, "COALESCE(value")
}

func TestRewriteColumnsInConstraintDef_CheckCase(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef(
		"CHECK (((CASE WHEN col > 0 THEN 1 ELSE 0 END) = 1))",
		one("col", "value"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "value")
}

func TestRewriteColumnsInConstraintDef_CheckAnyArray(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef(
		"CHECK ((status = ANY (ARRAY['a'::text, 'b'::text])))",
		one("status", "state"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "state")
}

func TestRewriteColumnsInConstraintDef_CheckInList(t *testing.T) {
	got, err := rewriteColumnsInConstraintDef(
		"CHECK ((status IN ('a', 'b')))",
		one("status", "state"),
	)
	require.NoError(t, err)
	assert.Contains(t, got, "state")
}

func TestRewriteColumnsInConstraintDef_NoCascadeOnChain(t *testing.T) {
	// Renames a->b and b->c must not cascade.
	got, err := rewriteColumnsInConstraintDef(
		"UNIQUE (a, b)",
		map[string]string{"a": "b", "b": "c"},
	)
	require.NoError(t, err)
	assert.Contains(t, got, "(b, c)")
	assert.NotContains(t, got, "(c, c)")
}

func TestRewriteColumnRefsInIndexes_RewritesAndClones(t *testing.T) {
	in := orderedmap.New[string, *model.Index]()
	original := &model.Index{
		Schema:     "public",
		Name:       "idx",
		Table:      "t",
		Definition: "CREATE INDEX idx ON public.t USING btree (name)",
	}
	in.Set("idx", original)

	out := rewriteColumnRefsInIndexes(in, one("name", "display_name"))
	got, ok := out.GetOk("idx")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "(display_name)")
	// The original input must not be mutated.
	assert.Contains(t, original.Definition, "(name)")
}

func TestRewriteColumnRefsInIndexes_FallbackOnParseError(t *testing.T) {
	in := orderedmap.New[string, *model.Index]()
	in.Set("idx", &model.Index{
		Schema:     "public",
		Name:       "idx",
		Table:      "t",
		Definition: "not a valid CREATE INDEX",
	})

	out := rewriteColumnRefsInIndexes(in, one("name", "display_name"))
	got, ok := out.GetOk("idx")
	require.True(t, ok)
	// Unparseable definitions are kept intact so downstream comparison still works.
	assert.Equal(t, "not a valid CREATE INDEX", got.Definition)
}

func TestRewriteColumnRefsInConstraints_RewritesAndClones(t *testing.T) {
	in := orderedmap.New[string, *model.Constraint]()
	original := &model.Constraint{Name: "u", Definition: "UNIQUE (email)"}
	in.Set("u", original)

	out := rewriteColumnRefsInConstraints(in, one("email", "email_addr"))
	got, ok := out.GetOk("u")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "(email_addr)")
	assert.Contains(t, original.Definition, "(email)")
}

func TestRenameColumnKeys_RemapsAndPreservesOrder(t *testing.T) {
	in := orderedmap.New[string, *model.Column]()
	in.Set("a", &model.Column{Name: "a"})
	in.Set("b", &model.Column{Name: "b"})
	in.Set("c", &model.Column{Name: "c"})

	out := renameColumnKeys(in, map[string]string{"b": "d"})
	keys := []string{}
	for k := range out.All() {
		keys = append(keys, k)
	}
	assert.Equal(t, []string{"a", "d", "c"}, keys)
}

func TestRenameColumnKeys_NoCascadeOnChain(t *testing.T) {
	// a->b alongside b->c must not cascade through a single column to c.
	in := orderedmap.New[string, *model.Column]()
	in.Set("a", &model.Column{Name: "a"})
	in.Set("b", &model.Column{Name: "b"})

	out := renameColumnKeys(in, map[string]string{"a": "b", "b": "c"})
	keys := []string{}
	for k := range out.All() {
		keys = append(keys, k)
	}
	assert.Equal(t, []string{"b", "c"}, keys)
}

func TestRewriteColumnRefsInForeignKeys_RewritesLocalAttrs(t *testing.T) {
	in := orderedmap.New[string, *model.ForeignKey]()
	in.Set("fk", &model.ForeignKey{
		Constraint: model.Constraint{
			Name:       "fk",
			Definition: "FOREIGN KEY (user_id) REFERENCES users(id)",
		},
	})

	out := rewriteColumnRefsInForeignKeys(in, one("user_id", "buyer_id"))
	got, ok := out.GetOk("fk")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "(buyer_id)")
	assert.Contains(t, got.Definition, "users (id)")
}

func TestRewriteColumnRefsInTriggers_RewritesUpdateOfAndWhen(t *testing.T) {
	in := orderedmap.New[string, *model.Trigger]()
	original := &model.Trigger{
		Schema: "public",
		Table:  "products",
		Name:   "products_audit",
		Definition: "CREATE TRIGGER products_audit AFTER UPDATE OF qty ON public.products " +
			"FOR EACH ROW WHEN (new.qty IS DISTINCT FROM old.qty) EXECUTE FUNCTION public.stamp()",
	}
	in.Set("products_audit", original)

	out := rewriteColumnRefsInTriggers(in, one("qty", "quantity"))
	got, ok := out.GetOk("products_audit")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "UPDATE OF quantity")
	assert.Contains(t, got.Definition, "new.quantity IS DISTINCT FROM old.quantity")
	assert.NotContains(t, got.Definition, "qty")
	// The original input must not be mutated.
	assert.Contains(t, original.Definition, "UPDATE OF qty")
}

func TestRewriteColumnRefsInTriggers_LeavesFunctionArgsAlone(t *testing.T) {
	// PostgreSQL does not rewrite the arguments passed to the trigger function
	// on RENAME COLUMN, so neither does the rewriter.
	in := orderedmap.New[string, *model.Trigger]()
	in.Set("t", &model.Trigger{
		Definition: "CREATE TRIGGER t AFTER UPDATE ON public.products " +
			"FOR EACH ROW EXECUTE FUNCTION public.stamp('qty')",
	})

	out := rewriteColumnRefsInTriggers(in, one("qty", "quantity"))
	got, ok := out.GetOk("t")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "'qty'")
}

func TestRewriteColumnRefsInTriggers_LeavesOtherQualifiersAlone(t *testing.T) {
	// Only NEW and OLD qualify a column in a WHEN clause. A qualifier naming
	// anything else is not a column reference this rewrite owns.
	in := orderedmap.New[string, *model.Trigger]()
	in.Set("t", &model.Trigger{
		Definition: "CREATE TRIGGER t AFTER UPDATE ON public.products " +
			"FOR EACH ROW WHEN (new.qty > other.qty) EXECUTE FUNCTION public.stamp()",
	})

	out := rewriteColumnRefsInTriggers(in, one("qty", "quantity"))
	got, ok := out.GetOk("t")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "new.quantity")
	assert.Contains(t, got.Definition, "other.qty")
}

func TestRewriteColumnRefsInTriggers_NoCascadeOnChain(t *testing.T) {
	// a->b alongside b->c must not cascade through a single reference to c.
	in := orderedmap.New[string, *model.Trigger]()
	in.Set("t", &model.Trigger{
		Definition: "CREATE TRIGGER t AFTER UPDATE OF a, b ON public.products " +
			"FOR EACH ROW EXECUTE FUNCTION public.stamp()",
	})

	out := rewriteColumnRefsInTriggers(in, map[string]string{"a": "b", "b": "c"})
	got, ok := out.GetOk("t")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "UPDATE OF b, c")
}

func TestRewriteColumnRefsInTriggers_RewritesQuotedName(t *testing.T) {
	in := orderedmap.New[string, *model.Trigger]()
	in.Set("t", &model.Trigger{
		Definition: `CREATE TRIGGER t AFTER UPDATE OF "Qty" ON public.products ` +
			`FOR EACH ROW WHEN (new."Qty" > 0) EXECUTE FUNCTION public.stamp()`,
	})

	out := rewriteColumnRefsInTriggers(in, one("Qty", "Quantity"))
	got, ok := out.GetOk("t")
	require.True(t, ok)
	assert.Contains(t, got.Definition, `UPDATE OF "Quantity"`)
	assert.Contains(t, got.Definition, `new."Quantity" > 0`)
}

func TestRewriteColumnRefsInTriggers_FallbackOnParseError(t *testing.T) {
	in := orderedmap.New[string, *model.Trigger]()
	in.Set("t", &model.Trigger{Definition: "not a valid CREATE TRIGGER"})

	out := rewriteColumnRefsInTriggers(in, one("qty", "quantity"))
	got, ok := out.GetOk("t")
	require.True(t, ok)
	// Unparseable definitions are kept intact so downstream comparison still works.
	assert.Equal(t, "not a valid CREATE TRIGGER", got.Definition)
}

func TestRewriteColumnRefsInTriggers_FallbackOnEmptyDefinition(t *testing.T) {
	in := orderedmap.New[string, *model.Trigger]()
	in.Set("t", &model.Trigger{Definition: ""})

	out := rewriteColumnRefsInTriggers(in, one("qty", "quantity"))
	got, ok := out.GetOk("t")
	require.True(t, ok)
	assert.Equal(t, "", got.Definition)
}

func TestRewriteColumnRefsInTriggers_FallbackOnOtherStatement(t *testing.T) {
	in := orderedmap.New[string, *model.Trigger]()
	in.Set("t", &model.Trigger{Definition: "SELECT qty FROM public.products"})

	out := rewriteColumnRefsInTriggers(in, one("qty", "quantity"))
	got, ok := out.GetOk("t")
	require.True(t, ok)
	// Parseable but not a CREATE TRIGGER, so nothing is rewritten.
	assert.Equal(t, "SELECT qty FROM public.products", got.Definition)
}

func TestRewriteColumnRefsInTriggers_NilPassesThrough(t *testing.T) {
	// A relation with no triggers carries a nil map, which triggerMap
	// normalizes downstream.
	assert.Nil(t, rewriteColumnRefsInTriggers(nil, one("qty", "quantity")))
}
