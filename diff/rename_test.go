package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func TestCollectColumnRenames(t *testing.T) {
	cols := orderedmap.New[string, *model.Column]()
	cols.Set("display_name", &model.Column{Name: "display_name", RenameFrom: ptr("name")})
	cols.Set("id", &model.Column{Name: "id"})
	cols.Set("self", &model.Column{Name: "self", RenameFrom: ptr("self")}) // no-op same-name rename

	renames := collectColumnRenames(cols)
	assert.Equal(t, map[string]string{"name": "display_name"}, renames)
}

func TestRewriteColumnInIndexDef_PlainColumn(t *testing.T) {
	got, err := rewriteColumnInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (name)",
		"name", "display_name",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "(display_name)")
	assert.NotContains(t, got, "(name)")
}

func TestRewriteColumnInIndexDef_LeavesOtherColumns(t *testing.T) {
	got, err := rewriteColumnInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (a, b)",
		"a", "x",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "(x, b)")
}

func TestRewriteColumnInIndexDef_ExpressionIndex(t *testing.T) {
	got, err := rewriteColumnInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (lower(email))",
		"email", "email_addr",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "lower(email_addr)")
}

func TestRewriteColumnInIndexDef_PartialWhere(t *testing.T) {
	got, err := rewriteColumnInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (id) WHERE deleted_at IS NULL",
		"deleted_at", "removed_at",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "removed_at IS NULL")
}

func TestRewriteColumnInIndexDef_IncludeClause(t *testing.T) {
	got, err := rewriteColumnInIndexDef(
		"CREATE INDEX idx ON public.t USING btree (sku) INCLUDE (name)",
		"name", "product_name",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "INCLUDE (product_name)")
}

func TestRewriteColumnInIndexDef_ParseError(t *testing.T) {
	_, err := rewriteColumnInIndexDef("not a valid sql", "a", "b")
	require.Error(t, err)
}

func TestRewriteColumnInConstraintDef_Unique(t *testing.T) {
	got, err := rewriteColumnInConstraintDef("UNIQUE (email)", "email", "email_addr")
	require.NoError(t, err)
	assert.Contains(t, got, "(email_addr)")
}

func TestRewriteColumnInConstraintDef_PrimaryKey(t *testing.T) {
	got, err := rewriteColumnInConstraintDef("PRIMARY KEY (id)", "id", "user_id")
	require.NoError(t, err)
	assert.Contains(t, got, "(user_id)")
}

func TestRewriteColumnInConstraintDef_Check(t *testing.T) {
	got, err := rewriteColumnInConstraintDef("CHECK ((qty > 0))", "qty", "quantity")
	require.NoError(t, err)
	assert.Contains(t, got, "quantity > 0")
}

func TestRewriteColumnInConstraintDef_CheckBoolExpr(t *testing.T) {
	got, err := rewriteColumnInConstraintDef(
		"CHECK ((qty > 0 AND qty < 1000))",
		"qty", "quantity",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "quantity > 0")
	assert.Contains(t, got, "quantity < 1000")
}

func TestRewriteColumnInConstraintDef_CheckNullTest(t *testing.T) {
	got, err := rewriteColumnInConstraintDef("CHECK ((deleted_at IS NULL))", "deleted_at", "removed_at")
	require.NoError(t, err)
	assert.Contains(t, got, "removed_at IS NULL")
}

func TestRewriteColumnInConstraintDef_FKLocalAttrs(t *testing.T) {
	got, err := rewriteColumnInConstraintDef(
		"FOREIGN KEY (user_id) REFERENCES users(id)",
		"user_id", "buyer_id",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "(buyer_id)")
	// PkAttrs (referenced columns) must NOT be rewritten
	assert.Contains(t, got, "users (id)")
}

func TestRewriteColumnInConstraintDef_FKReferencedNotRewritten(t *testing.T) {
	got, err := rewriteColumnInConstraintDef(
		"FOREIGN KEY (user_id) REFERENCES users(id)",
		"id", "user_pk",
	)
	require.NoError(t, err)
	// PkAttrs side: "id" should stay as-is even though it matches oldName
	assert.Contains(t, got, "users (id)")
}

func TestRewriteColumnInConstraintDef_Exclusion(t *testing.T) {
	got, err := rewriteColumnInConstraintDef(
		"EXCLUDE USING gist (room WITH =, during WITH &&)",
		"during", "time_range",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "time_range WITH")
}

func TestRewriteColumnInConstraintDef_UniqueInclude(t *testing.T) {
	got, err := rewriteColumnInConstraintDef(
		"UNIQUE (email) INCLUDE (name)",
		"name", "display_name",
	)
	require.NoError(t, err)
	assert.Contains(t, got, "INCLUDE (display_name)")
}

func TestRewriteColumnInConstraintDef_ParseError(t *testing.T) {
	_, err := rewriteColumnInConstraintDef("not a valid def", "a", "b")
	require.Error(t, err)
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

	out := rewriteColumnRefsInIndexes(in, map[string]string{"name": "display_name"})
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

	out := rewriteColumnRefsInIndexes(in, map[string]string{"name": "display_name"})
	got, ok := out.GetOk("idx")
	require.True(t, ok)
	// Unparseable definitions are kept intact so downstream comparison still works.
	assert.Equal(t, "not a valid CREATE INDEX", got.Definition)
}

func TestRewriteColumnRefsInConstraints_RewritesAndClones(t *testing.T) {
	in := orderedmap.New[string, *model.Constraint]()
	original := &model.Constraint{Name: "u", Definition: "UNIQUE (email)"}
	in.Set("u", original)

	out := rewriteColumnRefsInConstraints(in, map[string]string{"email": "email_addr"})
	got, ok := out.GetOk("u")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "(email_addr)")
	assert.Contains(t, original.Definition, "(email)")
}

func TestRewriteColumnRefsInForeignKeys_RewritesLocalAttrs(t *testing.T) {
	in := orderedmap.New[string, *model.ForeignKey]()
	in.Set("fk", &model.ForeignKey{
		Constraint: model.Constraint{
			Name:       "fk",
			Definition: "FOREIGN KEY (user_id) REFERENCES users(id)",
		},
	})

	out := rewriteColumnRefsInForeignKeys(in, map[string]string{"user_id": "buyer_id"})
	got, ok := out.GetOk("fk")
	require.True(t, ok)
	assert.Contains(t, got.Definition, "(buyer_id)")
	assert.Contains(t, got.Definition, "users (id)")
}
