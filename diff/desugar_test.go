package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The catalog side of every case below is what pg_get_constraintdef returned
// for the desired side loaded into PostgreSQL 16, so the pairs are the two
// spellings of one stored constraint rather than two forms believed equal.

func TestEqualConstraintDef_between(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK (((level >= 1) AND (level <= 10)))",
		"CHECK (level BETWEEN 1 AND 10)",
	))
	assert.True(t, equalConstraintDef(
		"CHECK (((level < 1) OR (level > 10)))",
		"CHECK (level NOT BETWEEN 1 AND 10)",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((((level >= 1) AND (level <= 10)) OR ((level >= 10) AND (level <= 1))))",
		"CHECK (level BETWEEN SYMMETRIC 1 AND 10)",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((((level < 1) OR (level > 10)) AND ((level < 10) OR (level > 1))))",
		"CHECK (level NOT BETWEEN SYMMETRIC 1 AND 10)",
	))
}

func TestEqualConstraintDef_betweenOverExpression(t *testing.T) {
	// The operand is copied into each comparison, so one that is a tree
	// rather than a column reference has to survive being cloned. The
	// function call also has its schema stripped before the copies are made.
	assert.True(t, equalConstraintDef(
		"CHECK (((char_length(code) >= 43) AND (char_length(code) <= 128)))",
		"CHECK (pg_catalog.char_length(code) BETWEEN 43 AND 128)",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((((level + 1) >= 1) AND ((level + 1) <= 10)))",
		"CHECK (level + 1 BETWEEN 1 AND 10)",
	))
}

func TestEqualConstraintDef_betweenInConjunction(t *testing.T) {
	// The grammar flattens `a AND b AND c` as it parses, so the catalog
	// definition re-parses flat while the expansion leaves a nested BoolExpr
	// behind. Both operand positions: the grammar flattens on the left alone.
	assert.True(t, equalConstraintDef(
		"CHECK ((((level >= 1) AND (level <= 10)) AND active))",
		"CHECK ((level BETWEEN 1 AND 10) AND active)",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((active AND ((level >= 1) AND (level <= 10))))",
		"CHECK (active AND (level BETWEEN 1 AND 10))",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((((level >= 1) AND (level <= 10)) AND ((other >= 1) AND (other <= 10)) AND active))",
		"CHECK ((level BETWEEN 1 AND 10) AND (other BETWEEN 1 AND 10) AND active)",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((((level < 1) OR (level > 10)) OR active))",
		"CHECK ((level NOT BETWEEN 1 AND 10) OR active)",
	))
}

func TestEqualConstraintDef_betweenBoundsNotFolded(t *testing.T) {
	// A different bound, and a negation, are different constraints.
	assert.False(t, equalConstraintDef(
		"CHECK (((level >= 1) AND (level <= 10)))",
		"CHECK (level BETWEEN 1 AND 20)",
	))
	assert.False(t, equalConstraintDef(
		"CHECK (((level >= 1) AND (level <= 10)))",
		"CHECK (level NOT BETWEEN 1 AND 10)",
	))
	// SYMMETRIC widens the predicate rather than restating it.
	assert.False(t, equalConstraintDef(
		"CHECK (((level >= 1) AND (level <= 10)))",
		"CHECK (level BETWEEN SYMMETRIC 1 AND 10)",
	))
}

func TestEqualConstraintDef_patternMatch(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier ~~ 'own%'::text))",
		"CHECK (carrier LIKE 'own%')",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier ~~* 'own%'::text))",
		"CHECK (carrier ILIKE 'own%')",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier !~~ 'sub%'::text))",
		"CHECK (carrier NOT LIKE 'sub%')",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier !~~* 'sub%'::text))",
		"CHECK (carrier NOT ILIKE 'sub%')",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier ~ similar_to_escape('own%'::text)))",
		"CHECK (carrier SIMILAR TO 'own%')",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier !~ similar_to_escape('own%'::text)))",
		"CHECK (carrier NOT SIMILAR TO 'own%')",
	))
}

func TestEqualConstraintDef_patternMatchEscape(t *testing.T) {
	// An ESCAPE clause is already a call in the right operand; the catalog
	// prints it unqualified because pg_catalog is on the search_path.
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier ~~ like_escape('own!%'::text, '!'::text)))",
		"CHECK (carrier LIKE 'own!%' ESCAPE '!')",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier ~ similar_to_escape('own%'::text, '!'::text)))",
		"CHECK (carrier SIMILAR TO 'own%' ESCAPE '!')",
	))
}

func TestEqualConstraintDef_patternMatchOperatorsNotFolded(t *testing.T) {
	// Case sensitivity and negation tell these operators apart.
	assert.False(t, equalConstraintDef(
		"CHECK ((carrier ~~ 'own%'::text))",
		"CHECK (carrier ILIKE 'own%')",
	))
	assert.False(t, equalConstraintDef(
		"CHECK ((carrier ~~ 'own%'::text))",
		"CHECK (carrier NOT LIKE 'own%')",
	))
	assert.False(t, equalConstraintDef(
		"CHECK ((carrier ~ similar_to_escape('own%'::text)))",
		"CHECK (carrier LIKE 'own%')",
	))
}

func TestEqualConstraintDef_notIn(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier <> ALL (ARRAY['a'::text, 'b'::text])))",
		"CHECK (carrier NOT IN ('a', 'b'))",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((carrier = ANY (ARRAY['a'::text, 'b'::text])))",
		"CHECK (carrier IN ('a', 'b'))",
	))
	// The two are each other's negation.
	assert.False(t, equalConstraintDef(
		"CHECK ((carrier <> ALL (ARRAY['a'::text, 'b'::text])))",
		"CHECK (carrier IN ('a', 'b'))",
	))
}

func TestEqualConstraintDef_scalarArrayOperatorMatters(t *testing.T) {
	// Only `= ANY` and `<> ALL` are the shapes an IN list is stored as. Any
	// other operator reaches the catalog as written, and folding one would
	// make the two below compare equal.
	assert.False(t, equalConstraintDef(
		"CHECK ((level > ANY (ARRAY[1, 2])))",
		"CHECK (level > ALL (ARRAY[1, 2]))",
	))
	assert.True(t, equalConstraintDef(
		"CHECK ((level > ANY (ARRAY[1, 2])))",
		"CHECK (level > ANY(ARRAY[1, 2]))",
	))
	// A qualified operator name has no rule, so it is left as written and
	// only compares equal to itself.
	assert.True(t, equalConstraintDef(
		"CHECK ((level OPERATOR(pg_catalog.=) ANY (ARRAY[1, 2])))",
		"CHECK (level OPERATOR(pg_catalog.=) ANY(ARRAY[1, 2]))",
	))
}

func TestEqualConstraintDef_rowConstructor(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((ROW(lo, hi) > ROW(0, 0)))",
		"CHECK ((lo, hi) > (0, 0))",
	))
	assert.False(t, equalConstraintDef(
		"CHECK ((ROW(lo, hi) > ROW(0, 0)))",
		"CHECK ((lo, hi) >= (0, 0))",
	))
}

func TestEqualConstraintDef_anyOverArrayColumn(t *testing.T) {
	// The fold rewrites `= ANY (ARRAY[...])` into the IN list it stands for.
	// `= ANY` over an array-valued expression is not an IN list and has
	// nothing to fold to, so it is compared as written.
	assert.True(t, equalConstraintDef(
		"CHECK ((level = ANY (levels)))",
		"CHECK (level = ANY(levels))",
	))
	assert.False(t, equalConstraintDef(
		"CHECK ((level = ANY (levels)))",
		"CHECK (level = ANY(others))",
	))
}
