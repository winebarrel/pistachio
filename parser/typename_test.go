package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/parser"
)

// The normalization reaches the diff through every column, so an alias or a
// modifier it renders differently from format_type drifts on every plan. The
// cases below fix both the shape it changes and the shapes it leaves alone.
func TestNormalizeTypeName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain type", "integer", "integer"},
		{"alias", "varchar(255)", "character varying(255)"},
		{"alias without modifier", "float", "double precision"},
		{"alias with time zone", "timestamptz", "timestamp with time zone"},
		{"modifier spacing", "numeric(10, 2)", "numeric(10,2)"},

		// PostgreSQL takes a numeric with only a precision as a scale of zero
		// and format_type prints both digits.
		{"numeric precision only", "numeric(5)", "numeric(5,0)"},
		{"numeric alias precision only", "decimal(7)", "numeric(7,0)"},
		{"numeric with scale", "numeric(10,2)", "numeric(10,2)"},
		{"numeric without modifier", "numeric", "numeric"},
		{"numeric array precision only", "numeric(5)[]", "numeric(5,0)[]"},
		{"numeric array with scale", "numeric(10,2)[]", "numeric(10,2)[]"},

		// A type of another schema keeps its own modifier, whatever it is
		// named.
		{"qualified numeric", "myschema.numeric(5)", "myschema.numeric(5)"},

		// PostgreSQL accepts these and enforces neither, so they are left as
		// written; see LIMITATIONS.
		{"array", "text[]", "text[]"},
		{"multidimensional array", "integer[][]", "integer[][]"},
		{"bounded array", "integer[3]", "integer[3]"},

		// The modifier is not a plain precision, so there is no scale to fill.
		{"mixed case modifier", "geometry(Polygon,4326)", "geometry(Polygon,4326)"},
		{"interval field", "interval(6)", "interval(6)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parser.NormalizeTypeName(tt.in))
		})
	}
}

func TestSplitTypeSuffix(t *testing.T) {
	tests := []struct {
		name             string
		suffix           string
		wantMod, wantArr string
	}{
		{"empty", "", "", ""},
		{"modifier only", "(10,2)", "(10,2)", ""},
		{"array only", "[]", "", "[]"},
		{"modifier and array", "(10,2)[]", "(10,2)", "[]"},
		{"bounded array", "[3]", "", "[3]"},
		// Deparse output always closes its parenthesis, so this only guards
		// against losing the text.
		{"unterminated modifier", "(10", "(10", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mod, array := parser.SplitTypeSuffix(tt.suffix)
			assert.Equal(t, tt.wantMod, mod)
			assert.Equal(t, tt.wantArr, array)
			assert.Equal(t, tt.suffix, mod+array, "the two halves rejoin to the input")
		})
	}
}

func TestFillNumericScale(t *testing.T) {
	assert.Equal(t, "(5,0)", parser.FillNumericScale("(5)"))
	assert.Equal(t, "(10,2)", parser.FillNumericScale("(10,2)"))
	assert.Empty(t, parser.FillNumericScale(""))
	assert.Equal(t, "()", parser.FillNumericScale("()"))
	// Not a plain precision, so it is left alone rather than guessed at.
	assert.Equal(t, "(Polygon)", parser.FillNumericScale("(Polygon)"))
	assert.Equal(t, "(5 )", parser.FillNumericScale("(5 )"))
}
