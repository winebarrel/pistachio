package pgast_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/internal/pgast"
)

// Every "want" below is what a PostgreSQL 17 server printed for
// SELECT '<src>'::jsonpath::text. 15, 16 and 18 print the same.
func TestCanonicalJSONPath(t *testing.T) {
	for _, tt := range []struct{ src, want string }{
		{"$", "$"},
		{"$.x", `$."x"`},
		{`$."x"`, `$."x"`},
		{"$.x.y[0]", `$."x"."y"[0]`},
		{"$.a.b.c", `$."a"."b"."c"`},
		{"$.a[*].b", `$."a"[*]."b"`},
		{"$.x[*][0]", `$."x"[*][0]`},
		{"$.*", "$.*"},
		{"$[*]", "$[*]"},
		{"$[0]", "$[0]"},
		{"$[0 to 2]", "$[0 to 2]"},
		{"$[1,2]", "$[1,2]"},
		{"$.x[last]", `$."x"[last]`},
		{"$.**", "$.**"},
		{"$.**{2}", "$.**{2}"},
		{"lax $.x", `$."x"`},
		{"strict $.x", `strict $."x"`},
		{"strict $[*]", "strict $[*]"},
		{"$x", `$"x"`},
		{"$.x.size()", `$."x".size()`},
		{"$.x.type()", `$."x".type()`},
		{"$.x.keyvalue()", `$."x".keyvalue()`},
		{`$."a b"`, `$."a b"`},
		{`$."1"`, `$."1"`},
		// Whitespace the server does not keep.
		{"  $.x  ", `$."x"`},
		{"$[0  to  2]", "$[0 to 2]"},
	} {
		got, ok := pgast.CanonicalJSONPath(tt.src)
		assert.True(t, ok, tt.src)
		assert.Equal(t, tt.want, got, tt.src)
	}
}

// A path the canonicaliser does not fully recognise comes back untouched, so
// the caller compares the two sides exactly as they were written. The server
// re-spaces and parenthesises these, which needs an expression parser rather
// than a lexer; recorded in TODO.md.
func TestCanonicalJSONPath_NotRecognised(t *testing.T) {
	for _, src := range []string{
		"$.x ? (@ > 1)",
		"$.a?(@>1).b",
		"$.x + 1",
		`$.x like_regex "a"`,
		`$.x.datetime("HH24")`,
		"$[$n]",
		"$[1 + 1]",
		"$[1 to 2 to 3]",
		"$[ to 2]",
		"$[]",
		"$.**{2",
		`$."a\"b"`,
		"$.x[",
		"x",
		"",
		// A keyword only counts whole: strictly is a name, not a mode.
		"strictly $.x",
	} {
		got, ok := pgast.CanonicalJSONPath(src)
		assert.False(t, ok, src)
		assert.Equal(t, src, got, src)
	}
}

// laxity is not the lax keyword, and $.strictly is not a mode prefix.
func TestCanonicalJSONPath_ModeIsAWholeWord(t *testing.T) {
	got, ok := pgast.CanonicalJSONPath("$.laxative")
	assert.True(t, ok)
	assert.Equal(t, `$."laxative"`, got)
}
