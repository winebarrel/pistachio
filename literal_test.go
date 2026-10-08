package pistachio

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

func literalTables(def string) *orderedmap.Map[string, *model.Table] {
	cols := orderedmap.New[string, *model.Column]()
	cols.Set("iv", &model.Column{Name: "iv", TypeName: "interval", Default: &def})
	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.t", &model.Table{
		Schema: "public", Name: "t", Columns: cols,
		Constraints: orderedmap.New[string, *model.Constraint](),
	})
	return tables
}

func TestEvaluateLiterals_RewritesCurrentOnly(t *testing.T) {
	current := literalTables("'01:00:00'::interval")
	desired := literalTables("'1 hour'")
	domains := orderedmap.New[string, *model.Domain]()
	var asked []string
	printer := func(exprs []string) map[string]string {
		asked = exprs
		return map[string]string{"'1 hour'::interval": "01:00:00"}
	}

	tables, _ := evaluateLiterals(printer, current, desired, domains, domains)
	assert.Equal(t, []string{"'1 hour'::interval"}, asked)
	assert.Equal(t, "'1 hour'::interval", *tables.Get("public.t").Columns.Get("iv").Default)
	// The maps passed in are left as they were.
	assert.Equal(t, "'01:00:00'::interval", *current.Get("public.t").Columns.Get("iv").Default)
	assert.Equal(t, "'1 hour'", *desired.Get("public.t").Columns.Get("iv").Default)
}

func TestMatchLiterals(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		current string
		want    []string
	}{
		{"bare", "'1 hour'", "'01:00:00'::interval", []string{"'1 hour'::interval"}},
		{"same type", "interval '1 hour'", "'01:00:00'::interval", []string{"'1 hour'::interval"}},
		{"nested", "now() - '1 day'", "now() - '1 day'::interval", nil},
		{"nested differs", "now() - '24 hours'", "now() - '1 day'::interval", []string{"'24 hours'::interval"}},
		{"other type", "'1 hour'::text", "'01:00:00'::interval", nil},
		{"not a string", "1", "'1'::integer", nil},
		{"uncast current", "'a'", "'b'", nil},
		{"cast on an expression", "now()", "now()::date", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, des, err := pgast.ParseExpr(tt.desired)
			require.NoError(t, err)
			_, cur, err := pgast.ParseExpr(tt.current)
			require.NoError(t, err)
			var got []string
			matchLiterals(des.Val, cur.Val, func(_ *pg_query.String, pair literalPair) {
				got = append(got, pair.expr)
			})
			assert.Equal(t, tt.want, got)
		})
	}
}
