package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A column constraint takes ENFORCED and NOT ENFORCED the way the table
// constraint does, so each column below reads as the table constraint written
// in its comment.
func TestParseSQL_ColumnConstraintEnforced(t *testing.T) {
	tests := []struct {
		column     string
		name       string
		definition string
		validated  bool
		deferrable bool
	}{
		// CONSTRAINT posts_a_check CHECK (a > 0) NOT ENFORCED
		{column: "a integer CHECK (a > 0) NOT ENFORCED", name: "posts_a_check", definition: "CHECK (a > 0) NOT ENFORCED"},
		// CONSTRAINT posts_a_check CHECK (a > 0) ENFORCED
		{column: "a integer CHECK (a > 0) ENFORCED", name: "posts_a_check", definition: "CHECK (a > 0)", validated: true},
		{column: "a integer CHECK (a > 0) NOT ENFORCED NOT NULL", name: "posts_a_check", definition: "CHECK (a > 0) NOT ENFORCED"},
		// CONSTRAINT posts_a_fkey FOREIGN KEY (a) REFERENCES public.p (id) NOT ENFORCED
		{column: "a integer REFERENCES public.p (id) NOT ENFORCED", name: "posts_a_fkey", definition: "FOREIGN KEY (a) REFERENCES public.p (id) NOT ENFORCED"},
		{column: "a integer REFERENCES public.p (id) ENFORCED", name: "posts_a_fkey", definition: "FOREIGN KEY (a) REFERENCES public.p (id)", validated: true},
		// CONSTRAINT posts_a_fkey FOREIGN KEY (a) REFERENCES public.p (id) NOT ENFORCED DEFERRABLE
		{column: "a integer REFERENCES public.p (id) NOT ENFORCED DEFERRABLE", name: "posts_a_fkey", definition: "FOREIGN KEY (a) REFERENCES public.p (id) DEFERRABLE NOT ENFORCED", deferrable: true},
		{column: "a integer REFERENCES public.p (id) DEFERRABLE NOT ENFORCED", name: "posts_a_fkey", definition: "FOREIGN KEY (a) REFERENCES public.p (id) DEFERRABLE NOT ENFORCED", deferrable: true},
	}

	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			result, err := parseSQLWithPublicSchema("CREATE TABLE public.p (id integer PRIMARY KEY);\nCREATE TABLE public.posts (" + tt.column + ");")
			require.NoError(t, err)
			table := result.Tables.Get("public.posts")

			if fk, ok := table.ForeignKeys.GetOk(tt.name); ok {
				assert.Equal(t, tt.definition, fk.Definition)
				assert.Equal(t, tt.validated, fk.Validated)
				assert.Equal(t, tt.deferrable, fk.Deferrable)
				return
			}
			con, ok := table.Constraints.GetOk(tt.name)
			require.True(t, ok)
			assert.Equal(t, tt.definition, con.Definition)
			assert.Equal(t, tt.validated, con.Validated)
		})
	}
}

// The errors are the ones PostgreSQL 18 raises for the same column.
func TestParseSQL_ColumnConstraintEnforcedError(t *testing.T) {
	tests := []struct {
		column string
		err    string
	}{
		{"a integer NOT ENFORCED", "misplaced NOT ENFORCED clause"},
		{"a integer ENFORCED", "misplaced ENFORCED clause"},
		{"a integer UNIQUE NOT ENFORCED", "misplaced NOT ENFORCED clause"},
		{"a integer PRIMARY KEY ENFORCED", "misplaced ENFORCED clause"},
		{"a integer NOT NULL NOT ENFORCED", "misplaced NOT ENFORCED clause"},
		{"a integer CHECK (a > 0) NOT NULL NOT ENFORCED", "misplaced NOT ENFORCED clause"},
		{"a integer CHECK (a > 0) ENFORCED NOT ENFORCED", "multiple ENFORCED/NOT ENFORCED clauses not allowed"},
		{"a integer REFERENCES public.p (id) NOT ENFORCED NOT ENFORCED", "multiple ENFORCED/NOT ENFORCED clauses not allowed"},
		{"a integer CHECK (a > 0) NOT ENFORCED DEFERRABLE", "misplaced DEFERRABLE clause"},
	}

	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			_, err := parseSQLWithPublicSchema("CREATE TABLE public.p (id integer PRIMARY KEY);\nCREATE TABLE public.posts (" + tt.column + ");")
			assert.EqualError(t, err, tt.err)
		})
	}
}
