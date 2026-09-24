package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The expected values are what PostgreSQL 18 gives for the same column in
// CREATE TABLE: pg_constraint.condeferrable/condeferred, or the error it
// raises.
func TestParseSQL_ColumnConstraintAttrs(t *testing.T) {
	type deferral struct{ deferrable, deferred bool }

	tests := []struct {
		column string
		fk     *deferral // posts_a_fkey
		unique *deferral // posts_a_key
		pkey   *deferral // posts_pkey
	}{
		{column: "a integer REFERENCES public.p (id)", fk: &deferral{false, false}},
		{column: "a integer REFERENCES public.p (id) DEFERRABLE", fk: &deferral{true, false}},
		{column: "a integer REFERENCES public.p (id) NOT DEFERRABLE", fk: &deferral{false, false}},
		{column: "a integer REFERENCES public.p (id) INITIALLY DEFERRED", fk: &deferral{true, true}},
		{column: "a integer REFERENCES public.p (id) INITIALLY IMMEDIATE", fk: &deferral{false, false}},
		{column: "a integer REFERENCES public.p (id) DEFERRABLE INITIALLY DEFERRED", fk: &deferral{true, true}},
		{column: "a integer REFERENCES public.p (id) INITIALLY DEFERRED DEFERRABLE", fk: &deferral{true, true}},
		{column: "a integer REFERENCES public.p (id) DEFERRABLE INITIALLY IMMEDIATE", fk: &deferral{true, false}},
		{column: "a integer REFERENCES public.p (id) INITIALLY IMMEDIATE DEFERRABLE", fk: &deferral{true, false}},
		{column: "a integer REFERENCES public.p (id) NOT DEFERRABLE INITIALLY IMMEDIATE", fk: &deferral{false, false}},
		{column: "a integer REFERENCES public.p (id) INITIALLY IMMEDIATE NOT DEFERRABLE", fk: &deferral{false, false}},
		{column: "a integer REFERENCES public.p (id) DEFERRABLE NOT NULL", fk: &deferral{true, false}},
		{column: "a integer UNIQUE DEFERRABLE", unique: &deferral{true, false}},
		{column: "a integer PRIMARY KEY INITIALLY DEFERRED", pkey: &deferral{true, true}},
		// Each constraint takes the clauses after it, up to the next one.
		{column: "a integer UNIQUE REFERENCES public.p (id) DEFERRABLE", fk: &deferral{true, false}, unique: &deferral{false, false}},
		{column: "a integer UNIQUE DEFERRABLE REFERENCES public.p (id) DEFERRABLE", fk: &deferral{true, false}, unique: &deferral{true, false}},
		{
			column: "a integer UNIQUE DEFERRABLE INITIALLY DEFERRED REFERENCES public.p (id) NOT DEFERRABLE INITIALLY IMMEDIATE",
			fk:     &deferral{false, false},
			unique: &deferral{true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			result, err := parseSQLWithPublicSchema("CREATE TABLE public.p (id integer PRIMARY KEY);\nCREATE TABLE public.posts (" + tt.column + ");")
			require.NoError(t, err)
			table := result.Tables.Get("public.posts")

			fk, ok := table.ForeignKeys.GetOk("posts_a_fkey")
			assert.Equal(t, tt.fk != nil, ok)
			if tt.fk != nil && ok {
				got := deferral{fk.Deferrable, fk.Deferred}
				assert.Equal(t, *tt.fk, got)
			}
			for name, want := range map[string]*deferral{"posts_a_key": tt.unique, "posts_pkey": tt.pkey} {
				con, ok := table.Constraints.GetOk(name)
				assert.Equal(t, want != nil, ok, name)
				if want != nil && ok {
					got := deferral{con.Deferrable, con.Deferred}
					assert.Equal(t, *want, got, name)
				}
			}
		})
	}
}

func TestParseSQL_ColumnConstraintAttrsError(t *testing.T) {
	tests := []struct {
		column string
		err    string
	}{
		{"a integer REFERENCES public.p (id) NOT DEFERRABLE INITIALLY DEFERRED", "constraint declared INITIALLY DEFERRED must be DEFERRABLE"},
		{"a integer REFERENCES public.p (id) INITIALLY DEFERRED NOT DEFERRABLE", "constraint declared INITIALLY DEFERRED must be DEFERRABLE"},
		{"a integer REFERENCES public.p (id) DEFERRABLE DEFERRABLE", "multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"},
		{"a integer REFERENCES public.p (id) DEFERRABLE NOT DEFERRABLE", "multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"},
		{"a integer REFERENCES public.p (id) NOT DEFERRABLE DEFERRABLE", "multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"},
		{"a integer REFERENCES public.p (id) NOT DEFERRABLE NOT DEFERRABLE", "multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"},
		{"a integer REFERENCES public.p (id) INITIALLY DEFERRED INITIALLY DEFERRED", "multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"},
		{"a integer REFERENCES public.p (id) INITIALLY DEFERRED INITIALLY IMMEDIATE", "multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"},
		{"a integer REFERENCES public.p (id) INITIALLY IMMEDIATE INITIALLY DEFERRED", "multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"},
		{"a integer REFERENCES public.p (id) INITIALLY IMMEDIATE INITIALLY IMMEDIATE", "multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"},
		{"a integer DEFERRABLE", "misplaced DEFERRABLE clause"},
		{"a integer NOT DEFERRABLE", "misplaced NOT DEFERRABLE clause"},
		{"a integer INITIALLY DEFERRED", "misplaced INITIALLY DEFERRED clause"},
		{"a integer INITIALLY IMMEDIATE", "misplaced INITIALLY IMMEDIATE clause"},
		{"a integer NOT NULL DEFERRABLE", "misplaced DEFERRABLE clause"},
		{"a integer NULL DEFERRABLE", "misplaced DEFERRABLE clause"},
		{"a integer DEFAULT 1 DEFERRABLE", "misplaced DEFERRABLE clause"},
		{"a integer CHECK (a > 0) DEFERRABLE", "misplaced DEFERRABLE clause"},
		{"a integer CHECK (a > 0) INITIALLY DEFERRED", "misplaced INITIALLY DEFERRED clause"},
		{"a integer GENERATED ALWAYS AS (1) STORED DEFERRABLE", "misplaced DEFERRABLE clause"},
		{"a integer GENERATED ALWAYS AS IDENTITY DEFERRABLE", "misplaced DEFERRABLE clause"},
		{"a integer REFERENCES public.p (id) NOT NULL DEFERRABLE", "misplaced DEFERRABLE clause"},
	}

	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			_, err := parseSQLWithPublicSchema("CREATE TABLE public.p (id integer PRIMARY KEY);\nCREATE TABLE public.posts (" + tt.column + ");")
			assert.EqualError(t, err, tt.err)
		})
	}
}
