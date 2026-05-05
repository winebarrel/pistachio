package pistachio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeAlterTable(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "empty",
			in:   nil,
			want: []string{},
		},
		{
			name: "single mergeable stmt is left as-is",
			in:   []string{"ALTER TABLE public.users ADD COLUMN email text;"},
			want: []string{"ALTER TABLE public.users ADD COLUMN email text;"},
		},
		{
			name: "two adds on same table merge",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN email text;",
				"ALTER TABLE public.users ADD COLUMN age int;",
			},
			want: []string{
				"ALTER TABLE public.users\n  ADD COLUMN email text,\n  ADD COLUMN age int;",
			},
		},
		{
			name: "mixed mergeable actions on same table merge",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN email text;",
				"ALTER TABLE public.users ALTER COLUMN name SET NOT NULL;",
				"ALTER TABLE public.users DROP COLUMN legacy;",
				"ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0);",
				"ALTER TABLE public.users DROP CONSTRAINT old_chk;",
			},
			want: []string{
				"ALTER TABLE public.users\n  ADD COLUMN email text,\n  ALTER COLUMN name SET NOT NULL,\n  DROP COLUMN legacy,\n  ADD CONSTRAINT chk_age CHECK (age > 0),\n  DROP CONSTRAINT old_chk;",
			},
		},
		{
			name: "different tables do not merge across",
			in: []string{
				"ALTER TABLE public.a ADD COLUMN x int;",
				"ALTER TABLE public.a ADD COLUMN y int;",
				"ALTER TABLE public.b ADD COLUMN z int;",
				"ALTER TABLE public.b ADD COLUMN w int;",
			},
			want: []string{
				"ALTER TABLE public.a\n  ADD COLUMN x int,\n  ADD COLUMN y int;",
				"ALTER TABLE public.b\n  ADD COLUMN z int,\n  ADD COLUMN w int;",
			},
		},
		{
			name: "RENAME breaks group and is left untouched",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"ALTER TABLE public.users RENAME COLUMN old TO new;",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
			want: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"ALTER TABLE public.users RENAME COLUMN old TO new;",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
		},
		{
			name: "RENAME TO is not merged",
			in: []string{
				"ALTER TABLE public.users RENAME TO accounts;",
				"ALTER TABLE public.accounts ADD COLUMN x int;",
				"ALTER TABLE public.accounts ADD COLUMN y int;",
			},
			want: []string{
				"ALTER TABLE public.users RENAME TO accounts;",
				"ALTER TABLE public.accounts\n  ADD COLUMN x int,\n  ADD COLUMN y int;",
			},
		},
		{
			name: "RENAME CONSTRAINT is not merged",
			in: []string{
				"ALTER TABLE public.users RENAME CONSTRAINT old TO new;",
				"ALTER TABLE public.users DROP CONSTRAINT chk_age;",
			},
			want: []string{
				"ALTER TABLE public.users RENAME CONSTRAINT old TO new;",
				"ALTER TABLE public.users DROP CONSTRAINT chk_age;",
			},
		},
		{
			name: "VALIDATE CONSTRAINT is not merged",
			in: []string{
				"ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0) NOT VALID;",
				"ALTER TABLE public.users VALIDATE CONSTRAINT chk_age;",
				"ALTER TABLE public.users ADD CONSTRAINT chk_b CHECK (b > 0);",
			},
			want: []string{
				"ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0) NOT VALID;",
				"ALTER TABLE public.users VALIDATE CONSTRAINT chk_age;",
				"ALTER TABLE public.users ADD CONSTRAINT chk_b CHECK (b > 0);",
			},
		},
		{
			name: "RLS toggles are not merged",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;",
				"ALTER TABLE public.users FORCE ROW LEVEL SECURITY;",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
			want: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;",
				"ALTER TABLE public.users FORCE ROW LEVEL SECURITY;",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
		},
		{
			name: "skipped comment breaks group",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"-- skipped: ALTER TABLE public.users DROP COLUMN legacy;",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
			want: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"-- skipped: ALTER TABLE public.users DROP COLUMN legacy;",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
		},
		{
			name: "CREATE INDEX between actions breaks group",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"CREATE INDEX idx_x ON public.users (x);",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
			want: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"CREATE INDEX idx_x ON public.users (x);",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
		},
		{
			name: "COMMENT ON COLUMN does not merge",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"COMMENT ON COLUMN public.users.x IS 'hello';",
			},
			want: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"COMMENT ON COLUMN public.users.x IS 'hello';",
			},
		},
		{
			name: "CREATE TABLE does not merge",
			in: []string{
				"CREATE TABLE public.t (id int);",
				"ALTER TABLE public.t ADD COLUMN y int;",
			},
			want: []string{
				"CREATE TABLE public.t (id int);",
				"ALTER TABLE public.t ADD COLUMN y int;",
			},
		},
		{
			name: "ALTER TABLE ONLY is not merged (FK shape)",
			in: []string{
				"ALTER TABLE ONLY public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);",
				"ALTER TABLE public.orders ADD COLUMN x int;",
			},
			want: []string{
				"ALTER TABLE ONLY public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);",
				"ALTER TABLE public.orders ADD COLUMN x int;",
			},
		},
		{
			name: "ALTER TABLE IF EXISTS is not merged",
			in: []string{
				"ALTER TABLE IF EXISTS public.users ADD COLUMN x int;",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
			want: []string{
				"ALTER TABLE IF EXISTS public.users ADD COLUMN x int;",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
		},
		{
			name: "quoted schema-qualified identifier",
			in: []string{
				`ALTER TABLE "MySchema"."MyTable" ADD COLUMN x int;`,
				`ALTER TABLE "MySchema"."MyTable" ADD COLUMN y int;`,
			},
			want: []string{
				"ALTER TABLE \"MySchema\".\"MyTable\"\n  ADD COLUMN x int,\n  ADD COLUMN y int;",
			},
		},
		{
			name: "two non-consecutive groups across same table with break in between",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN x int;",
				"ALTER TABLE public.users ADD COLUMN y int;",
				"ALTER TABLE public.users RENAME COLUMN old TO new;",
				"ALTER TABLE public.users ADD COLUMN z int;",
				"ALTER TABLE public.users ADD COLUMN w int;",
			},
			want: []string{
				"ALTER TABLE public.users\n  ADD COLUMN x int,\n  ADD COLUMN y int;",
				"ALTER TABLE public.users RENAME COLUMN old TO new;",
				"ALTER TABLE public.users\n  ADD COLUMN z int,\n  ADD COLUMN w int;",
			},
		},
		{
			name: "malformed ALTER TABLE without semicolon is left as-is",
			in: []string{
				"ALTER TABLE public.users ADD COLUMN x int",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
			want: []string{
				"ALTER TABLE public.users ADD COLUMN x int",
				"ALTER TABLE public.users ADD COLUMN y int;",
			},
		},
		{
			name: "ALTER TABLE with no fqtn is left as-is",
			in: []string{
				"ALTER TABLE  ADD COLUMN x int;",
			},
			want: []string{
				"ALTER TABLE  ADD COLUMN x int;",
			},
		},
		{
			name: "ALTER TABLE with non-identifier first char is left as-is",
			in: []string{
				"ALTER TABLE ;",
			},
			want: []string{
				"ALTER TABLE ;",
			},
		},
		{
			name: "ALTER TABLE with no space after fqtn is left as-is",
			in: []string{
				"ALTER TABLE public.users;",
			},
			want: []string{
				"ALTER TABLE public.users;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeAlterTable(tt.in)
			if tt.want == nil {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseMergeableAlterTable_Negatives(t *testing.T) {
	negatives := []string{
		"",
		"DROP TABLE public.users;",
		"CREATE TABLE public.t (id int);",
		"-- skipped: ALTER TABLE public.users DROP COLUMN x;",
		"ALTER TABLE public.users RENAME TO accounts;",
		"ALTER TABLE public.users RENAME COLUMN a TO b;",
		"ALTER TABLE public.users RENAME CONSTRAINT a TO b;",
		"ALTER TABLE public.users VALIDATE CONSTRAINT chk;",
		"ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;",
		"ALTER TABLE public.users DISABLE ROW LEVEL SECURITY;",
		"ALTER TABLE public.users FORCE ROW LEVEL SECURITY;",
		"ALTER TABLE public.users NO FORCE ROW LEVEL SECURITY;",
		"ALTER TABLE ONLY public.users ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES public.t(id);",
		"ALTER TABLE IF EXISTS public.users ADD COLUMN x int;",
	}

	for _, stmt := range negatives {
		t.Run(stmt, func(t *testing.T) {
			_, _, ok := parseMergeableAlterTable(stmt)
			assert.False(t, ok, "expected non-mergeable: %q", stmt)
		})
	}
}
