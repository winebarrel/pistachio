package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqualCollation(t *testing.T) {
	tests := []struct {
		name     string
		a, b     *string
		expected bool
	}{
		{"both nil", nil, nil, true},
		{"one nil", new(`pg_catalog."C"`), nil, false},
		{"identical", new(`pg_catalog."C"`), new(`pg_catalog."C"`), true},
		{"qualified vs bare", new(`pg_catalog."C"`), new(`"C"`), true},
		{"different name", new(`pg_catalog."C"`), new(`pg_catalog."POSIX"`), false},
		{"different schema", new(`pg_catalog."C"`), new(`public."C"`), false},
		// A dot inside the name is part of the name, not a separator.
		{"dotted name", new(`public."my.coll"`), new(`public."my.coll"`), true},
		{"dotted name vs bare", new(`public."my.coll"`), new(`"my.coll"`), true},
		{"dotted name differs", new(`public."C.utf8"`), new(`public."C.iso"`), false},
		// Without the quote-aware split these would compare as schema "C" and
		// name "utf8".
		{"dotted vs undotted", new(`pg_catalog."C.utf8"`), new(`"C"`), false},
		{"empty", new(""), new(""), true},
		// quote_ident quotes col_name keywords, model.Ident does not; the two
		// forms must still compare equal.
		{"quoted vs bare name", new(`public."int"`), new(`public.int`), true},
		{"quoted vs bare schema", new(`"int"."C"`), new(`int."C"`), true},
		{"case folding", new(`public.mycoll`), new(`public.MyColl`), true},
		{"quoted case is significant", new(`public."MyColl"`), new(`public.mycoll`), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, equalCollation(tt.a, tt.b))
		})
	}
}
