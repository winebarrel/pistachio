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
		{"one nil", ptr(`pg_catalog."C"`), nil, false},
		{"identical", ptr(`pg_catalog."C"`), ptr(`pg_catalog."C"`), true},
		{"qualified vs bare", ptr(`pg_catalog."C"`), ptr(`"C"`), true},
		{"different name", ptr(`pg_catalog."C"`), ptr(`pg_catalog."POSIX"`), false},
		{"different schema", ptr(`pg_catalog."C"`), ptr(`public."C"`), false},
		// A dot inside the name is part of the name, not a separator.
		{"dotted name", ptr(`public."my.coll"`), ptr(`public."my.coll"`), true},
		{"dotted name vs bare", ptr(`public."my.coll"`), ptr(`"my.coll"`), true},
		{"dotted name differs", ptr(`public."C.utf8"`), ptr(`public."C.iso"`), false},
		// Without the quote-aware split these would compare as schema "C" and
		// name "utf8".
		{"dotted vs undotted", ptr(`pg_catalog."C.utf8"`), ptr(`"C"`), false},
		{"empty", ptr(""), ptr(""), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, equalCollation(tt.a, tt.b))
		})
	}
}

func ptr(s string) *string { return &s }
