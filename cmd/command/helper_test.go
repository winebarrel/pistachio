package command_test

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

// assertConnectedCommentFirst verifies plan/apply/dump prepend a
// "-- Connected to ..." comment as the first output line, and that the
// password from the test connection (if any) does not appear in the output.
func assertConnectedCommentFirst(t *testing.T, out string, cfg *pgx.ConnConfig) {
	t.Helper()
	assert.True(t, strings.HasPrefix(out, "-- Connected to "), "first line must be '-- Connected to ...', got: %q", firstLine(out))
	if cfg.Password != "" {
		assert.NotContains(t, out, cfg.Password, "password must not appear in output")
	}
}

func firstLine(s string) string {
	if before, _, ok := strings.Cut(s, "\n"); ok {
		return before
	}
	return s
}

// columnsByName indexes a table's columns by name. The document writes them as
// an array, so a test that wants one column looks it up rather than keying
// into a map.
func columnsByName(t *testing.T, table map[string]any) map[string]map[string]any {
	t.Helper()

	columns := map[string]map[string]any{}
	for _, c := range table["columns"].([]any) {
		column := c.(map[string]any)
		columns[column["name"].(string)] = column
	}

	return columns
}
