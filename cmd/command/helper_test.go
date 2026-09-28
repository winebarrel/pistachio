package command_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
)

// assertConnectedCommentFirst verifies plan/apply/dump write the connection
// comment for connString as the first output line. What the comment holds,
// and that it carries no password, is ConnInfoComment's to test.
func assertConnectedCommentFirst(t *testing.T, out string, connString string) {
	t.Helper()
	want, err := pistachio.NewClient(&pistachio.Options{ConnString: connString}).ConnInfoComment()
	require.NoError(t, err)
	assert.Equal(t, want, firstLine(out))
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
