package command_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertConnectedCommentFirst verifies plan/apply/dump prepend a
// "-- Connected to ..." comment as the first output line, and that the comment
// carries no password. It checks where the password would go rather than
// searching the output for its value, so the check also runs when the test
// connection has no password, and a password that equals the user name does
// not fail it.
func assertConnectedCommentFirst(t *testing.T, out string, cfg *pgx.ConnConfig) {
	t.Helper()
	target, ok := strings.CutPrefix(firstLine(out), "-- Connected to ")
	if !assert.True(t, ok, "first line must be '-- Connected to ...', got: %q", firstLine(out)) {
		return
	}
	if strings.HasPrefix(target, "host=") {
		assert.NotContains(t, target, "password=", "password must not appear in output")
		return
	}
	u, err := url.Parse(target)
	require.NoError(t, err)
	assert.Equal(t, cfg.User, u.User.Username())
	_, set := u.User.Password()
	assert.False(t, set, "password must not appear in output")
	assert.False(t, u.Query().Has("password"), "password must not appear in output")
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
