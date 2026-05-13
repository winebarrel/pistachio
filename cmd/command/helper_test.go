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
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
