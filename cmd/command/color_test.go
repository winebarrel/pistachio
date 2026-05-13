package command_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/cmd/command"
)

func TestSQLWriter_NoColor(t *testing.T) {
	var buf bytes.Buffer
	w := command.NewSQLWriter(&buf, false)
	sql := "-- Plan for schema public\nCREATE TABLE t (id int);\n"
	_, err := w.Write([]byte(sql))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	assert.Equal(t, sql, buf.String())
	// No ANSI escape sequences when color is off.
	assert.NotContains(t, buf.String(), "\x1b[")
}

func TestSQLWriter_WithColor(t *testing.T) {
	var buf bytes.Buffer
	w := command.NewSQLWriter(&buf, true)
	sql := "CREATE TABLE t (id int);\n"
	_, err := w.Write([]byte(sql))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	// ANSI escape sequences are emitted by the terminal256 formatter.
	assert.Contains(t, buf.String(), "\x1b[")
	// The original SQL text is still present in the output.
	stripped := stripANSI(buf.String())
	assert.Contains(t, stripped, "CREATE TABLE")
}

func TestSQLWriter_FlushEmpty(t *testing.T) {
	var buf bytes.Buffer
	w := command.NewSQLWriter(&buf, false)
	require.NoError(t, w.Flush())
	assert.Empty(t, buf.String())
}

// stripANSI removes ANSI escape sequences from s.
func stripANSI(s string) string {
	var out strings.Builder
	inEsc := false
	for _, r := range s {
		if inEsc {
			if r == 'm' {
				inEsc = false
			}
			continue
		}
		if r == '\x1b' {
			inEsc = true
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}
