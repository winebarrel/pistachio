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
	// When color is off, Write streams straight through to the underlying
	// writer — the data is in buf even before Flush is called.
	assert.Equal(t, sql, buf.String())
	require.NoError(t, w.Flush())
	// Flush is a no-op in non-color mode, so the output is unchanged.
	assert.Equal(t, sql, buf.String())
	assert.NotContains(t, buf.String(), "\x1b[")
}

func TestSQLWriter_NoColorStreamsAcrossWrites(t *testing.T) {
	// Each Write must reach the underlying writer immediately when color is
	// off — required for large outputs (multi-MB `dump`) so we don't buffer
	// the whole thing in memory.
	var buf bytes.Buffer
	w := command.NewSQLWriter(&buf, false)
	_, err := w.Write([]byte("chunk1\n"))
	require.NoError(t, err)
	assert.Equal(t, "chunk1\n", buf.String(), "first chunk must stream immediately")
	_, err = w.Write([]byte("chunk2\n"))
	require.NoError(t, err)
	assert.Equal(t, "chunk1\nchunk2\n", buf.String(), "second chunk must append")
}

func TestSQLWriter_FlushResetsBuffer(t *testing.T) {
	// Flush clears the internal buffer so a second Flush emits nothing rather
	// than duplicating the previous output, and so memory can be reclaimed.
	var buf bytes.Buffer
	w := command.NewSQLWriter(&buf, true)
	_, err := w.Write([]byte("CREATE TABLE t (id int);\n"))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	first := buf.String()
	require.NotEmpty(t, first)
	require.NoError(t, w.Flush())
	assert.Equal(t, first, buf.String(), "second Flush must not duplicate output")
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
