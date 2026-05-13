package command

import (
	"bytes"
	"io"

	"github.com/alecthomas/chroma/v2/quick"
)

// SQLWriter is an io.Writer that, when color is enabled, buffers every Write
// and on Flush emits the buffered text to the underlying writer through
// chroma's PostgreSQL lexer. SQL-style comments (e.g. "-- Plan for ...") are
// colored as comments by the same lexer, so callers can build the entire
// output (header + DDL + footer) via fmt.Fprintf-style calls and colorize
// all of it in one shot at the end. Buffering is required because chroma's
// highlighter needs the full input to tokenize correctly (multi-line strings,
// comments, etc. span Write boundaries).
//
// When color is disabled, Write passes straight through to the underlying
// writer and Flush is a no-op, preserving the streaming behavior of
// pre-color pista output for piped/redirected use (e.g. `pista dump | gzip`
// must not balloon RSS to buffer a multi-MB schema dump).
type SQLWriter struct {
	buf   bytes.Buffer
	out   io.Writer
	color bool
}

func NewSQLWriter(w io.Writer, color bool) *SQLWriter {
	return &SQLWriter{out: w, color: color}
}

func (s *SQLWriter) Write(p []byte) (int, error) {
	if !s.color {
		return s.out.Write(p)
	}
	return s.buf.Write(p)
}

// Flush highlights and emits the buffered text. It also resets the internal
// buffer so a second Flush is a no-op (rather than duplicating output) and
// the buffered memory can be reclaimed.
func (s *SQLWriter) Flush() error {
	if !s.color {
		return nil
	}
	err := quick.Highlight(s.out, s.buf.String(), "postgres", "terminal256", "monokai")
	s.buf.Reset()
	return err
}
