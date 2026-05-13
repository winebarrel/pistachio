package command

import (
	"bytes"
	"io"

	"github.com/alecthomas/chroma/v2/quick"
)

// SQLWriter is an io.Writer that buffers every Write and, on Flush, emits the
// buffered text to the underlying writer — optionally through chroma's
// PostgreSQL lexer. SQL-style comments (e.g. "-- Plan for ...") are colored
// as comments by the same lexer, so callers can build the entire output
// (header + DDL + footer) via fmt.Fprintf-style calls and colorize all of it
// in one shot at the end. Flushing is required because chroma's highlighter
// needs the full input to tokenize correctly (multi-line strings, comments,
// etc. span Write boundaries).
type SQLWriter struct {
	buf   bytes.Buffer
	out   io.Writer
	color bool
}

func NewSQLWriter(w io.Writer, color bool) *SQLWriter {
	return &SQLWriter{out: w, color: color}
}

func (s *SQLWriter) Write(p []byte) (int, error) {
	return s.buf.Write(p)
}

func (s *SQLWriter) Flush() error {
	if !s.color {
		_, err := s.out.Write(s.buf.Bytes())
		return err
	}
	return quick.Highlight(s.out, s.buf.String(), "postgres", "terminal256", "monokai")
}
