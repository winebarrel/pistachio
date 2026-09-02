package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	pgparser "github.com/pganalyze/pg_query_go/v6/parser"
)

// stmtStart returns the byte offset of a statement's first token. pg_query
// counts the blank lines and comments before a statement as part of it, so
// StmtLocation on its own can point at whitespace rather than at the keyword.
func stmtStart(sql string, rawStmt *pg_query.RawStmt) int32 {
	start := rawStmt.StmtLocation
	end := start + rawStmt.StmtLen
	// pg_query leaves StmtLen at 0 for the final statement in the input.
	if rawStmt.StmtLen == 0 || end > int32(len(sql)) {
		end = int32(len(sql))
	}
	return start + int32(findLeadingCommentEnd(sql[start:end]))
}

// fileSpan records where a desired-schema file starts in the SQL handed to
// the parser, which parses every file joined into one input.
type fileSpan struct {
	path  string
	start int // byte offset in the joined SQL
}

// locatedError is an error that knows the byte offset in the parsed SQL it
// refers to. Directive validation returns it; pg_query errors carry their own
// cursor position instead.
type locatedError struct {
	msg    string
	offset int
}

func (e *locatedError) Error() string {
	return e.msg
}

// annotateError appends the file, line and column an error points at, along
// with the line itself and a caret under the offending column:
//
//	failed to parse SQL: syntax error at or near "TABEL"
//	 --> schema/items.sql:6:8
//	  |
//	6 | CREATE TABEL public.items (
//	  |        ^
//
// The position comes from a pg_query error's cursor (a 1-based character
// index) or a locatedError's byte offset. An error carrying neither is
// returned as it is.
func annotateError(err error, sql string, spans []fileSpan) error {
	offset := -1

	if pgErr, ok := errors.AsType[*pgparser.Error](err); ok && pgErr.Cursorpos > 0 {
		offset = runeOffsetToByte(sql, pgErr.Cursorpos-1)
	} else if locErr, ok := errors.AsType[*locatedError](err); ok {
		offset = locErr.offset
	}

	pos, ok := locate(sql, spans, offset)
	if !ok {
		return err
	}

	// The caret pad mirrors the runes before the column, keeping tabs so the
	// caret lands under the column however the line is indented.
	var pad strings.Builder
	for _, r := range sql[pos.lineStart:pos.offset] {
		if r == '\t' {
			pad.WriteByte('\t')
		} else {
			pad.WriteByte(' ')
		}
	}

	num := strconv.Itoa(pos.line)
	gutter := strings.Repeat(" ", len(num))

	return fmt.Errorf("%s\n%s--> %s\n%s |\n%s | %s\n%s | %s^",
		err.Error(),
		gutter, pos,
		gutter,
		num, sql[pos.lineStart:pos.lineEnd],
		gutter, pad.String())
}

// sourcePos is a byte offset in the parsed SQL resolved to the file it came
// from and the line and column within it.
type sourcePos struct {
	path string
	line int
	col  int
	// offset is the resolved offset, clamped to the input; lineStart and
	// lineEnd bound the line holding it.
	offset    int
	lineStart int
	lineEnd   int
}

func (p sourcePos) String() string {
	return fmt.Sprintf("%s:%d:%d", p.path, p.line, p.col)
}

// locate resolves a byte offset in the SQL handed to the parser against the
// files it was joined from. It reports false when there is no position, or no
// file to name, as when a string was parsed rather than files.
func locate(sql string, spans []fileSpan, offset int) (sourcePos, bool) {
	if offset < 0 || len(spans) == 0 {
		return sourcePos{}, false
	}
	offset = min(offset, len(sql))

	span := spans[0]
	for _, s := range spans[1:] {
		if s.start > offset {
			break
		}
		span = s
	}

	lineStart := strings.LastIndexByte(sql[:offset], '\n') + 1
	lineEnd := len(sql)
	if i := strings.IndexByte(sql[offset:], '\n'); i >= 0 {
		lineEnd = offset + i
	}

	return sourcePos{
		path:      span.path,
		line:      1 + strings.Count(sql[span.start:offset], "\n"),
		col:       1 + utf8.RuneCountInString(sql[lineStart:offset]),
		offset:    offset,
		lineStart: lineStart,
		lineEnd:   lineEnd,
	}, true
}

// runeOffsetToByte returns the byte offset of the n-th rune (0-based), or
// len(sql) when the input has fewer runes.
func runeOffsetToByte(sql string, n int) int {
	runes := 0
	for i := range sql {
		if runes == n {
			return i
		}
		runes++
	}
	return len(sql)
}
