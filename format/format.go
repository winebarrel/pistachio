// Package format lays out schema SQL without changing the statements it
// reads. It works on the token stream, so the identifiers, the keywords and
// the expressions come out exactly as they were written; only the whitespace
// between the tokens moves. The one exception is a quoted identifier that does
// not need its quotes, which is unquoted after the change is proved to leave
// the statement's meaning alone.
package format

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// indentUnit is one level of indentation inside a definition list. pg_dump and
// pista dump both write four spaces.
const indentUnit = "    "

type token struct {
	kind pg_query.Token
	// orig is the token as it was written, out the token as it is emitted.
	// The two differ only for an identifier that lost its quotes.
	orig  string
	out   string
	start int
	end   int
	// nl counts the newlines between the previous token and this one, space
	// reports whitespace without a newline, and indent holds the original
	// indentation of the line this token opens.
	nl     int
	space  bool
	indent string
}

func (t *token) isComment() bool {
	return t.kind == pg_query.Token_SQL_COMMENT || t.kind == pg_query.Token_C_COMMENT
}

// isLineComment reports a "--" comment, which swallows the rest of its line.
// A token after one has to start a new line.
func (t *token) isLineComment() bool {
	return t.kind == pg_query.Token_SQL_COMMENT
}

type stmt struct {
	start int
	end   int
	node  *pg_query.Node
}

// region is the definition list of a CREATE TABLE or a CREATE TYPE, from its
// opening parenthesis to the matching closing one.
type region struct {
	open  int
	close int
}

// Format returns sql laid out. It fails when sql does not parse, and when the
// result would not carry the same tokens as the input.
func Format(sql string) (string, error) {
	toks, err := scan(sql)
	if err != nil {
		return "", err
	}

	if len(toks) == 0 {
		return sql, nil
	}

	stmts, err := split(sql)
	if err != nil {
		return "", err
	}

	unquote(sql, toks, stmts)
	depth := parenDepth(toks)
	out := render(toks, &layout{
		regions:    listRegions(toks, stmts, depth),
		depth:      depth,
		body:       viewBodies(toks, stmts, depth),
		stmtStart:  stmtStarts(toks, stmts),
		routineCon: routineConts(toks, stmts),
	})

	if err := verify(toks, out); err != nil {
		return "", err
	}

	return out, nil
}

func scan(sql string) ([]*token, error) {
	result, err := pg_query.Scan(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to scan SQL: %w", err)
	}

	toks := make([]*token, 0, len(result.Tokens))
	prev := 0

	for _, t := range result.Tokens {
		start, end := int(t.Start), int(t.End)
		if start < prev || end > len(sql) || start > end {
			return nil, fmt.Errorf("unexpected token range %d-%d", start, end)
		}

		gap := sql[prev:start]
		tok := &token{
			kind:  t.Token,
			orig:  sql[start:end],
			out:   sql[start:end],
			start: start,
			end:   end,
			nl:    strings.Count(gap, "\n"),
		}

		switch {
		case tok.nl > 0:
			tok.indent = gap[strings.LastIndex(gap, "\n")+1:]
		case len(toks) == 0:
			tok.indent = gap
		default:
			tok.space = gap != ""
		}

		toks = append(toks, tok)
		prev = end
	}

	return toks, nil
}

// split returns one entry per statement. A statement pg_query reports no
// length for, which is what a file whose last statement has no semicolon
// gives, runs to the next statement or to the end of the input.
func split(sql string) ([]stmt, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}

	stmts := make([]stmt, 0, len(tree.Stmts))

	for i, raw := range tree.Stmts {
		start := int(raw.StmtLocation)
		end := start + int(raw.StmtLen)
		if raw.StmtLen == 0 {
			if i+1 < len(tree.Stmts) {
				end = int(tree.Stmts[i+1].StmtLocation)
			} else {
				end = len(sql)
			}
		}
		stmts = append(stmts, stmt{start: start, end: end, node: raw.Stmt})
	}

	return stmts, nil
}

// tokenRange returns the tokens of one statement, from the first that starts
// at or after it to the first that starts after it ends. The tokens are in
// order, so the ends are found by binary search: a linear scan per statement
// makes formatting quadratic in the size of the file, which a dump of a few
// thousand tables notices.
func tokenRange(toks []*token, s stmt) (int, int) {
	lo := sort.Search(len(toks), func(i int) bool { return toks[i].start >= s.start })
	hi := sort.Search(len(toks), func(i int) bool { return toks[i].start >= s.end })

	return lo, hi
}

// nextCode returns the index of the first token at or after i that is not a
// comment.
func nextCode(toks []*token, i, hi int) int {
	for ; i < hi; i++ {
		if !toks[i].isComment() {
			return i
		}
	}

	return hi
}

func upper(t *token) string {
	return strings.ToUpper(t.orig)
}

// parenDepth returns the parenthesis depth of every token. A closing
// parenthesis carries the depth it leaves behind, so the line it opens lines
// up with whatever opened the group.
func parenDepth(toks []*token) []int {
	depth := make([]int, len(toks))
	level := 0

	for i, t := range toks {
		switch {
		case t.isComment():
			depth[i] = level
		case t.orig == "(":
			depth[i] = level
			level++
		case t.orig == ")":
			if level > 0 {
				level--
			}
			depth[i] = level
		default:
			depth[i] = level
		}
	}

	return depth
}

// viewBodies marks the query of a view, a materialized view and a CREATE TABLE
// AS. PostgreSQL writes such a body back with an indentation of its own, which
// dump carries, so the depth rule is kept away from it.
func viewBodies(toks []*token, stmts []stmt, depth []int) []bool {
	body := make([]bool, len(toks))

	for _, s := range stmts {
		if s.node.GetViewStmt() == nil && s.node.GetCreateTableAsStmt() == nil {
			continue
		}

		lo, hi := tokenRange(toks, s)
		if lo >= hi {
			continue
		}
		base := depth[lo]

		for i := lo; i < hi; i++ {
			if !toks[i].isComment() && depth[i] == base && upper(toks[i]) == "AS" {
				for j := i + 1; j < hi; j++ {
					body[j] = true
				}
				break
			}
		}
	}

	return body
}

// routineConts marks the tokens of a CREATE FUNCTION or CREATE PROCEDURE that
// are not the first of the statement. A line one of them opens is a
// continuation of the statement, which dump indents by one level.
func routineConts(toks []*token, stmts []stmt) []bool {
	cont := make([]bool, len(toks))

	for _, s := range stmts {
		if s.node.GetCreateFunctionStmt() == nil {
			continue
		}

		lo, hi := tokenRange(toks, s)
		if first := nextCode(toks, lo, hi); first < hi {
			for i := first + 1; i < hi; i++ {
				cont[i] = true
			}
		}
	}

	return cont
}

// listRegions marks the definition lists of the statements that have one, and
// records the parenthesis depth of every token inside them. A closing
// parenthesis carries the depth it leaves behind, so the line it opens lines
// up with the element that owns it.
func listRegions(toks []*token, stmts []stmt, depth []int) []*region {
	regions := make([]*region, len(toks))

	for _, s := range stmts {
		lo, hi := tokenRange(toks, s)

		open, ok := listOpen(toks, lo, hi, s.node)
		if !ok {
			continue
		}

		closeIdx, ok := matchParen(toks, open, hi, depth)
		if !ok {
			continue
		}

		r := &region{open: open, close: closeIdx}
		for i := open; i <= closeIdx; i++ {
			regions[i] = r
		}
	}

	return regions
}

// matchParen returns the parenthesis that closes the one at open.
func matchParen(toks []*token, open, hi int, depth []int) (int, bool) {
	for i := open + 1; i < hi; i++ {
		if toks[i].orig == ")" && depth[i] == depth[open] {
			return i, true
		}
	}

	return 0, false
}

// listOpen returns the parenthesis that opens the statement's definition list.
// Only CREATE TABLE and the two CREATE TYPE forms have one; dump writes those
// three across several lines and every other statement on one.
func listOpen(toks []*token, lo, hi int, node *pg_query.Node) (int, bool) {
	switch {
	case node.GetCreateStmt() != nil:
		return tableListOpen(toks, lo, hi)
	case node.GetCompositeTypeStmt() != nil, node.GetCreateEnumStmt() != nil:
		return typeListOpen(toks, lo, hi)
	}

	return 0, false
}

// tableListOpen finds the parenthesis that follows the table name. A partition
// child written as PARTITION OF has none, and neither has a typed table, so
// the name has to be walked rather than assumed.
func tableListOpen(toks []*token, lo, hi int) (int, bool) {
	i := lo
	for ; i < hi; i++ {
		if !toks[i].isComment() && upper(toks[i]) == "TABLE" {
			break
		}
	}
	if i == hi {
		return 0, false
	}

	i = nextCode(toks, i+1, hi)
	if i+2 < hi && upper(toks[i]) == "IF" && upper(toks[i+1]) == "NOT" && upper(toks[i+2]) == "EXISTS" {
		i = nextCode(toks, i+3, hi)
	}

	// The name is one part, or several joined by dots.
	if i >= hi || toks[i].orig == "(" {
		return 0, false
	}
	i = nextCode(toks, i+1, hi)
	for i < hi && toks[i].orig == "." {
		i = nextCode(toks, i+1, hi)
		if i >= hi {
			return 0, false
		}
		i = nextCode(toks, i+1, hi)
	}

	if i < hi && toks[i].orig == "(" {
		return i, true
	}

	return 0, false
}

// typeListOpen finds the parenthesis after AS, or after AS ENUM.
func typeListOpen(toks []*token, lo, hi int) (int, bool) {
	i := lo
	for ; i < hi; i++ {
		if !toks[i].isComment() && upper(toks[i]) == "AS" {
			break
		}
	}
	if i == hi {
		return 0, false
	}

	i = nextCode(toks, i+1, hi)
	if i < hi && upper(toks[i]) == "ENUM" {
		i = nextCode(toks, i+1, hi)
	}

	if i < hi && toks[i].orig == "(" {
		return i, true
	}

	return 0, false
}

var bareIdentPattern = regexp.MustCompile(`^[a-z_][a-z0-9_$]*$`)

// unquote drops the quotes around an identifier that reads the same without
// them. Whether it does depends on where the identifier sits: a column may be
// named name unquoted, a function may not be named precision unquoted. Rather
// than model the grammar, the statement is parsed again with the quotes gone
// and kept only when it deparses to what it deparsed to before.
func unquote(sql string, toks []*token, stmts []stmt) {
	for _, s := range stmts {
		lo, hi := tokenRange(toks, s)

		var candidates []int
		for i := lo; i < hi; i++ {
			if _, ok := bareIdent(toks[i]); ok {
				candidates = append(candidates, i)
			}
		}
		if len(candidates) == 0 {
			continue
		}

		text := sql[s.start:s.end]
		before, ok := deparse(text)
		if !ok {
			continue
		}

		keeps := func(sel []int) bool {
			after, ok := deparse(replaceTokens(text, s.start, toks, sel))
			return ok && after == before
		}

		if keeps(candidates) {
			applyUnquote(toks, candidates)
			continue
		}

		// One of them changes the statement. Keep the ones that do not.
		var safe []int
		for _, i := range candidates {
			if keeps([]int{i}) {
				safe = append(safe, i)
			}
		}
		if len(safe) > 0 && keeps(safe) {
			applyUnquote(toks, safe)
		}
	}
}

// bareIdent returns the identifier's text without its quotes, when dropping
// them is worth trying at all. An identifier holding an upper-case letter
// folds to something else unquoted, so it keeps its quotes.
//
// The keyword categories follow model.quoteIdent, which is how dump decides:
// an unreserved or a col_name keyword is written bare, a reserved or a
// type_func_name keyword is quoted. Whether the position at hand really takes
// the bare spelling is settled by the caller, which parses the statement
// again.
func bareIdent(t *token) (string, bool) {
	if t.kind != pg_query.Token_IDENT || len(t.orig) < 3 {
		return "", false
	}
	if !strings.HasPrefix(t.orig, `"`) || !strings.HasSuffix(t.orig, `"`) {
		return "", false
	}

	inner := strings.ReplaceAll(t.orig[1:len(t.orig)-1], `""`, `"`)
	if !bareIdentPattern.MatchString(inner) {
		return "", false
	}

	result, err := pg_query.Scan(inner)
	if err != nil || len(result.Tokens) != 1 {
		return "", false
	}

	switch result.Tokens[0].KeywordKind {
	case pg_query.KeywordKind_NO_KEYWORD,
		pg_query.KeywordKind_UNRESERVED_KEYWORD,
		pg_query.KeywordKind_COL_NAME_KEYWORD:
		return inner, true
	default:
		return "", false
	}
}

func applyUnquote(toks []*token, sel []int) {
	for _, i := range sel {
		if inner, ok := bareIdent(toks[i]); ok {
			toks[i].out = inner
		}
	}
}

// replaceTokens rewrites text, which starts at offset in the input, with the
// selected identifiers unquoted.
func replaceTokens(text string, offset int, toks []*token, sel []int) string {
	var b strings.Builder
	prev := 0

	for _, i := range sel {
		inner, ok := bareIdent(toks[i])
		if !ok {
			continue
		}
		start, end := toks[i].start-offset, toks[i].end-offset
		if start < prev || end > len(text) {
			continue
		}
		b.WriteString(text[prev:start])
		b.WriteString(inner)
		prev = end
	}

	b.WriteString(text[prev:])

	return b.String()
}

func deparse(text string) (string, bool) {
	tree, err := pg_query.Parse(text)
	if err != nil {
		return "", false
	}

	out, err := pg_query.Deparse(tree)
	if err != nil {
		return "", false
	}

	return out, true
}

// stmtStarts marks the first token of every statement, each of which opens a
// line of its own. A comment the input left after the previous semicolon is
// not moved, so the mark sits on the first token that is not one.
func stmtStarts(toks []*token, stmts []stmt) []bool {
	starts := make([]bool, len(toks))

	for _, s := range stmts {
		lo, hi := tokenRange(toks, s)
		if j := nextCode(toks, lo, hi); j < hi {
			starts[j] = true
		}
	}

	return starts
}

// layout holds what the renderer needs to know about each token: the
// definition list it belongs to, its parenthesis depth, whether it sits in a
// view body, and whether it opens a statement.
type layout struct {
	regions    []*region
	depth      []int
	body       []bool
	stmtStart  []bool
	routineCon []bool
}

// parenFrame remembers where an open parenthesis sits, so the lines inside it
// hang off the line that opened it and the closing parenthesis lines up with
// that line again.
type parenFrame struct {
	openLine string
	content  string
}

func render(toks []*token, l *layout) string {
	var lines []string
	var cur strings.Builder
	started := false

	// lineIndent is the indentation of the line being built, and stack holds
	// one frame per parenthesis that is still open.
	lineIndent := ""
	var stack []parenFrame

	flush := func() {
		if started {
			lines = append(lines, strings.TrimRight(cur.String(), " \t"))
			cur.Reset()
			started = false
		}
	}

	newline := func(indent string, blank bool) {
		flush()
		if blank {
			lines = append(lines, "")
		}
		cur.WriteString(indent)
		lineIndent = indent
		started = true
	}

	indentFor := func(i int, t *token) string {
		if l.body[i] {
			return t.indent
		}
		if len(stack) == 0 {
			// A routine's clauses hang off its CREATE line, the way dump
			// writes them. Everything else keeps the indentation it had.
			if l.routineCon[i] && t.orig != ")" {
				return indentUnit
			}

			return t.indent
		}
		if t.orig == ")" {
			return stack[len(stack)-1].openLine
		}

		return stack[len(stack)-1].content
	}

	// tailIndent is set once a definition list has been closed: whatever
	// follows the closing parenthesis, an INHERITS or a PARTITION BY, opens a
	// line of its own, lined up with that parenthesis. The semicolon that ends
	// the statement stays where it is.
	var tailIndent *string

	// pending marks a break owed to the next token: after a line comment,
	// which would otherwise swallow it, and after the comma that ends an
	// element of a definition list.
	pending := false
	var prev *token

	for i, t := range toks {
		r := l.regions[i]
		inList := r != nil && i > r.open && i <= r.close
		listElem := inList && l.depth[i] == l.depth[r.open]+1
		// owed marks the comma that still owes the break its element ends
		// with. A comma that had to open a line of its own does not: the
		// element after it follows on that line instead.
		owed := listElem && t.orig == ","

		switch {
		case !started:
			newline(indentFor(i, t), false)
			pending = false

		case listElem && t.orig == ",":
			// The comma belongs to the element it ends, whichever line the
			// input put it on, unless that line ends in a comment, which
			// would swallow it.
			if pending {
				newline(indentFor(i, t), false)
				pending = false
				owed = false
			}

		case inList:
			sameLine := t.isComment() && t.nl == 0 && i != r.close
			if (pending || i == r.open+1 || i == r.close || t.nl > 0) && !sameLine {
				newline(indentFor(i, t), t.nl >= 2)
				pending = false
			} else if needsSpace(prev, t, l.body[i-1], l.body[i]) {
				cur.WriteString(" ")
			}

		case tailIndent != nil && t.orig != ";" && (!t.isComment() || t.nl > 0):
			newline(*tailIndent, t.nl >= 2)
			pending = false

		default:
			if pending || t.nl > 0 || l.stmtStart[i] {
				newline(indentFor(i, t), t.nl >= 2)
				pending = false
			} else if needsSpace(prev, t, l.body[i-1], l.body[i]) {
				cur.WriteString(" ")
			}
		}

		cur.WriteString(t.out)
		started = true

		switch t.orig {
		case "(":
			stack = append(stack, parenFrame{openLine: lineIndent, content: lineIndent + indentUnit})
		case ")":
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}

		switch {
		case inList && i == r.close:
			tail := lineIndent
			tailIndent = &tail
		case t.isComment() && t.nl == 0:
			// A comment left on the line does not answer the break the
			// closing parenthesis owes.
		default:
			tailIndent = nil
		}

		if t.isLineComment() || owed {
			pending = true
		}
		prev = t
	}

	flush()

	if len(lines) == 0 {
		return ""
	}

	return strings.Join(lines, "\n") + "\n"
}

// needsSpace decides the separator between two tokens that share a line. The
// whitespace the input has is kept, collapsed to one space, and dropped before
// a comma and a semicolon and just inside a parenthesis, which is where
// pg_dump closes them up as well. Two tokens written with nothing between them
// stay that way unless one of them lost its quotes and the two would now read
// as one token.
//
// A view body keeps the spacing PostgreSQL gave it, so a dump reads back
// unchanged.
func needsSpace(prev, cur *token, prevBody, curBody bool) bool {
	if cur.out == "," || cur.out == ";" {
		return false
	}
	if closesUp(prev, cur) && !prevBody && !curBody && joins(prev.out, cur.out) {
		return false
	}
	if cur.space {
		return true
	}
	if prev == nil || (prev.out == prev.orig && cur.out == cur.orig) {
		return false
	}

	return !joins(prev.out, cur.out)
}

// closesUp reports a pair that carries no space in the way PostgreSQL writes
// it: just inside a parenthesis, around the square brackets of an array type
// or a subscript, and on either side of a cast. Square brackets and "::" have
// no other use, so unlike a parenthesis, which follows a keyword as often as a
// name, they take one rule each.
//
// A comment is left where it is; closing the space up in front of one would
// not change what it comments out, but it reads as part of the comment.
func closesUp(prev, cur *token) bool {
	if prev == nil || prev.isComment() || cur.isComment() {
		return false
	}

	switch {
	case prev.orig == "(", cur.orig == ")":
		return true
	case prev.orig == "[", cur.orig == "[", cur.orig == "]":
		return true
	case prev.orig == "::", cur.orig == "::":
		return true
	}

	return false
}

// joins reports whether two adjacent tokens still read as those two tokens
// when nothing separates them.
func joins(a, b string) bool {
	result, err := pg_query.Scan(a + b)
	if err != nil || len(result.Tokens) != 2 {
		return false
	}

	text := a + b
	first := text[result.Tokens[0].Start:result.Tokens[0].End]
	second := text[result.Tokens[1].Start:result.Tokens[1].End]

	return first == a && second == b
}

// verify reports a formatted result that does not carry the tokens the input
// carried. Nothing is written when it fails, so a bug in the layout cannot
// damage a schema file.
func verify(toks []*token, out string) error {
	result, err := pg_query.Scan(out)
	if err != nil {
		return fmt.Errorf("failed to scan the formatted SQL: %w", err)
	}

	if len(result.Tokens) != len(toks) {
		return fmt.Errorf("formatting changed the token count: %d -> %d", len(toks), len(result.Tokens))
	}

	for i, t := range result.Tokens {
		got := out[t.Start:t.End]
		// An identifier that lost its quotes scans as the keyword it spells,
		// so only a token that was left alone is compared by kind.
		if got != toks[i].out || (toks[i].out == toks[i].orig && t.Token != toks[i].kind) {
			return fmt.Errorf("formatting changed a token: %q -> %q", toks[i].out, got)
		}
	}

	return nil
}
