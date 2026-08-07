package pgast

import (
	"strings"
)

// CanonicalJSONPath rewrites a jsonpath literal into the spelling PostgreSQL
// stores, and reports whether it recognised the whole path.
//
// PostgreSQL parses the argument of JSON_VALUE, JSON_QUERY and JSON_EXISTS
// into a jsonpath value and prints its canonical form back, so a definition
// written as '$.x' comes back from the catalog as '$."x"' and the two never
// compare equal. To pg_query the argument is a plain string constant, so the
// canonical form has to be produced here. Observed identical on 15 through 18:
//
//	$.x         -> $."x"
//	$.x.y[0]    -> $."x"."y"[0]
//	$.a[*].b    -> $."a"[*]."b"
//	lax $.x     -> $."x"          the default mode is not printed
//	strict $.x  -> strict $."x"
//	$x          -> $"x"
//	$[0 to 2]   -> $[0 to 2]      subscripts, wildcards and methods pass through
//
// A path holding a filter, arithmetic or a comparison is printed back with its
// own spacing and parentheses ('$.x + 1' becomes '($."x" + 1)'), which needs a
// jsonpath expression parser rather than a lexer. Those return ok=false and the
// caller leaves the literal alone, which is the behaviour that was there
// before. Bailing out matters more than covering them: a canonicaliser that
// lost information would report two different paths as equal, which is worse
// than the drift it set out to fix.
func CanonicalJSONPath(s string) (string, bool) {
	p := &jsonPathParser{in: s}
	out, ok := p.parse()
	if !ok {
		return s, false
	}
	return out, true
}

type jsonPathParser struct {
	in  string
	pos int
	out strings.Builder
}

func (p *jsonPathParser) parse() (string, bool) {
	p.skipSpace()
	// The mode keyword is optional and only lax is dropped, being the default.
	switch {
	case p.acceptWord("strict"):
		p.out.WriteString("strict ")
	case p.acceptWord("lax"):
	}
	p.skipSpace()

	if !p.accept('$') {
		return "", false
	}
	p.out.WriteByte('$')
	// $ on its own is the whole document; $name is a variable, which the server
	// prints quoted.
	if name, ok := p.readKey(); ok {
		p.writeQuoted(name)
	}

	for {
		p.skipSpace()
		if p.pos >= len(p.in) {
			return p.out.String(), true
		}
		if !p.accessor() {
			return "", false
		}
	}
}

// accessor consumes one member, wildcard, method or subscript step.
func (p *jsonPathParser) accessor() bool {
	switch {
	case p.accept('.'):
		return p.memberAccessor()
	case p.accept('['):
		return p.subscript()
	}
	return false
}

func (p *jsonPathParser) memberAccessor() bool {
	if p.acceptString("**") {
		// Recursive wildcard, optionally with a depth range: $.**{2}
		p.out.WriteString(".**")
		if p.accept('{') {
			body, ok := p.readUntil('}')
			if !ok {
				return false
			}
			p.out.WriteByte('{')
			p.out.WriteString(normalizeSpace(body))
			p.out.WriteByte('}')
		}
		return true
	}
	if p.accept('*') {
		p.out.WriteString(".*")
		return true
	}
	name, ok := p.readKey()
	if !ok {
		return false
	}
	// A name followed by () is a method (.size(), .type(), .abs(), ...), which
	// the server prints unquoted. One taking an argument, .datetime("FMT"),
	// is left to the caller.
	if p.accept('(') {
		if !p.accept(')') {
			return false
		}
		p.out.WriteByte('.')
		p.out.WriteString(name)
		p.out.WriteString("()")
		return true
	}
	p.out.WriteByte('.')
	p.writeQuoted(name)
	return true
}

// subscript consumes the contents of a [...] step. Only the forms whose
// spelling the server preserves are accepted: a wildcard, an integer, the
// keyword last, and a range between two of those, in a comma-separated list.
// An index that is itself an expression ($[$n], $[1 + 1]) is not.
func (p *jsonPathParser) subscript() bool {
	body, ok := p.readUntil(']')
	if !ok {
		return false
	}
	body = strings.TrimSpace(body)
	if body == "*" {
		p.out.WriteString("[*]")
		return true
	}
	var parts []string
	for item := range strings.SplitSeq(body, ",") {
		bounds := strings.Split(normalizeSpace(strings.TrimSpace(item)), " to ")
		if len(bounds) > 2 {
			return false
		}
		for _, b := range bounds {
			if !isSubscriptBound(b) {
				return false
			}
		}
		parts = append(parts, strings.Join(bounds, " to "))
	}
	p.out.WriteByte('[')
	p.out.WriteString(strings.Join(parts, ","))
	p.out.WriteByte(']')
	return true
}

func isSubscriptBound(s string) bool {
	if s == "last" {
		return true
	}
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// readKey reads a member name, either bare or double-quoted, and returns it
// unescaped. A quoted name carrying a backslash escape is refused rather than
// decoded: the escapes are rare enough that re-emitting them exactly is not
// worth the risk of getting one wrong.
func (p *jsonPathParser) readKey() (string, bool) {
	if p.pos >= len(p.in) {
		return "", false
	}
	if p.in[p.pos] == '"' {
		p.pos++
		body, ok := p.readUntil('"')
		if !ok || strings.ContainsRune(body, '\\') {
			return "", false
		}
		return body, true
	}
	start := p.pos
	for p.pos < len(p.in) && isKeyByte(p.in[p.pos], p.pos == start) {
		p.pos++
	}
	if p.pos == start {
		return "", false
	}
	return p.in[start:p.pos], true
}

func isKeyByte(c byte, first bool) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_', c >= 0x80:
		return true
	case first:
		return false
	}
	return c >= '0' && c <= '9' || c == '$'
}

// writeQuoted emits a member name the way the server does, always quoted.
// Names holding a quote or a backslash never reach here; readKey refuses them.
func (p *jsonPathParser) writeQuoted(name string) {
	p.out.WriteByte('"')
	p.out.WriteString(name)
	p.out.WriteByte('"')
}

func (p *jsonPathParser) skipSpace() {
	for p.pos < len(p.in) && (p.in[p.pos] == ' ' || p.in[p.pos] == '\t' || p.in[p.pos] == '\n' || p.in[p.pos] == '\r') {
		p.pos++
	}
}

func (p *jsonPathParser) accept(c byte) bool {
	if p.pos < len(p.in) && p.in[p.pos] == c {
		p.pos++
		return true
	}
	return false
}

func (p *jsonPathParser) acceptString(s string) bool {
	if strings.HasPrefix(p.in[p.pos:], s) {
		p.pos += len(s)
		return true
	}
	return false
}

// acceptWord matches a keyword only when it is not the start of a longer name.
func (p *jsonPathParser) acceptWord(word string) bool {
	rest := p.in[p.pos:]
	if !strings.HasPrefix(rest, word) {
		return false
	}
	if len(rest) > len(word) && isKeyByte(rest[len(word)], false) {
		return false
	}
	p.pos += len(word)
	return true
}

func (p *jsonPathParser) readUntil(end byte) (string, bool) {
	i := strings.IndexByte(p.in[p.pos:], end)
	if i < 0 {
		return "", false
	}
	body := p.in[p.pos : p.pos+i]
	p.pos += i + 1
	return body, true
}

func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
