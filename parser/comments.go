package parser

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/pistachio/model"
)

// CodeStart returns the offset of the first non-whitespace, non-comment
// character at or after pos in sql. pg_query's StmtLocation lumps preceding
// comments with the statement, so we use this to find the real start of the
// SQL code (e.g. the "C" of "CREATE").
func CodeStart(sql string, pos int32) int32 {
	end := int32(len(sql))
	i := pos
	for i < end {
		switch {
		case sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r':
			i++
		case i+1 < end && sql[i] == '-' && sql[i+1] == '-':
			for i < end && sql[i] != '\n' {
				i++
			}
		case i+1 < end && sql[i] == '/' && sql[i+1] == '*':
			i += 2
			for i+1 < end && (sql[i] != '*' || sql[i+1] != '/') {
				i++
			}
			if i+1 < end {
				i += 2
			}
		default:
			return i
		}
	}
	return i
}

// filterTopLevelComments drops comments whose byte range falls inside any
// parsed statement's actual code region (after leading comments are skipped).
// Such comments (e.g. inline column comments) cannot be safely relocated.
func filterTopLevelComments(sql string, comments []Comment, stmts []*pg_query.RawStmt) []Comment {
	if len(comments) == 0 || len(stmts) == 0 {
		return comments
	}
	type span struct{ start, end int32 }
	spans := make([]span, 0, len(stmts))
	for _, s := range stmts {
		if s.StmtLen == 0 {
			continue
		}
		spans = append(spans, span{
			start: CodeStart(sql, s.StmtLocation),
			end:   s.StmtLocation + s.StmtLen,
		})
	}
	out := make([]Comment, 0, len(comments))
	for _, c := range comments {
		inside := false
		for _, sp := range spans {
			if c.Start >= sp.start && c.End <= sp.end {
				inside = true
				break
			}
		}
		if !inside {
			out = append(out, c)
		}
	}
	return out
}

type Comment struct {
	Start int32
	End   int32
	Text  string
}

// ScanComments returns SQL line comments (--) and C-style block comments (/* */)
// from sql, in source order, with byte offsets into sql.
// Comments matching pistachio directives (-- pist:...) are filtered out.
func ScanComments(sql string) ([]Comment, error) {
	scan, err := pg_query.Scan(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to scan SQL for comments: %w", err)
	}

	var out []Comment
	for _, tok := range scan.Tokens {
		if tok.Token != pg_query.Token_SQL_COMMENT && tok.Token != pg_query.Token_C_COMMENT {
			continue
		}
		start, end := tok.Start, tok.End
		if start < 0 || end > int32(len(sql)) || start >= end {
			continue
		}
		text := sql[start:end]
		if isDirectiveComment(text) {
			continue
		}
		out = append(out, Comment{Start: start, End: end, Text: text})
	}
	return out, nil
}

func isDirectiveComment(text string) bool {
	t := strings.TrimSpace(text)
	if !strings.HasPrefix(t, "--") {
		return false
	}
	t = strings.TrimSpace(strings.TrimPrefix(t, "--"))
	return strings.HasPrefix(t, "pist:")
}

// commentStmtTargetFQN returns the FQN of the entity targeted by a COMMENT ON
// statement. For COMMENT ON COLUMN, the FQN of the parent table is returned.
// The second return value is false if the objtype is not supported.
func commentStmtTargetFQN(cs *pg_query.CommentStmt, defaultSchema string) (string, bool) {
	var names []string
	switch cs.Objtype {
	case pg_query.ObjectType_OBJECT_TYPE, pg_query.ObjectType_OBJECT_DOMAIN:
		names = stringSvalNames(cs.Object.GetTypeName().GetNames())
	case pg_query.ObjectType_OBJECT_TABLE,
		pg_query.ObjectType_OBJECT_VIEW,
		pg_query.ObjectType_OBJECT_MATVIEW:
		names = stringSvalNames(cs.Object.GetList().GetItems())
	case pg_query.ObjectType_OBJECT_COLUMN:
		// names: [table, col] or [schema, table, col]; drop the column.
		items := stringSvalNames(cs.Object.GetList().GetItems())
		if len(items) >= 2 {
			names = items[:len(items)-1]
		}
	default:
		return "", false
	}
	if len(names) == 0 {
		return "", false
	}
	return identFromQualified(names, defaultSchema), true
}

func stringSvalNames(nodes []*pg_query.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if s := n.GetString_(); s != nil {
			out = append(out, s.Sval)
		}
	}
	return out
}

// identFromQualified returns model.Ident(schema, name) given names of length 1
// (uses defaultSchema) or 2 (schema-qualified).
func identFromQualified(names []string, defaultSchema string) string {
	schema := defaultSchema
	name := names[0]
	if len(names) >= 2 {
		schema = names[0]
		name = names[1]
	}
	return model.Ident(schema, name)
}
