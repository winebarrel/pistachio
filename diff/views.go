package diff

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// normalizeViewDef normalizes a view definition by parsing and deparsing it
// through pg_query, so that formatting differences are eliminated.
// It also strips implicit ::text casts on string literals that pg_get_viewdef adds.
func normalizeViewDef(def string) (string, error) {
	sql := "CREATE VIEW _v AS " + def
	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", err
	}
	for _, stmt := range result.Stmts {
		stripImplicitTextCastsFromNode(stmt.Stmt)
	}
	return pg_query.Deparse(result)
}

// isImplicitTextCast returns true if the TypeCast is a ::text cast on a string literal.
// pg_get_viewdef adds these casts implicitly; they are semantically redundant.
func isImplicitTextCast(tc *pg_query.TypeCast) bool {
	if tc == nil || tc.Arg == nil || tc.TypeName == nil {
		return false
	}
	if c := tc.Arg.GetAConst(); c == nil || c.GetSval() == nil {
		return false
	}
	for _, n := range tc.TypeName.Names {
		if s := n.GetString_(); s != nil && s.Sval == "text" {
			return true
		}
	}
	return false
}

var nodeFullName = (&pg_query.Node{}).ProtoReflect().Descriptor().FullName()

// stripImplicitTextCastsFromNode unwraps ::text casts on string literals in the given node.
func stripImplicitTextCastsFromNode(node *pg_query.Node) {
	if node == nil {
		return
	}
	if tc := node.GetTypeCast(); isImplicitTextCast(tc) {
		node.Node = tc.Arg.Node
		stripImplicitTextCastsFromNode(node)
		return
	}
	walkNodeChildren(node.ProtoReflect())
}

// walkNodeChildren recursively visits all child Node fields via protobuf reflection.
func walkNodeChildren(msg protoreflect.Message) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind {
			return true
		}
		if fd.IsList() {
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				m := list.Get(i).Message()
				if m.Descriptor().FullName() == nodeFullName {
					stripImplicitTextCastsFromNode(m.Interface().(*pg_query.Node))
				} else {
					walkNodeChildren(m)
				}
			}
		} else {
			m := v.Message()
			if m.Descriptor().FullName() == nodeFullName {
				stripImplicitTextCastsFromNode(m.Interface().(*pg_query.Node))
			} else {
				walkNodeChildren(m)
			}
		}
		return true
	})
}

// equalViewDef compares two view definitions by normalizing them through
// pg_query parse/deparse to ignore formatting differences.
// proto.Equal cannot be used here because parse trees include source
// location information that differs when the same query has different formatting.
func equalViewDef(a, b string) bool {
	if a == b {
		return true
	}
	normA, errA := normalizeViewDef(a)
	normB, errB := normalizeViewDef(b)
	if errA != nil || errB != nil {
		return a == b
	}
	return normA == normB
}

// ViewDiffResult separates view DROP and CREATE/MODIFY statements.
// Drops should run before table changes, creates after.
type ViewDiffResult struct {
	DropStmts   []string // DROP VIEW (should run before table changes)
	CreateStmts []string // ALTER VIEW RENAME, CREATE OR REPLACE VIEW, comments (should run after table changes)
}

func DiffViews(current, desired *orderedmap.Map[string, *model.View], dc DropChecker) (*ViewDiffResult, error) {
	dc = NormalizeDropChecker(dc)
	result := &ViewDiffResult{}

	// Detect renames
	renameStmts, current, err := detectViewRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.CreateStmts = append(result.CreateStmts, renameStmts...)

	// New or modified views (CREATE OR REPLACE)
	for k, desiredView := range desired.All() {
		currentView, ok := current.GetOk(k)
		if !ok || !equalViewDef(currentView.Definition, desiredView.Definition) {
			result.CreateStmts = append(result.CreateStmts, desiredView.SQL())
		}
	}

	// Dropped views
	if dc.IsDropAllowed("view") {
		for k := range current.Keys() {
			if _, ok := desired.GetOk(k); !ok {
				result.DropStmts = append(result.DropStmts, "DROP VIEW "+k+";")
			}
		}
	}

	// Comment changes
	for k, desiredView := range desired.All() {
		currentView, ok := current.GetOk(k)
		var currentComment *string
		if ok {
			currentComment = currentView.Comment
		}
		if !equalPtr(currentComment, desiredView.Comment) {
			if desiredView.Comment != nil {
				result.CreateStmts = append(result.CreateStmts, "COMMENT ON VIEW "+k+" IS "+model.QuoteLiteral(*desiredView.Comment)+";")
			} else {
				result.CreateStmts = append(result.CreateStmts, "COMMENT ON VIEW "+k+" IS NULL;")
			}
		}
	}

	return result, nil
}
