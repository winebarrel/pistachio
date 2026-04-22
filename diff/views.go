package diff

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// normalizeViewDef normalizes a view definition by parsing and deparsing it
// through pg_query, so that formatting differences are eliminated.
func normalizeViewDef(def string) (string, error) {
	sql := "CREATE VIEW _v AS " + def
	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", err
	}
	return pg_query.Deparse(result)
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

func DiffViews(current, desired *orderedmap.Map[string, *model.View], dc DropChecker) ([]string, error) {
	dc = NormalizeDropChecker(dc)
	var stmts []string

	// Detect renames
	renameStmts, current, err := detectViewRenames(current, desired)
	if err != nil {
		return nil, err
	}
	stmts = append(stmts, renameStmts...)

	// New or modified views (CREATE OR REPLACE)
	for k, desiredView := range desired.All() {
		currentView, ok := current.GetOk(k)
		if !ok || !equalViewDef(currentView.Definition, desiredView.Definition) {
			stmts = append(stmts, desiredView.SQL())
		}
	}

	// Dropped views
	if dc.IsDropAllowed("view") {
		for k := range current.Keys() {
			if _, ok := desired.GetOk(k); !ok {
				stmts = append(stmts, "DROP VIEW "+k+";")
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
				stmts = append(stmts, "COMMENT ON VIEW "+k+" IS "+model.QuoteLiteral(*desiredView.Comment)+";")
			} else {
				stmts = append(stmts, "COMMENT ON VIEW "+k+" IS NULL;")
			}
		}
	}

	return stmts, nil
}
