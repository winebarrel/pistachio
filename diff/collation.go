package diff

import "github.com/winebarrel/pistachio/model"

// equalCollation reports whether two COLLATE clauses name the same collation.
// The catalog always reports a collation schema-qualified, while the desired
// schema may leave it unqualified, so the schema is compared only when both
// sides carry one. Splitting respects quoting, because a collation name can
// contain a dot ("C.utf8", "en_US.utf8").
func equalCollation(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}

	pa := model.SplitQualifiedName(*a)
	pb := model.SplitQualifiedName(*b)
	if len(pa) == 0 || len(pb) == 0 {
		return *a == *b
	}

	if pa[len(pa)-1] != pb[len(pb)-1] {
		return false
	}
	if len(pa) > 1 && len(pb) > 1 {
		return pa[len(pa)-2] == pb[len(pb)-2]
	}
	return true
}
