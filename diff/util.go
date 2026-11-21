package diff

import "github.com/winebarrel/pistachio/model"

// commentOnSQL returns COMMENT ON target setting comment, or clearing it when
// comment is nil.
func commentOnSQL(target string, comment *string) string {
	if comment == nil {
		return "COMMENT ON " + target + " IS NULL;"
	}
	return "COMMENT ON " + target + " IS " + model.QuoteLiteral(*comment) + ";"
}

func equalPtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
