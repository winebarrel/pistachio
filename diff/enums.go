package diff

import (
	"slices"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func DiffEnums(current, desired *orderedmap.Map[string, *model.Enum]) []string {
	var stmts []string

	// New enums
	for k, desiredEnum := range desired.All() {
		if _, ok := current.GetOk(k); !ok {
			stmts = append(stmts, desiredEnum.SQL())
			if commentSQL := desiredEnum.CommentSQL(); commentSQL != "" {
				stmts = append(stmts, commentSQL)
			}
		}
	}

	// Modified enums (add new values, comment changes)
	for k, desiredEnum := range desired.All() {
		currentEnum, ok := current.GetOk(k)
		if !ok {
			continue
		}

		// Add new enum values
		// Note: PostgreSQL only supports adding values, not removing or reordering
		for _, val := range desiredEnum.Values {
			if !slices.Contains(currentEnum.Values, val) {
				stmts = append(stmts, "ALTER TYPE "+k+" ADD VALUE "+model.QuoteLiteral(val)+";")
			}
		}

		// Comment changes
		if !equalPtr(currentEnum.Comment, desiredEnum.Comment) {
			if desiredEnum.Comment != nil {
				stmts = append(stmts, "COMMENT ON TYPE "+k+" IS "+model.QuoteLiteral(*desiredEnum.Comment)+";")
			} else {
				stmts = append(stmts, "COMMENT ON TYPE "+k+" IS NULL;")
			}
		}
	}

	// Dropped enums
	for k := range current.Keys() {
		if _, ok := desired.GetOk(k); !ok {
			stmts = append(stmts, "DROP TYPE "+k+";")
		}
	}

	return stmts
}
