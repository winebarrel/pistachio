package diff

import (
	"slices"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

type EnumDiffResult struct {
	Stmts     []string
	DropStmts []string
}

func DiffEnums(current, desired *orderedmap.Map[string, *model.Enum]) *EnumDiffResult {
	result := &EnumDiffResult{}

	// New enums
	for k, desiredEnum := range desired.All() {
		if _, ok := current.GetOk(k); !ok {
			result.Stmts = append(result.Stmts, desiredEnum.SQL())
			if commentSQL := desiredEnum.CommentSQL(); commentSQL != "" {
				result.Stmts = append(result.Stmts, commentSQL)
			}
		}
	}

	// Modified enums (add new values, comment changes)
	for k, desiredEnum := range desired.All() {
		currentEnum, ok := current.GetOk(k)
		if !ok {
			continue
		}

		// Add new enum values with correct positioning
		for i, val := range desiredEnum.Values {
			if !slices.Contains(currentEnum.Values, val) {
				stmt := "ALTER TYPE " + k + " ADD VALUE " + model.QuoteLiteral(val)
				// Find the preceding value in desired that exists in current for BEFORE/AFTER positioning
				if after := findPrecedingExisting(desiredEnum.Values, i, currentEnum.Values); after != "" {
					stmt += " AFTER " + model.QuoteLiteral(after)
				} else if before := findFollowingExisting(desiredEnum.Values, i, currentEnum.Values); before != "" {
					stmt += " BEFORE " + model.QuoteLiteral(before)
				}
				result.Stmts = append(result.Stmts, stmt+";")
			}
		}

		// Comment changes
		if !equalPtr(currentEnum.Comment, desiredEnum.Comment) {
			if desiredEnum.Comment != nil {
				result.Stmts = append(result.Stmts, "COMMENT ON TYPE "+k+" IS "+model.QuoteLiteral(*desiredEnum.Comment)+";")
			} else {
				result.Stmts = append(result.Stmts, "COMMENT ON TYPE "+k+" IS NULL;")
			}
		}
	}

	// Dropped enums
	for k := range current.Keys() {
		if _, ok := desired.GetOk(k); !ok {
			result.DropStmts = append(result.DropStmts, "DROP TYPE "+k+";")
		}
	}

	return result
}

// findPrecedingExisting finds the closest value before index i in desired that exists in current.
func findPrecedingExisting(desired []string, i int, current []string) string {
	for j := i - 1; j >= 0; j-- {
		if slices.Contains(current, desired[j]) {
			return desired[j]
		}
	}
	return ""
}

// findFollowingExisting finds the closest value after index i in desired that exists in current.
func findFollowingExisting(desired []string, i int, current []string) string {
	for j := i + 1; j < len(desired); j++ {
		if slices.Contains(current, desired[j]) {
			return desired[j]
		}
	}
	return ""
}
