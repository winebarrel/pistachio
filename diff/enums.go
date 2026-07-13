package diff

import (
	"fmt"
	"slices"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

type EnumDiffResult struct {
	Stmts               []string
	DropStmts           []string
	DisallowedDropStmts []string
}

func DiffEnums(current, desired *orderedmap.Map[string, *model.Enum], dc DropChecker) (*EnumDiffResult, error) {
	dc = normalizeDropChecker(dc)
	result := &EnumDiffResult{}

	// Detect renames
	renameStmts, current, err := detectEnumRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, renameStmts...)

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

		stmts, err := diffEnumValues(k, currentEnum.Values, desiredEnum.Values, desiredEnum.ValueRenameFrom)
		if err != nil {
			return nil, err
		}
		result.Stmts = append(result.Stmts, stmts...)

		// Comment changes
		if !equalPtr(currentEnum.Comment, desiredEnum.Comment) {
			if desiredEnum.Comment != nil {
				result.Stmts = append(result.Stmts, "COMMENT ON TYPE "+k+" IS "+model.QuoteLiteral(*desiredEnum.Comment)+";")
			} else {
				result.Stmts = append(result.Stmts, "COMMENT ON TYPE "+k+" IS NULL;")
			}
		}
	}

	// Dropped enums. When the enum-drop policy disallows it, emit a commented DROP.
	enumAllowed := dc.IsDropAllowed("enum")
	for k := range current.Keys() {
		if _, ok := desired.GetOk(k); !ok {
			if enumAllowed {
				result.DropStmts = append(result.DropStmts, "DROP TYPE "+k+";")
			} else {
				result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: DROP TYPE "+k+";")
			}
		}
	}

	return result, nil
}

func diffEnumValues(fqen string, current, desired []string, renameFrom map[string]string) ([]string, error) {
	var stmts []string

	// Apply value renames first so renamed values are not seen as removals.
	// RENAME VALUE keeps the value's position, so the rename is reflected
	// in-place on the current side.
	if len(renameFrom) > 0 {
		current = slices.Clone(current)
		for _, val := range desired {
			old, ok := renameFrom[val]
			if !ok || old == val {
				continue
			}
			oldIdx := slices.Index(current, old)
			if oldIdx < 0 {
				if slices.Contains(current, val) {
					// Already applied
					continue
				}
				return nil, fmt.Errorf("rename source %s not found for enum value %s in %s", model.QuoteLiteral(old), model.QuoteLiteral(val), fqen)
			}
			if slices.Contains(current, val) {
				return nil, fmt.Errorf("cannot rename enum value %s to %s in %s: destination already exists", model.QuoteLiteral(old), model.QuoteLiteral(val), fqen)
			}
			stmts = append(stmts, "ALTER TYPE "+fqen+" RENAME VALUE "+model.QuoteLiteral(old)+" TO "+model.QuoteLiteral(val)+";")
			current[oldIdx] = val
		}
	}

	// Detect removed values (not supported by PostgreSQL)
	for _, val := range current {
		if !slices.Contains(desired, val) {
			return nil, fmt.Errorf("cannot remove enum value %s from %s: PostgreSQL does not support removing enum values", model.QuoteLiteral(val), fqen)
		}
	}

	// Detect reordering (not supported by PostgreSQL)
	if isReordered(current, desired) {
		return nil, fmt.Errorf("cannot reorder enum values in %s: PostgreSQL does not support reordering enum values", fqen)
	}

	// Add new enum values with correct positioning.
	// Maintain a working slice that tracks values added so far,
	// so later insertions can reference previously added values.
	working := slices.Clone(current)

	for i, val := range desired {
		if slices.Contains(working, val) {
			continue
		}

		stmt := "ALTER TYPE " + fqen + " ADD VALUE " + model.QuoteLiteral(val)

		after := findPrecedingExisting(desired, i, working)
		if after != "" {
			stmt += " AFTER " + model.QuoteLiteral(after)
			// Insert into working after the anchor
			idx := slices.Index(working, after)
			working = slices.Insert(working, idx+1, val)
		} else {
			before := findFollowingExisting(desired, i, working)
			if before != "" {
				stmt += " BEFORE " + model.QuoteLiteral(before)
				idx := slices.Index(working, before)
				working = slices.Insert(working, idx, val)
			} else {
				working = append(working, val)
			}
		}

		stmts = append(stmts, stmt+";")
	}

	return stmts, nil
}

// isReordered checks if existing values in current appear in a different
// relative order in desired. New values in desired are ignored.
func isReordered(current, desired []string) bool {
	var desiredExisting []string
	for _, v := range desired {
		if slices.Contains(current, v) {
			desiredExisting = append(desiredExisting, v)
		}
	}
	return !slices.Equal(current, desiredExisting)
}

// findPrecedingExisting finds the closest value before index i in desired that exists in working.
func findPrecedingExisting(desired []string, i int, working []string) string {
	for j := i - 1; j >= 0; j-- {
		if slices.Contains(working, desired[j]) {
			return desired[j]
		}
	}
	return ""
}

// findFollowingExisting finds the closest value after index i in desired that exists in working.
func findFollowingExisting(desired []string, i int, working []string) string {
	for j := i + 1; j < len(desired); j++ {
		if slices.Contains(working, desired[j]) {
			return desired[j]
		}
	}
	return ""
}
