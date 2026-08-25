package diff

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

type RoutineDiffResult struct {
	Stmts               []string
	DropStmts           []string
	DisallowedDropStmts []string
}

// DiffRoutines compares functions and procedures. Both maps are keyed by FQRN,
// which carries the identity argument types, so an overload set appears as
// several independent objects and a change to an argument type reads as a drop
// plus a create rather than a modification.
func DiffRoutines(current, desired *orderedmap.Map[string, *model.Routine], dc DropChecker) (*RoutineDiffResult, error) {
	dc = normalizeDropChecker(dc)
	result := &RoutineDiffResult{}
	dropAllowed := dc.IsDropAllowed("routine")

	// New routines
	for k, desiredRoutine := range desired.All() {
		if _, ok := current.GetOk(k); ok {
			continue
		}
		result.Stmts = append(result.Stmts, desiredRoutine.SQL())
		if commentSQL := desiredRoutine.CommentSQL(); commentSQL != "" {
			result.Stmts = append(result.Stmts, commentSQL)
		}
	}

	// Modified routines
	for k, desiredRoutine := range desired.All() {
		currentRoutine, ok := current.GetOk(k)
		if !ok {
			continue
		}
		stmts, disallowed := diffRoutine(currentRoutine, desiredRoutine, dropAllowed)
		result.Stmts = append(result.Stmts, stmts...)
		result.DisallowedDropStmts = append(result.DisallowedDropStmts, disallowed...)
	}

	// Dropped routines. When the drop policy disallows it, emit a commented DROP.
	for k, currentRoutine := range current.All() {
		if _, ok := desired.GetOk(k); ok {
			continue
		}
		if dropAllowed {
			result.DropStmts = append(result.DropStmts, currentRoutine.DropSQL())
		} else {
			result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: "+currentRoutine.DropSQL())
		}
	}

	return result, nil
}

// diffRoutine returns the statements that bring one routine in line, plus any
// drop the policy suppressed.
func diffRoutine(current, desired *model.Routine, dropAllowed bool) (stmts, disallowed []string) {
	if current.SQL() != desired.SQL() {
		if needsDropCreate(current, desired) {
			// PostgreSQL rejects CREATE OR REPLACE for this change, so the
			// routine has to go first. Without the drop policy the current
			// definition stays, and the comment is left alone with it.
			if !dropAllowed {
				return nil, []string{"-- skipped: " + current.DropSQL()}
			}
			// A recreated routine loses its comment, so re-apply it.
			stmts = append(stmts, current.DropSQL(), desired.SQL())
			if commentSQL := desired.CommentSQL(); commentSQL != "" {
				stmts = append(stmts, commentSQL)
			}
			return stmts, nil
		}
		stmts = append(stmts, desired.SQL())
	}

	if !equalPtr(current.Comment, desired.Comment) {
		if desired.Comment != nil {
			stmts = append(stmts, desired.CommentSQL())
		} else {
			stmts = append(stmts, "COMMENT ON "+desired.Kind()+" "+desired.FQRN()+" IS NULL;")
		}
	}

	return stmts, nil
}

// needsDropCreate reports whether PostgreSQL refuses to apply the change with
// CREATE OR REPLACE. It rejects a change of routine kind, of return type, and
// of a parameter's name, mode or type, and it refuses to remove a parameter
// default. Everything else - the body, the language, and the attributes -
// replaces in place.
func needsDropCreate(current, desired *model.Routine) bool {
	if current.Procedure != desired.Procedure {
		return true
	}
	if current.ReturnType != desired.ReturnType || current.ReturnsSet != desired.ReturnsSet {
		return true
	}
	if !equalArgShapes(current.Args, desired.Args) {
		return true
	}
	return defaultRemoved(current.Args, desired.Args)
}

// equalArgShapes compares every parameter's mode, name and type. The identity
// types already match (both sides share a key), so this catches a renamed
// parameter and any change to the OUT or RETURNS TABLE parameters, each of
// which PostgreSQL rejects in place.
func equalArgShapes(current, desired []*model.RoutineArg) bool {
	if len(current) != len(desired) {
		return false
	}
	for i, c := range current {
		if argShape(c) != argShape(desired[i]) {
			return false
		}
	}
	return true
}

func argShape(a *model.RoutineArg) string {
	return strings.Join([]string{a.Mode, a.Name, a.Type}, "\x00")
}

// defaultRemoved reports whether a parameter that had a default no longer has
// one. PostgreSQL allows adding or changing a default in place but not
// dropping one. The caller has already established that the two lists have the
// same shape, so they are the same length here.
func defaultRemoved(current, desired []*model.RoutineArg) bool {
	for i, c := range current {
		if c.Default != "" && desired[i].Default == "" {
			return true
		}
	}
	return false
}
