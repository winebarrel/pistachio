package pistachio

import (
	"fmt"
	"strings"
)

// --refuse-hazard refuses a plan that holds a statement of a kind the user
// names, so a pipeline can stop a change that rewrites a large table or locks
// it against reads before it runs. -- pista:refuse-hazard on a CREATE TABLE
// does the same for a statement on that table or on one under it, a
// partition or an INHERITS child, and the two add up. The kinds are the classification
// --explain writes, so the comment on a statement says why it was refused.
// The whole plan is refused rather than the statement alone: a statement
// after it may depend on it, as an ADD CONSTRAINT ... USING INDEX does on the
// index build before it.

// hazardSet holds the kinds --refuse-hazard names. A nil set matches nothing.
type hazardSet []string

// matches reports whether the effect is of one of the kinds. may rewrite is
// not a rewrite: it is what plan writes when the server cannot tell yet, and
// its lock is still caught by blocks-reads.
func (s hazardSet) matches(eff explainEffect) bool {
	for _, kind := range s {
		switch kind {
		case "rewrite":
			if eff.touch == touchRewrite {
				return true
			}
		case "scan":
			if eff.touch == touchScan {
				return true
			}
		case "blocks-reads":
			if eff.block >= blockAll {
				return true
			}
		case "blocks-writes":
			if eff.block >= blockWrites {
				return true
			}
		}
	}
	return false
}

// refusedBy says what refused a plan: --refuse-hazard, a
// -- pista:refuse-hazard on a table, or both.
type refusedBy struct {
	flag      bool
	directive bool
}

// HazardRefusedError is returned when --refuse-hazard or a
// -- pista:refuse-hazard refuses the plan. The CLI exits with code 3 on it.
// Kinds are those of --refuse-hazard, set when the flag refused a statement,
// and Directive is set when a directive did. Stmts holds the refused
// statements with their comments, for apply, which writes no plan to show
// them in.
type HazardRefusedError struct {
	Count     int
	Kinds     []string
	Directive bool
	Stmts     string
}

// newHazardRefusedError returns the error for a plan with count refused
// statements, or nil when there are none.
func newHazardRefusedError(count int, kinds []string, by refusedBy, stmts string) *HazardRefusedError {
	if count == 0 {
		return nil
	}
	e := &HazardRefusedError{Count: count, Directive: by.directive, Stmts: stmts}
	if by.flag {
		e.Kinds = kinds
	}
	return e
}

func (e *HazardRefusedError) Error() string {
	var by []string
	if len(e.Kinds) > 0 {
		by = append(by, "--refuse-hazard="+strings.Join(e.Kinds, ","))
	}
	if e.Directive {
		by = append(by, "-- pista:refuse-hazard")
	}
	msg := fmt.Sprintf("%s refused by %s", pluralize(e.Count, "statement"), strings.Join(by, " and "))
	if e.Stmts != "" {
		msg += ":\n" + e.Stmts
	}
	return msg
}
