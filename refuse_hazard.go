package pistachio

import (
	"fmt"
	"strings"
)

// --refuse-hazard refuses a plan that holds a statement of a kind the user
// names, so a pipeline can stop a change that rewrites a large table or locks
// it against reads before it runs. The kinds are the classification
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

// HazardRefusedError is returned when --refuse-hazard refuses the plan. The
// CLI exits with code 3 on it. Stmts holds the refused statements with their
// comments, for apply, which writes no plan to show them in.
type HazardRefusedError struct {
	Count int
	Kinds []string
	Stmts string
}

func (e *HazardRefusedError) Error() string {
	msg := fmt.Sprintf("%s refused by --refuse-hazard=%s", pluralize(e.Count, "statement"), strings.Join(e.Kinds, ","))
	if e.Stmts != "" {
		msg += ":\n" + e.Stmts
	}
	return msg
}
