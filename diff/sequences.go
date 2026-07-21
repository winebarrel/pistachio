package diff

import (
	"strconv"
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

type SequenceDiffResult struct {
	Stmts               []string
	DropStmts           []string
	DisallowedDropStmts []string
}

func DiffSequences(current, desired *orderedmap.Map[string, *model.Sequence], dc DropChecker) (*SequenceDiffResult, error) {
	dc = normalizeDropChecker(dc)
	result := &SequenceDiffResult{}

	// Detect renames
	renameStmts, current, err := detectSequenceRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, renameStmts...)

	// New sequences
	for k, desiredSeq := range desired.All() {
		if _, ok := current.GetOk(k); !ok {
			result.Stmts = append(result.Stmts, desiredSeq.SQL())
			if commentSQL := desiredSeq.CommentSQL(); commentSQL != "" {
				result.Stmts = append(result.Stmts, commentSQL)
			}
		}
	}

	// Modified sequences
	for k, desiredSeq := range desired.All() {
		currentSeq, ok := current.GetOk(k)
		if !ok {
			continue
		}
		result.Stmts = append(result.Stmts, diffSequence(k, currentSeq, desiredSeq)...)
	}

	// Dropped sequences. When the sequence-drop policy disallows it, emit a commented DROP.
	seqAllowed := dc.IsDropAllowed("sequence")
	for k := range current.Keys() {
		if _, ok := desired.GetOk(k); !ok {
			if seqAllowed {
				result.DropStmts = append(result.DropStmts, "DROP SEQUENCE "+k+";")
			} else {
				result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: DROP SEQUENCE "+k+";")
			}
		}
	}

	return result, nil
}

// diffSequence emits a single ALTER SEQUENCE combining every changed option,
// plus a separate COMMENT statement when the comment changed.
func diffSequence(fqn string, current, desired *model.Sequence) []string {
	var clauses []string

	if current.DataType != desired.DataType {
		clauses = append(clauses, "AS "+desired.DataType)
	}
	if current.Increment != desired.Increment {
		clauses = append(clauses, "INCREMENT BY "+strconv.FormatInt(desired.Increment, 10))
	}
	if current.Min != desired.Min {
		clauses = append(clauses, "MINVALUE "+strconv.FormatInt(desired.Min, 10))
	}
	if current.Max != desired.Max {
		clauses = append(clauses, "MAXVALUE "+strconv.FormatInt(desired.Max, 10))
	}
	if current.Start != desired.Start {
		clauses = append(clauses, "START WITH "+strconv.FormatInt(desired.Start, 10))
	}
	if current.Cache != desired.Cache {
		clauses = append(clauses, "CACHE "+strconv.FormatInt(desired.Cache, 10))
	}
	if current.Cycle != desired.Cycle {
		if desired.Cycle {
			clauses = append(clauses, "CYCLE")
		} else {
			clauses = append(clauses, "NO CYCLE")
		}
	}

	var stmts []string
	if len(clauses) > 0 {
		stmts = append(stmts, "ALTER SEQUENCE "+fqn+" "+strings.Join(clauses, " ")+";")
	}

	if !equalPtr(current.Comment, desired.Comment) {
		if desired.Comment != nil {
			stmts = append(stmts, "COMMENT ON SEQUENCE "+fqn+" IS "+model.QuoteLiteral(*desired.Comment)+";")
		} else {
			stmts = append(stmts, "COMMENT ON SEQUENCE "+fqn+" IS NULL;")
		}
	}

	return stmts
}
