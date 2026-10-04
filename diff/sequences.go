package diff

import (
	"strconv"
	"strings"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

type SequenceDiffResult struct {
	Stmts []string
	// OwnedByStmts set the owner. They run after the table statements,
	// because the column must exist.
	OwnedByStmts []string
	// DisownStmts remove the owner. They run before the table statements, so
	// that dropping the table or column does not drop the sequence.
	DisownStmts         []string
	DropStmts           []string
	DisallowedDropStmts []string
}

func DiffSequences(current, desired *orderedmap.Map[string, *model.Sequence], dc DropChecker) (*SequenceDiffResult, error) {
	dc = normalizeDropChecker(dc)
	result := &SequenceDiffResult{}

	original := current

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
			if ownedBySQL := desiredSeq.OwnedBySQL(); ownedBySQL != "" {
				result.OwnedByStmts = append(result.OwnedByStmts, ownedBySQL)
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

		if equalPtr(currentSeq.OwnerTable, desiredSeq.OwnerTable) && equalPtr(currentSeq.OwnerColumn, desiredSeq.OwnerColumn) {
			continue
		}
		if desiredSeq.Owned() {
			result.OwnedByStmts = append(result.OwnedByStmts, desiredSeq.OwnedBySQL())
			continue
		}
		// This runs before the rename, so it uses the old name.
		name := k
		if from := desiredSeq.RenameFrom; from != nil {
			if _, ok := original.GetOk(*from); ok {
				name = *from
			}
		}
		result.DisownStmts = append(result.DisownStmts, "ALTER SEQUENCE "+name+" OWNED BY NONE;")
	}

	// Dropped sequences. When the sequence-drop policy disallows it, emit a commented DROP.
	drops, skipped := dropMissing(current, desired, "SEQUENCE", dc.IsDropAllowed("sequence"))
	result.DropStmts = append(result.DropStmts, drops...)
	result.DisallowedDropStmts = append(result.DisallowedDropStmts, skipped...)

	return result, nil
}

// diffSequence emits a single ALTER SEQUENCE combining every changed option,
// plus a separate COMMENT statement when the comment changed.
func diffSequence(fqn string, current, desired *model.Sequence) []string {
	var stmts []string

	// SET LOGGED / SET UNLOGGED is a form of its own; the option list of
	// ALTER SEQUENCE does not take it.
	if current.Unlogged != desired.Unlogged {
		if desired.Unlogged {
			stmts = append(stmts, "ALTER SEQUENCE "+fqn+" SET UNLOGGED;")
		} else {
			stmts = append(stmts, "ALTER SEQUENCE "+fqn+" SET LOGGED;")
		}
	}

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

	if len(clauses) > 0 {
		stmts = append(stmts, "ALTER SEQUENCE "+fqn+" "+strings.Join(clauses, " ")+";")
	}

	if !equalPtr(current.Comment, desired.Comment) {
		stmts = append(stmts, commentOnSQL("SEQUENCE "+fqn, desired.Comment))
	}

	return stmts
}
