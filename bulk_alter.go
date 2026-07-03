package pistachio

import "strings"

// mergeAlterTable collapses consecutive `ALTER TABLE <fqtn> <action>;` statements
// that target the same table into a single statement with comma-separated actions.
// shouldMerge decides per table (by fqtn) whether its statements are merged;
// tables it rejects pass through unchanged.
//
// Only a small whitelist of action prefixes is mergeable; everything else
// (renames, VALIDATE CONSTRAINT, RLS toggles, "-- skipped:" comments, non-ALTER
// TABLE statements, and ALTER TABLE ONLY/IF EXISTS variants) flushes the
// current group and is emitted unchanged. Groups also break when the target
// fqtn changes between consecutive statements.
//
// Caller must only invoke this on statements that are sequenced per-table
// (i.e. diff.TableDiffResult.Stmts), since merging relies on the natural
// per-table contiguity that DiffTables produces.
func mergeAlterTable(stmts []string, shouldMerge func(fqtn string) bool) []string {
	result := make([]string, 0, len(stmts))

	var groupFQTN string
	var groupActions []string
	var groupOriginals []string

	flush := func() {
		switch len(groupActions) {
		case 0:
			// nothing to flush
		case 1:
			// Single action: keep the original byte-for-byte to avoid any
			// reformatting drift from rebuilding the statement.
			result = append(result, groupOriginals[0])
		default:
			var b strings.Builder
			b.WriteString("ALTER TABLE ")
			b.WriteString(groupFQTN)
			for i, a := range groupActions {
				b.WriteString("\n  ")
				b.WriteString(a)
				if i < len(groupActions)-1 {
					b.WriteByte(',')
				}
			}
			b.WriteByte(';')
			result = append(result, b.String())
		}
		groupFQTN = ""
		groupActions = nil
		groupOriginals = nil
	}

	for _, stmt := range stmts {
		fqtn, action, ok := parseMergeableAlterTable(stmt)
		if !ok || !shouldMerge(fqtn) {
			flush()
			result = append(result, stmt)
			continue
		}
		if groupFQTN != "" && groupFQTN != fqtn {
			flush()
		}
		groupFQTN = fqtn
		groupActions = append(groupActions, action)
		groupOriginals = append(groupOriginals, stmt)
	}
	flush()
	return result
}

// parseMergeableAlterTable returns (fqtn, action, true) when stmt has the exact
// shape `ALTER TABLE <fqtn> <action>;` AND action's leading keyword is on the
// mergeable whitelist. Anything else (ALTER TABLE ONLY, ALTER TABLE IF EXISTS,
// non-mergeable actions like RENAME/VALIDATE/ENABLE-RLS, or non-ALTER-TABLE
// statements) returns ok=false.
func parseMergeableAlterTable(stmt string) (fqtn, action string, ok bool) {
	const prefix = "ALTER TABLE "
	if !strings.HasPrefix(stmt, prefix) {
		return "", "", false
	}
	rest := stmt[len(prefix):]
	// Reject ALTER TABLE ONLY ... and ALTER TABLE IF EXISTS ...; both reserve
	// non-action semantics that we don't want to silently combine.
	if strings.HasPrefix(rest, "ONLY ") || strings.HasPrefix(rest, "IF EXISTS ") {
		return "", "", false
	}

	fqtn = extractFirstIdentifier(rest)
	if fqtn == "" {
		return "", "", false
	}
	rest = rest[len(fqtn):]
	if !strings.HasPrefix(rest, " ") {
		return "", "", false
	}
	rest = rest[1:]
	if !strings.HasSuffix(rest, ";") {
		return "", "", false
	}
	action = rest[:len(rest)-1]

	if !isMergeableAction(action) {
		return "", "", false
	}
	return fqtn, action, true
}

// isMergeableAction returns true for ALTER TABLE actions that are safe to
// combine with peers under the same target table:
//   - ADD COLUMN / DROP COLUMN
//   - ALTER COLUMN ... (all subforms: SET DATA TYPE, SET/DROP DEFAULT,
//     SET/DROP NOT NULL, IDENTITY transitions)
//   - ADD CONSTRAINT (CHECK / UNIQUE; FK lives in TableDiffResult.FKAddStmts
//     and never reaches mergeAlterTable's input)
//   - DROP CONSTRAINT (same FK note)
//
// Renames (column/constraint), VALIDATE CONSTRAINT, RLS toggles, and any other
// action keyword are intentionally excluded so they pass through untouched.
func isMergeableAction(action string) bool {
	switch {
	case strings.HasPrefix(action, "ADD COLUMN "):
		return true
	case strings.HasPrefix(action, "DROP COLUMN "):
		return true
	case strings.HasPrefix(action, "ALTER COLUMN "):
		return true
	case strings.HasPrefix(action, "ADD CONSTRAINT "):
		return true
	case strings.HasPrefix(action, "DROP CONSTRAINT "):
		return true
	}
	return false
}
