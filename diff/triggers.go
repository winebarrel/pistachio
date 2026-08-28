package diff

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// diffTriggers emits the statements that bring one relation's triggers to the
// desired state.
//
// A definition change goes through CREATE OR REPLACE TRIGGER, which PostgreSQL
// has carried since 14 and which takes a lighter lock than DROP TRIGGER. It
// refuses to turn a constraint trigger into a plain one or back, so that pair
// falls back to DROP and CREATE, honoring the trigger-drop policy via dc the
// same as a removal: with the drop denied, the trigger keeps its current
// definition rather than running the CREATE half alone. Either way PostgreSQL
// leaves the new trigger enabled, so a desired state other than the default is
// re-applied after the statement that reset it.
func diffTriggers(
	fqtn string,
	current, desired *orderedmap.Map[string, *model.Trigger],
	dc DropChecker,
) (stmts []string, disallowed []string, err error) {
	dc = normalizeDropChecker(dc)
	current = triggerMap(current)
	desired = triggerMap(desired)

	// Detect renames first so the steps below see the renamed trigger under
	// its new name in the adjusted current map.
	current, renamedFrom, err := detectTriggerRenames(fqtn, current, desired)
	if err != nil {
		return nil, nil, err
	}

	// Compare once. The loops below need both whether the definition changed
	// and whether the change crosses the constraint-trigger line, which is the
	// one PostgreSQL will not replace in place.
	changed := map[string]bool{}
	recreated := map[string]bool{}
	for name, des := range desired.All() {
		cur, ok := current.GetOk(name)
		if !ok {
			continue
		}
		same, err := equalTriggerDef(cur.Definition, des.Definition, des.Schema)
		if err != nil {
			return nil, nil, err
		}
		if same {
			continue
		}
		changed[name] = true
		if isConstraintTrigger(cur.Definition) != isConstraintTrigger(des.Definition) {
			recreated[name] = true
		}
	}

	triggerAllowed := dc.IsDropAllowed("trigger")

	// A recreate crossing the constraint-trigger line needs the same drop the
	// removal branch below honors: nothing else about the trigger can change
	// while the drop that clears the way for it is denied, so it stays
	// exactly as it is, the way DiffViews leaves a view alone when its own
	// recreate is denied.
	recreateDenied := map[string]bool{}
	if !triggerAllowed {
		for name := range recreated {
			recreateDenied[name] = true
		}
	}

	// A trigger that is both renamed and recreated skips the RENAME, since the
	// CREATE below already puts the new name in place. One whose recreate is
	// denied keeps its old name too, recreated[name] being true either way.
	for name := range desired.Keys() {
		oldName, renamed := renamedFrom[name]
		if !renamed || recreated[name] {
			continue
		}
		stmts = append(stmts, renameTriggerSQL(fqtn, oldName, name))
	}

	// Drop the triggers the desired schema no longer declares, then the ones
	// the recreate needs out of the way. A recreate whose RENAME was skipped
	// still has to drop the old name.
	for name := range current.Keys() {
		if _, ok := desired.GetOk(name); !ok {
			drop := dropTriggerSQL(fqtn, name)
			if !triggerAllowed {
				disallowed = append(disallowed, "-- skipped: "+drop)
				continue
			}
			stmts = append(stmts, drop)
			continue
		}
		if recreated[name] {
			dropName := name
			if oldName, ok := renamedFrom[name]; ok {
				dropName = oldName
			}
			drop := dropTriggerSQL(fqtn, dropName)
			if recreateDenied[name] {
				disallowed = append(disallowed, "-- skipped: "+drop)
				continue
			}
			stmts = append(stmts, drop)
		}
	}

	for name, des := range desired.All() {
		if recreateDenied[name] {
			continue
		}

		cur, ok := current.GetOk(name)
		if !ok || recreated[name] {
			stmts = append(stmts, des.SQL())
			if s := des.StateSQL(); s != "" {
				stmts = append(stmts, s)
			}
			continue
		}

		if !changed[name] {
			if cur.State.Action() != des.State.Action() {
				stmts = append(stmts, alterTriggerStateSQL(fqtn, des))
			}
			continue
		}

		replace, err := replaceTriggerSQL(des.Definition)
		if err != nil {
			return nil, nil, err
		}
		stmts = append(stmts, replace)
		if s := des.StateSQL(); s != "" {
			stmts = append(stmts, s)
		}
	}

	return stmts, disallowed, nil
}

// detectTriggerRenames reads the -- pista:renamed-from directives on the
// desired triggers and returns the current map rekeyed under the new names,
// plus the new name to old name pairs the caller renders as ALTER TRIGGER.
func detectTriggerRenames(
	fqtn string,
	current, desired *orderedmap.Map[string, *model.Trigger],
) (*orderedmap.Map[string, *model.Trigger], map[string]string, error) {
	adjusted := cloneMap(current)
	renamedFrom := map[string]string{}

	for newName, des := range desired.All() {
		if des.RenameFrom == nil {
			continue
		}
		oldName := *des.RenameFrom
		if oldName == newName {
			continue
		}

		old, ok := adjusted.GetOk(oldName)
		if !ok {
			// The rename has already been applied on an earlier run, so the
			// directive can stay in the file until the next cleanup.
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source trigger %s not found on %s", model.Ident(oldName), fqtn)
		}
		if _, exists := adjusted.GetOk(newName); exists {
			return nil, nil, fmt.Errorf("cannot rename trigger %s to %s on %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
		}

		// ALTER TRIGGER ... RENAME changes the name pg_get_triggerdef writes,
		// so carry the new name into the definition too. Without it the
		// comparison below reads the old name as a definition change and adds
		// a redundant CREATE OR REPLACE.
		def, err := renameTriggerDef(old.Definition, newName)
		if err != nil {
			return nil, nil, err
		}

		renamedFrom[newName] = oldName
		adjusted.Delete(oldName)
		renamed := *old
		renamed.Name = newName
		renamed.Definition = def
		adjusted.Set(newName, &renamed)
	}

	return adjusted, renamedFrom, nil
}

// triggerMap returns m, or an empty map when the caller built its model
// without one.
func triggerMap(m *orderedmap.Map[string, *model.Trigger]) *orderedmap.Map[string, *model.Trigger] {
	if m == nil {
		return orderedmap.New[string, *model.Trigger]()
	}
	return m
}

func dropTriggerSQL(fqtn, name string) string {
	return "DROP TRIGGER " + model.Ident(name) + " ON " + fqtn + ";"
}

func renameTriggerSQL(fqtn, oldName, newName string) string {
	return "ALTER TRIGGER " + model.Ident(oldName) + " ON " + fqtn + " RENAME TO " + model.Ident(newName) + ";"
}

func alterTriggerStateSQL(fqtn string, trg *model.Trigger) string {
	return model.TriggerStateSQL(fqtn, trg.Name, trg.State)
}

// parseTriggerDef parses a CREATE TRIGGER statement and takes out the wording
// that says nothing about the trigger's state. An implicit relation schema is
// filled in from the owning relation, and OR REPLACE is cleared.
//
// The EXECUTE FUNCTION name loses its schema, which pg_get_triggerdef omits
// once the function's schema is on the search_path, the way stripFuncSchema
// handles a call inside an expression. The WHEN expression goes through the
// normalization constraint CHECK bodies use, since pg_get_triggerdef adds casts
// there the way pg_get_constraintdef does.
//
// It returns the whole result so the caller can deparse it back.
func parseTriggerDef(def, schema string) (*pg_query.ParseResult, *pg_query.CreateTrigStmt, error) {
	result, err := pg_query.Parse(def)
	if err != nil {
		return nil, nil, err
	}
	if len(result.Stmts) != 1 {
		return nil, nil, fmt.Errorf("unexpected parse result for trigger definition: %s", def)
	}
	ct := result.Stmts[0].Stmt.GetCreateTrigStmt()
	if ct == nil {
		return nil, nil, fmt.Errorf("unexpected parse result for trigger definition: %s", def)
	}
	if ct.Relation != nil && ct.Relation.Schemaname == "" {
		ct.Relation.Schemaname = schema
	}
	ct.Replace = false
	if len(ct.Funcname) > 1 {
		ct.Funcname = ct.Funcname[len(ct.Funcname)-1:]
	}
	if ct.WhenClause != nil {
		ct.WhenClause = normalizeCheckExpr(ct.WhenClause)
	}
	return result, ct, nil
}

// equalTriggerDef compares two CREATE TRIGGER definitions by deparsing their
// normalized parse trees. pg_get_triggerdef and the deparser disagree on
// wording that carries no meaning (the AS in REFERENCING, an explicit FOR EACH
// STATEMENT), so the definitions cannot be compared as they arrive, and the
// trees themselves carry the byte offset of every token, which differs as soon
// as the two spellings differ in length. schema fills in a relation the
// catalog wrote bare because search_path reached it.
func equalTriggerDef(current, desired, schema string) (bool, error) {
	if current == desired {
		return true, nil
	}
	curResult, _, err := parseTriggerDef(current, schema)
	if err != nil {
		return false, fmt.Errorf("failed to parse the current trigger definition: %w", err)
	}
	desResult, _, err := parseTriggerDef(desired, schema)
	if err != nil {
		return false, fmt.Errorf("failed to parse the desired trigger definition: %w", err)
	}
	cur, err := pg_query.Deparse(curResult)
	if err != nil {
		return false, fmt.Errorf("failed to deparse the current trigger definition: %w", err)
	}
	des, err := pg_query.Deparse(desResult)
	if err != nil {
		return false, fmt.Errorf("failed to deparse the desired trigger definition: %w", err)
	}
	return cur == des, nil
}

// isConstraintTrigger reports whether a definition is a CREATE CONSTRAINT
// TRIGGER. PostgreSQL will not replace one with a plain trigger or the other
// way round, so the two need a DROP in between.
func isConstraintTrigger(def string) bool {
	_, ct, err := parseTriggerDef(def, "")
	if err != nil {
		return false
	}
	return ct.Isconstraint
}

// renameTriggerDef rewrites a CREATE TRIGGER definition to carry a new trigger
// name, the way the catalog reports it after ALTER TRIGGER ... RENAME.
func renameTriggerDef(def, newName string) (string, error) {
	result, err := pg_query.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse the current trigger definition: %w", err)
	}
	if len(result.Stmts) != 1 {
		return "", fmt.Errorf("unexpected parse result for trigger definition: %s", def)
	}
	ct := result.Stmts[0].Stmt.GetCreateTrigStmt()
	if ct == nil {
		return "", fmt.Errorf("unexpected parse result for trigger definition: %s", def)
	}
	ct.Trigname = newName
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse the current trigger definition: %w", err)
	}
	return sql, nil
}

// replaceTriggerSQL rewrites a CREATE TRIGGER definition into the
// CREATE OR REPLACE TRIGGER that updates the trigger in place.
func replaceTriggerSQL(def string) (string, error) {
	result, err := pg_query.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse the desired trigger definition: %w", err)
	}
	if len(result.Stmts) != 1 {
		return "", fmt.Errorf("unexpected parse result for trigger definition: %s", def)
	}
	ct := result.Stmts[0].Stmt.GetCreateTrigStmt()
	if ct == nil {
		return "", fmt.Errorf("unexpected parse result for trigger definition: %s", def)
	}
	ct.Replace = true
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse the desired trigger definition: %w", err)
	}
	return sql + ";", nil
}
