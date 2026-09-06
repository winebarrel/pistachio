package diff

import (
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// diffStorageParams returns the statements that bring a table's storage
// parameters in line with the desired schema: one SET for the parameters that
// arrive or change value, one RESET for the ones the desired schema no longer
// names. Neither rewrites the table. Both sides are keyed in name order, so
// the statements come out the same on every run.
//
// The caller clears the parameters on both sides unless --manage-storage-param
// asked for them, so an unmanaged run reaches here with two empty maps and
// plans nothing.
//
// A partitioned table needs no special case, since PostgreSQL rejects every
// parameter on a relation that holds no storage. A partition holds its own,
// which it does not take from the parent, and is diffed like any other table.
func diffStorageParams(fqtn string, current, desired *model.Table) []string {
	set, reset := storageParamChanges(current.StorageParams, desired.StorageParams)

	var stmts []string
	if len(set) > 0 {
		stmts = append(stmts, model.SetStorageParamsSQL(fqtn, set))
	}
	if len(reset) > 0 {
		stmts = append(stmts, model.ResetStorageParamsSQL(fqtn, reset))
	}
	return stmts
}

// diffViewStorageParams is diffStorageParams for a view, which takes the same
// two statements under its own keyword. It is reached only when the definition
// is left alone. A view the plan replaces or recreates carries the parameters
// on that statement, which replaces the options as a whole.
func diffViewStorageParams(fqvn string, current, desired *model.View) []string {
	set, reset := storageParamChanges(current.StorageParams, desired.StorageParams)

	var stmts []string
	if len(set) > 0 {
		stmts = append(stmts, model.SetViewStorageParamsSQL(fqvn, desired.ObjType(), set))
	}
	if len(reset) > 0 {
		stmts = append(stmts, model.ResetViewStorageParamsSQL(fqvn, desired.ObjType(), reset))
	}
	return stmts
}

// storageParamChanges reads two parameter maps and returns the name=value
// entries a SET carries and the names a RESET carries.
func storageParamChanges(current, desired *orderedmap.Map[string, string]) (set, reset []string) {
	currentParams := nonNilParams(current)
	desiredParams := nonNilParams(desired)

	for name, value := range desiredParams.All() {
		if cur, ok := currentParams.GetOk(name); !ok || cur != value {
			set = append(set, name+"="+model.QuoteLiteral(value))
		}
	}
	for name := range currentParams.Keys() {
		if _, ok := desiredParams.GetOk(name); !ok {
			reset = append(reset, name)
		}
	}
	return set, reset
}

// nonNilParams stands an empty map in for one built without any. The catalog
// and the parser always set it, other callers may not, and clearing it is what
// leaves the parameters unmanaged.
func nonNilParams(params *orderedmap.Map[string, string]) *orderedmap.Map[string, string] {
	if params == nil {
		return orderedmap.New[string, string]()
	}
	return params
}
