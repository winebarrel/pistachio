package diff

import (
	"fmt"

	pgquery "github.com/wasilibs/go-pgquery"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// detectEnumRenames finds desired enums with RenameFrom that match a current enum.
func detectEnumRenames(current, desired *orderedmap.Map[string, *model.Enum]) ([]string, *orderedmap.Map[string, *model.Enum], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredEnum := range desired.All() {
		if desiredEnum.RenameFrom == nil {
			continue
		}
		oldKey := *desiredEnum.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldEnum, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldEnum.Schema != desiredEnum.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER TYPE "+oldKey+" RENAME TO "+model.Ident(desiredEnum.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldEnum
		renamed.Name = desiredEnum.Name
		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// detectTableRenames finds desired tables with RenameFrom that match a current table.
//
// NOTE: After a table rename, other objects that reference the old table name
// (e.g. foreign keys in other tables, view definitions) are not updated in the
// adjusted current state. PostgreSQL automatically updates these on RENAME, so
// running plan/apply a second time after a rename will produce a clean diff.
// A single plan may emit redundant DROP/CREATE for dependent objects.
func detectTableRenames(current, desired *orderedmap.Map[string, *model.Table]) ([]string, *orderedmap.Map[string, *model.Table], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredTable := range desired.All() {
		if desiredTable.RenameFrom == nil {
			continue
		}
		oldKey := *desiredTable.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldTable, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldTable.Schema != desiredTable.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER TABLE "+oldKey+" RENAME TO "+model.Ident(desiredTable.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldTable
		renamed.Name = desiredTable.Name

		// Update index definitions to reflect the new table name via pg_query parse/deparse
		if renamed.Indexes.Len() > 0 {
			newIndexes := orderedmap.New[string, *model.Index]()
			for idxName, idx := range renamed.Indexes.All() {
				idxCopy := *idx
				idxCopy.Table = desiredTable.Name
				updatedDef, err := updateIndexTableName(idx.Definition, desiredTable.Name)
				if err != nil {
					return nil, nil, err
				}
				idxCopy.Definition = updatedDef
				newIndexes.Set(idxName, &idxCopy)
			}
			renamed.Indexes = newIndexes
		}

		// Update FK table name
		if renamed.ForeignKeys.Len() > 0 {
			newFKs := orderedmap.New[string, *model.ForeignKey]()
			for fkName, fk := range renamed.ForeignKeys.All() {
				fkCopy := *fk
				fkCopy.Table = desiredTable.Name
				newFKs.Set(fkName, &fkCopy)
			}
			renamed.ForeignKeys = newFKs
		}

		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// updateIndexTableName parses an index definition, updates the table name,
// and deparses it back to canonical SQL.
func updateIndexTableName(def string, newTableName string) (string, error) {
	result, err := pgquery.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse index definition: %w", err)
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil || is.Relation == nil {
		return "", fmt.Errorf("failed to parse index definition: expected IndexStmt with relation")
	}
	is.Relation.Relname = newTableName
	deparsed, err := pgquery.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse index definition: %w", err)
	}
	return deparsed, nil
}

// detectViewRenames finds desired views with RenameFrom that match a current view.
func detectViewRenames(current, desired *orderedmap.Map[string, *model.View]) ([]string, *orderedmap.Map[string, *model.View], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredView := range desired.All() {
		if desiredView.RenameFrom == nil {
			continue
		}
		oldKey := *desiredView.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldView, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if oldKey != newKey {
			if _, exists := adjusted.GetOk(newKey); exists {
				return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
			}
		}

		if oldView.Schema != desiredView.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER VIEW "+oldKey+" RENAME TO "+model.Ident(desiredView.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldView
		renamed.Name = desiredView.Name
		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}

// detectColumnRenames finds desired columns with RenameFrom that match a current column.
//
// NOTE: After a column rename, constraint/index/FK definitions that reference the
// old column name are not updated in the adjusted current state. PostgreSQL
// automatically updates these on RENAME COLUMN, so running plan/apply a second
// time will produce a clean diff. A single plan may emit redundant DROP/ADD for
// dependent constraints or indexes.
func detectColumnRenames(fqtn string, current, desired *orderedmap.Map[string, *model.Column]) ([]string, *orderedmap.Map[string, *model.Column], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredCol := range desired.All() {
		if desiredCol.RenameFrom == nil {
			continue
		}
		oldName := *desiredCol.RenameFrom

		if oldName == newName {
			continue
		}

		oldCol, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source column %s not found in %s", model.Ident(oldName), fqtn)
		}

		if oldName != newName {
			if _, exists := adjusted.GetOk(newName); exists {
				return nil, nil, fmt.Errorf("cannot rename column %s to %s in %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
			}
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME COLUMN "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldCol
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// detectConstraintRenames finds desired constraints with RenameFrom that match a current constraint.
func detectConstraintRenames(fqtn string, current, desired *orderedmap.Map[string, *model.Constraint]) ([]string, *orderedmap.Map[string, *model.Constraint], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredCon := range desired.All() {
		if desiredCon.RenameFrom == nil {
			continue
		}
		oldName := *desiredCon.RenameFrom

		if oldName == newName {
			continue
		}

		oldCon, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source constraint %s not found in %s", model.Ident(oldName), fqtn)
		}

		if oldName != newName {
			if _, exists := adjusted.GetOk(newName); exists {
				return nil, nil, fmt.Errorf("cannot rename constraint %s to %s in %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
			}
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME CONSTRAINT "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldCon
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// detectIndexRenames finds desired indexes with RenameFrom that match a current index.
func detectIndexRenames(current, desired *orderedmap.Map[string, *model.Index]) ([]string, *orderedmap.Map[string, *model.Index], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredIdx := range desired.All() {
		if desiredIdx.RenameFrom == nil {
			continue
		}
		oldName := *desiredIdx.RenameFrom

		if oldName == newName {
			continue
		}

		oldIdx, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source index %s not found", model.Ident(oldName))
		}

		if oldName != newName {
			if _, exists := adjusted.GetOk(newName); exists {
				return nil, nil, fmt.Errorf("cannot rename index %s to %s: destination already exists", model.Ident(oldName), model.Ident(newName))
			}
		}

		stmts = append(stmts, "ALTER INDEX "+model.Ident(oldIdx.Schema, oldName)+" RENAME TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldIdx
		renamed.Name = newName
		// Update definition to reflect the new index name via pg_query parse/deparse
		updatedDef, err := updateIndexName(renamed.Definition, newName)
		if err != nil {
			return nil, nil, err
		}
		renamed.Definition = updatedDef
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// updateIndexName parses an index definition, updates the index name, and deparses.
func updateIndexName(def string, newName string) (string, error) {
	result, err := pgquery.Parse(def)
	if err != nil {
		return "", fmt.Errorf("failed to parse index definition: %w", err)
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return "", fmt.Errorf("failed to parse index definition: expected IndexStmt")
	}
	is.Idxname = newName
	deparsed, err := pgquery.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse index definition: %w", err)
	}
	return deparsed, nil
}

// detectForeignKeyRenames finds desired foreign keys with RenameFrom that match a current FK.
func detectForeignKeyRenames(fqtn string, current, desired *orderedmap.Map[string, *model.ForeignKey]) ([]string, *orderedmap.Map[string, *model.ForeignKey], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newName, desiredFK := range desired.All() {
		if desiredFK.RenameFrom == nil {
			continue
		}
		oldName := *desiredFK.RenameFrom

		if oldName == newName {
			continue
		}

		oldFK, ok := adjusted.GetOk(oldName)
		if !ok {
			if _, exists := adjusted.GetOk(newName); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source foreign key %s not found in %s", model.Ident(oldName), fqtn)
		}

		if oldName != newName {
			if _, exists := adjusted.GetOk(newName); exists {
				return nil, nil, fmt.Errorf("cannot rename foreign key %s to %s in %s: destination already exists", model.Ident(oldName), model.Ident(newName), fqtn)
			}
		}

		stmts = append(stmts, "ALTER TABLE "+fqtn+" RENAME CONSTRAINT "+model.Ident(oldName)+" TO "+model.Ident(newName)+";")

		adjusted.Delete(oldName)
		renamed := *oldFK
		renamed.Name = newName
		adjusted.Set(newName, &renamed)
	}

	return stmts, adjusted, nil
}

// cloneMap creates a shallow copy of an orderedmap.
func cloneMap[K comparable, V any](m *orderedmap.Map[K, V]) *orderedmap.Map[K, V] {
	clone := orderedmap.New[K, V]()
	for k, v := range m.All() {
		clone.Set(k, v)
	}
	return clone
}
