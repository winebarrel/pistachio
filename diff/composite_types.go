package diff

import (
	"fmt"
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

type CompositeTypeDiffResult struct {
	Stmts               []string
	DropStmts           []string
	DisallowedDropStmts []string
}

func DiffCompositeTypes(current, desired *orderedmap.Map[string, *model.CompositeType], dc DropChecker) (*CompositeTypeDiffResult, error) {
	dc = normalizeDropChecker(dc)
	result := &CompositeTypeDiffResult{}

	// Detect renames
	renameStmts, current, err := detectCompositeTypeRenames(current, desired)
	if err != nil {
		return nil, err
	}
	result.Stmts = append(result.Stmts, renameStmts...)

	// New composite types
	for k, desiredCT := range desired.All() {
		if _, ok := current.GetOk(k); !ok {
			result.Stmts = append(result.Stmts, desiredCT.SQL())
			if commentSQL := desiredCT.CommentSQL(); commentSQL != "" {
				result.Stmts = append(result.Stmts, commentSQL)
			}
			result.Stmts = append(result.Stmts, desiredCT.AttributeCommentSQLs()...)
		}
	}

	// Modified composite types
	for k, desiredCT := range desired.All() {
		currentCT, ok := current.GetOk(k)
		if !ok {
			continue
		}

		stmts, disallowed, err := diffCompositeType(k, currentCT, desiredCT, dc)
		if err != nil {
			return nil, err
		}
		result.Stmts = append(result.Stmts, stmts...)
		result.DisallowedDropStmts = append(result.DisallowedDropStmts, disallowed...)
	}

	// Dropped composite types. When the composite-type-drop policy disallows it,
	// emit a commented DROP.
	ctAllowed := dc.IsDropAllowed("composite_type")
	for k := range current.Keys() {
		if _, ok := desired.GetOk(k); !ok {
			if ctAllowed {
				result.DropStmts = append(result.DropStmts, "DROP TYPE "+k+";")
			} else {
				result.DisallowedDropStmts = append(result.DisallowedDropStmts, "-- skipped: DROP TYPE "+k+";")
			}
		}
	}

	return result, nil
}

// cloneCompositeAttributes returns value copies of the attributes so renames
// applied to the working slice do not mutate the current model.
func cloneCompositeAttributes(attrs []*model.CompositeAttribute) []*model.CompositeAttribute {
	out := make([]*model.CompositeAttribute, len(attrs))
	for i, a := range attrs {
		copied := *a
		out[i] = &copied
	}
	return out
}

// equalCollation compares two attribute collations by their trailing name
// component. The catalog reports a collation schema-qualified (e.g.
// "pg_catalog.C"), while the parser keeps what the user wrote ("C"). Comparing
// only the collation name avoids a spurious ALTER ATTRIBUTE on every plan when
// the two forms differ but name the same collation.
func equalCollation(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return collationName(*a) == collationName(*b)
}

func collationName(s string) string {
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[i+1:]
	}
	return s
}

func indexCompositeAttribute(attrs []*model.CompositeAttribute, name string) int {
	for i, a := range attrs {
		if a.Name == name {
			return i
		}
	}
	return -1
}

// diffCompositeType returns the ALTER statements to converge a composite type,
// plus any disallowed DROP ATTRIBUTE statements (gated by the composite_type
// drop policy). Attributes are matched by name after applying renames;
// attribute order is not enforced because PostgreSQL cannot reorder them and
// ADD ATTRIBUTE always appends.
func diffCompositeType(fqcn string, current, desired *model.CompositeType, dc DropChecker) ([]string, []string, error) {
	var stmts []string
	var disallowed []string

	// Apply attribute renames first so renamed attributes are not seen as a
	// drop plus add.
	working := cloneCompositeAttributes(current.Attributes)
	for _, da := range desired.Attributes {
		if da.RenameFrom == nil || *da.RenameFrom == da.Name {
			continue
		}
		old := *da.RenameFrom

		oldIdx := indexCompositeAttribute(working, old)
		if oldIdx < 0 {
			if indexCompositeAttribute(working, da.Name) >= 0 {
				// Already applied
				continue
			}
			return nil, nil, fmt.Errorf("rename source attribute %s not found in composite type %s", model.Ident(old), fqcn)
		}
		if indexCompositeAttribute(working, da.Name) >= 0 {
			return nil, nil, fmt.Errorf("cannot rename attribute %s to %s in composite type %s: destination already exists", model.Ident(old), model.Ident(da.Name), fqcn)
		}

		stmts = append(stmts, "ALTER TYPE "+fqcn+" RENAME ATTRIBUTE "+model.Ident(old)+" TO "+model.Ident(da.Name)+";")
		working[oldIdx].Name = da.Name
	}

	workingByName := make(map[string]*model.CompositeAttribute, len(working))
	for _, a := range working {
		workingByName[a.Name] = a
	}
	desiredByName := make(map[string]*model.CompositeAttribute, len(desired.Attributes))
	for _, a := range desired.Attributes {
		desiredByName[a.Name] = a
	}

	// Drop attributes present in current but not desired.
	ctAllowed := dc.IsDropAllowed("composite_type")
	for _, wa := range working {
		if _, ok := desiredByName[wa.Name]; !ok {
			drop := "ALTER TYPE " + fqcn + " DROP ATTRIBUTE " + model.Ident(wa.Name) + ";"
			if ctAllowed {
				stmts = append(stmts, drop)
			} else {
				disallowed = append(disallowed, "-- skipped: "+drop)
			}
		}
	}

	// Add new attributes and alter changed types.
	for _, da := range desired.Attributes {
		wa, ok := workingByName[da.Name]
		if !ok {
			stmts = append(stmts, "ALTER TYPE "+fqcn+" ADD ATTRIBUTE "+model.Ident(da.Name)+" "+da.TypeSQL()+";")
			continue
		}
		if wa.TypeName != da.TypeName || !equalCollation(wa.Collation, da.Collation) {
			stmts = append(stmts, "ALTER TYPE "+fqcn+" ALTER ATTRIBUTE "+model.Ident(da.Name)+" TYPE "+da.TypeSQL()+";")
		}
	}

	// Type comment change.
	if !equalPtr(current.Comment, desired.Comment) {
		if desired.Comment != nil {
			stmts = append(stmts, "COMMENT ON TYPE "+fqcn+" IS "+model.QuoteLiteral(*desired.Comment)+";")
		} else {
			stmts = append(stmts, "COMMENT ON TYPE "+fqcn+" IS NULL;")
		}
	}

	// Attribute comment changes. The current comment is read from the working
	// attribute (matched by the desired name after rename); a newly added
	// attribute has no current comment.
	for _, da := range desired.Attributes {
		var currentComment *string
		if wa, ok := workingByName[da.Name]; ok {
			currentComment = wa.Comment
		}
		if equalPtr(currentComment, da.Comment) {
			continue
		}
		col := model.Ident(desired.Schema, desired.Name, da.Name)
		if da.Comment != nil {
			stmts = append(stmts, "COMMENT ON COLUMN "+col+" IS "+model.QuoteLiteral(*da.Comment)+";")
		} else {
			stmts = append(stmts, "COMMENT ON COLUMN "+col+" IS NULL;")
		}
	}

	return stmts, disallowed, nil
}

// detectCompositeTypeRenames finds desired composite types with RenameFrom that
// match a current composite type.
func detectCompositeTypeRenames(current, desired *orderedmap.Map[string, *model.CompositeType]) ([]string, *orderedmap.Map[string, *model.CompositeType], error) {
	var stmts []string
	adjusted := cloneMap(current)

	for newKey, desiredCT := range desired.All() {
		if desiredCT.RenameFrom == nil {
			continue
		}
		oldKey := *desiredCT.RenameFrom

		if oldKey == newKey {
			continue
		}

		oldCT, ok := adjusted.GetOk(oldKey)
		if !ok {
			if _, exists := adjusted.GetOk(newKey); exists {
				continue
			}
			return nil, nil, fmt.Errorf("rename source %s not found for %s", oldKey, newKey)
		}

		if _, exists := adjusted.GetOk(newKey); exists {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: destination already exists", oldKey, newKey)
		}

		if oldCT.Schema != desiredCT.Schema {
			return nil, nil, fmt.Errorf("cannot rename %s to %s: cross-schema rename is not supported", oldKey, newKey)
		}

		stmts = append(stmts, "ALTER TYPE "+oldKey+" RENAME TO "+model.Ident(desiredCT.Name)+";")

		adjusted.Delete(oldKey)
		renamed := *oldCT
		renamed.Name = desiredCT.Name
		renamed.Attributes = cloneCompositeAttributes(oldCT.Attributes)
		adjusted.Set(newKey, &renamed)
	}

	return stmts, adjusted, nil
}
