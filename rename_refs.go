package pistachio

import (
	"regexp"
	"slices"
	"strings"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// objectRename is a user type or sequence that -- pista:renamed-from renames.
type objectRename struct {
	schema, from, to string
}

// referenceRenames carries a type or sequence rename into the current side's
// references to it: the type of a column, a composite attribute or a domain,
// and the sequence in a nextval() default. PostgreSQL follows the rename by
// OID, but the catalog was read before it and still names the old object, so
// without this the diff retypes every column of the renamed type.
//
// The catalog writes a name without its schema only when the schema is on the
// search_path, so a bare name counts only then.
type referenceRenames struct {
	types, sequences []objectRename
	searchPath       []string
}

// collectRenames returns the renames the Diff* function for the same objects
// would plan: the desired object names a current one it is renamed from, and
// the new name is not in use yet. Any other combination is an error there or
// a rename already applied.
func collectRenames[V any](current, desired *orderedmap.Map[string, V], get func(V) (schema, name string, renameFrom *string)) []objectRename {
	var renames []objectRename
	for newKey, des := range desired.All() {
		_, to, renameFrom := get(des)
		if renameFrom == nil || *renameFrom == newKey {
			continue
		}
		cur, ok := current.GetOk(*renameFrom)
		if !ok {
			continue
		}
		if _, exists := current.GetOk(newKey); exists {
			continue
		}
		schema, from, _ := get(cur)
		renames = append(renames, objectRename{schema: schema, from: from, to: to})
	}
	return renames
}

// splitSearchPath returns the schemas a search_path value names.
func splitSearchPath(searchPath string) []string {
	var schemas []string
	for s := range strings.SplitSeq(searchPath, ",") {
		s = strings.TrimSpace(s)
		if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
			s = strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
		}
		if s != "" {
			schemas = append(schemas, s)
		}
	}
	return schemas
}

// rename returns name with a rename in renames applied, and whether one was.
// name is an identifier as the catalog writes it, bare or schema-qualified.
func (r *referenceRenames) rename(name string, renames []objectRename) (string, bool) {
	for _, o := range renames {
		if name == model.Ident(o.schema, o.from) {
			return model.Ident(o.schema, o.to), true
		}
		if name == model.Ident(o.from) && slices.Contains(r.searchPath, o.schema) {
			return model.Ident(o.to), true
		}
	}
	return name, false
}

// renameType applies a type rename to a type name, keeping any array suffix.
func (r *referenceRenames) renameType(typeName string) (string, bool) {
	base := strings.TrimRight(typeName, "[]")
	renamed, ok := r.rename(base, r.types)
	return renamed + typeName[len(base):], ok
}

var nextvalRe = regexp.MustCompile(`nextval\('((?:[^']|'')*)'`)

// renameDefault applies a sequence rename to the nextval() calls in a default.
func (r *referenceRenames) renameDefault(def *string) (*string, bool) {
	if def == nil || len(r.sequences) == 0 {
		return def, false
	}
	changed := false
	s := nextvalRe.ReplaceAllStringFunc(*def, func(m string) string {
		lit := nextvalRe.FindStringSubmatch(m)[1]
		renamed, ok := r.rename(strings.ReplaceAll(lit, "''", "'"), r.sequences)
		if !ok {
			return m
		}
		changed = true
		return "nextval('" + strings.ReplaceAll(renamed, "'", "''") + "'"
	})
	return &s, changed
}

// applyTables returns tables with the renames applied to its columns. A table
// with nothing to rename is shared, not copied.
func (r *referenceRenames) applyTables(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	out := tables.Clone()
	for key, t := range tables.All() {
		var cols *orderedmap.Map[string, *model.Column]
		for name, col := range t.Columns.All() {
			typeName, typeChanged := r.renameType(col.TypeName)
			def, defChanged := r.renameDefault(col.Default)
			if !typeChanged && !defChanged {
				continue
			}
			if cols == nil {
				cols = t.Columns.Clone()
			}
			c := *col
			c.TypeName = typeName
			c.Default = def
			cols.Set(name, &c)
		}
		if cols != nil {
			copied := *t
			copied.Columns = cols
			out.Set(key, &copied)
		}
	}
	return out
}

// applyDomains returns domains with the renames applied to their base type
// and default.
func (r *referenceRenames) applyDomains(domains *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
	out := domains.Clone()
	for key, d := range domains.All() {
		baseType, typeChanged := r.renameType(d.BaseType)
		def, defChanged := r.renameDefault(d.Default)
		if !typeChanged && !defChanged {
			continue
		}
		copied := *d
		copied.BaseType = baseType
		copied.Default = def
		out.Set(key, &copied)
	}
	return out
}

// applyCompositeTypes returns composite types with the type renames applied
// to their attributes.
func (r *referenceRenames) applyCompositeTypes(types *orderedmap.Map[string, *model.CompositeType]) *orderedmap.Map[string, *model.CompositeType] {
	out := types.Clone()
	for key, ct := range types.All() {
		var attrs []*model.CompositeAttribute
		for i, a := range ct.Attributes {
			typeName, changed := r.renameType(a.TypeName)
			if !changed {
				continue
			}
			if attrs == nil {
				attrs = slices.Clone(ct.Attributes)
			}
			copied := *a
			copied.TypeName = typeName
			attrs[i] = &copied
		}
		if attrs != nil {
			copied := *ct
			copied.Attributes = attrs
			out.Set(key, &copied)
		}
	}
	return out
}
