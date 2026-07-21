package pistachio

import "slices"

// DropPolicy controls which object types are allowed to be dropped.
// If AllowDrop is empty, no drops are allowed (safe default).
// If AllowDrop contains "all", all drops are allowed.
// Otherwise, only the listed object types are allowed to be dropped.
type DropPolicy struct {
	AllowDrop []string `enum:"all,table,view,enum,domain,sequence,column,constraint,foreign_key,index,policy" env:"PISTA_ALLOW_DROP" help:"Allow dropping these object types (repeatable; 'all' allows everything)."`
}

func (p *DropPolicy) IsDropAllowed(objectType string) bool {
	if p == nil || len(p.AllowDrop) == 0 {
		return false
	}
	return slices.Contains(p.AllowDrop, "all") || slices.Contains(p.AllowDrop, objectType)
}
