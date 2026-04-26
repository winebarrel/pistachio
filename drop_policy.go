package pistachio

import "slices"

// DropPolicy controls which object types are allowed to be dropped.
// If AllowDrop is empty, no drops are allowed (safe default).
// If AllowDrop contains "all", all drops are allowed.
// Otherwise, only the listed object types are allowed to be dropped.
type DropPolicy struct {
	AllowDrop []string `enum:"all,table,view,enum,domain,column,constraint,foreign_key,index" env:"PIST_ALLOW_DROP" help:"Allow dropping specified object types (can be repeated). Use 'all' to allow all drops."`
}

func (p *DropPolicy) IsDropAllowed(objectType string) bool {
	if p == nil || len(p.AllowDrop) == 0 {
		return false
	}
	return slices.Contains(p.AllowDrop, "all") || slices.Contains(p.AllowDrop, objectType)
}
