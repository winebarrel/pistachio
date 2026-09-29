package diff

import "github.com/winebarrel/orderedmap/v2"

// DropChecker checks whether dropping a specific object type is allowed.
type DropChecker interface {
	IsDropAllowed(objectType string) bool
}

// denyAllDrops is a DropChecker that denies all drops.
type denyAllDrops struct{}

func (denyAllDrops) IsDropAllowed(string) bool { return false }

// normalizeDropChecker returns dc if non-nil, otherwise returns denyAllDrops.
func normalizeDropChecker(dc DropChecker) DropChecker {
	if dc == nil {
		return denyAllDrops{}
	}
	return dc
}

// dropMissing returns "DROP <keyword> <name>;" for each current object that
// desired lacks, in current's order. When allowed is false the statements go
// to skipped instead, commented out.
func dropMissing[V any](current, desired *orderedmap.Map[string, V], keyword string, allowed bool) (drops, skipped []string) {
	for k := range current.Keys() {
		if _, ok := desired.GetOk(k); !ok {
			if allowed {
				drops = append(drops, "DROP "+keyword+" "+k+";")
			} else {
				skipped = append(skipped, "-- skipped: DROP "+keyword+" "+k+";")
			}
		}
	}
	return drops, skipped
}
