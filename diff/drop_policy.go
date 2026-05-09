package diff

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
