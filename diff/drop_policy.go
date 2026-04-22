package diff

// DropChecker checks whether dropping a specific object type is allowed.
type DropChecker interface {
	IsDropAllowed(objectType string) bool
}

// AllowAllDrops is a DropChecker that allows all drops.
type AllowAllDrops struct{}

func (AllowAllDrops) IsDropAllowed(string) bool { return true }

// DenyAllDrops is a DropChecker that denies all drops.
type DenyAllDrops struct{}

func (DenyAllDrops) IsDropAllowed(string) bool { return false }

// NormalizeDropChecker returns dc if non-nil, otherwise returns DenyAllDrops.
func NormalizeDropChecker(dc DropChecker) DropChecker {
	if dc == nil {
		return DenyAllDrops{}
	}
	return dc
}
