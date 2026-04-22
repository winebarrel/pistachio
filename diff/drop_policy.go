package diff

// DropChecker checks whether dropping a specific object type is allowed.
type DropChecker interface {
	IsDropAllowed(objectType string) bool
}

// AllowAllDrops is a DropChecker that allows all drops.
type AllowAllDrops struct{}

func (AllowAllDrops) IsDropAllowed(string) bool { return true }
