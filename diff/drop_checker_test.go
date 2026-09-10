package diff

// allowAllDrops is a DropChecker that permits every drop. It is a fixture used
// only by tests; production callers either pass a *cmd.DropPolicy or rely on
// normalizeDropChecker's default of denyAllDrops.
type allowAllDrops struct{}

func (allowAllDrops) IsDropAllowed(string) bool { return true }
