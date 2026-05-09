package diff

// allowAllDrops is a DropChecker that permits every drop. It is a fixture used
// only by tests; production callers either pass a *cmd.DropPolicy or rely on
// normalizeDropChecker's default of denyAllDrops.
type allowAllDrops struct{}

func (allowAllDrops) IsDropAllowed(string) bool { return true }

// AllowAllDrops and DenyAllDrops are exposed to external test packages
// (package diff_test) via type aliases.
type (
	AllowAllDrops = allowAllDrops
	DenyAllDrops  = denyAllDrops
)
