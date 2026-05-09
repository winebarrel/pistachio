package diff

// AllowAllDrops and DenyAllDrops are exposed only to tests; production callers
// pass a *cmd.DropPolicy as the DropChecker.
type (
	AllowAllDrops = allowAllDrops
	DenyAllDrops  = denyAllDrops
)
