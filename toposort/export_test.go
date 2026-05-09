package toposort

// NewGraph, ExtractDeps, and SortSQL are exposed only to tests; the production
// entry point is OrderFromSchema, which uses the unexported newGraph internally.
var (
	NewGraph    = newGraph
	ExtractDeps = extractDeps
	SortSQL     = sortSQL
)
