package toposort

// NewGraph is exposed only to tests; the production entry point is
// OrderFromSchema, which uses the unexported newGraph internally.
var NewGraph = newGraph
