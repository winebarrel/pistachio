//go:build testonly

package toposort

// The names the external tests use for what is unexported. The testonly tag
// keeps them out of a build without it, which is what a release is.

var NewGraph = newGraph
