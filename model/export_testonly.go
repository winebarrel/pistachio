//go:build testonly

package model

// The names the external tests use for what is unexported. The testonly tag
// keeps them out of a build without it, which is what a release is.

var QuoteIdent = quoteIdent
