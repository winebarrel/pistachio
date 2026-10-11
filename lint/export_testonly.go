//go:build testonly

package lint

// The names the external tests use for what is unexported. The testonly tag
// keeps them out of a build without it, which is what a release is.

// Must is a function rather than a variable because must is generic.
func Must[T any](v T, err error) T { return must(v, err) }
