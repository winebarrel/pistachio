//go:build testonly

package command

// The names the external tests use for what is unexported. The testonly tag
// keeps them out of a build without it, which is what a release is.

var (
	WriteDumpFiles    = writeDumpFiles
	IsTerminalDefault = isTerminalDefault

	// IsTerminalFnPtr points at the TTY check StartPager uses, so a test can
	// swap it and put it back.
	IsTerminalFnPtr = &isTerminalFn
)
