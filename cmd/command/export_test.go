package command

import "os"

// Exported to tests in cmd/command/dump_test.go (package command_test).
var WriteDumpFiles = writeDumpFiles

// IsTerminalDefault exposes the real TTY check used by StartPager when no
// test has overridden isTerminalFn, so cmd/command/pager_test.go can drive
// it directly with a regular file or a nil *os.File.
var IsTerminalDefault = isTerminalDefault

// SetIsTerminalForTest swaps the TTY-check used by StartPager and returns a
// function that restores the previous implementation.
func SetIsTerminalForTest(fn func(*os.File) bool) func() {
	old := isTerminalFn
	isTerminalFn = fn
	return func() { isTerminalFn = old }
}
