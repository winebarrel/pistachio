package command

import (
	"fmt"
	"io"
	"os"
	"os/exec"
)

// StartPager wraps w with a pager subprocess when all of the following hold:
//   - paging hasn't been disabled by --no-pager,
//   - w is a TTY (so the user is actually viewing the output interactively),
//   - PISTA_PAGER is set to a non-empty command line.
//
// Otherwise it returns w unchanged with a no-op close. The returned close
// function tears down the pager and waits for it to exit, so callers should
// always invoke it (typically via defer).
func StartPager(w io.Writer, noPager bool) (io.Writer, func(), error) {
	noop := func() {}
	if noPager {
		return w, noop, nil
	}
	f, ok := w.(*os.File)
	if !ok || !isTerminalFn(f) {
		return w, noop, nil
	}
	cmdline := os.Getenv("PISTA_PAGER")
	if cmdline == "" {
		return w, noop, nil
	}

	cmd := exec.Command("sh", "-c", cmdline)
	cmd.Stdout = f
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return w, noop, fmt.Errorf("opening pager pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		stdin.Close() //nolint:errcheck
		return w, noop, fmt.Errorf("starting pager %q: %w", cmdline, err)
	}
	closer := func() {
		stdin.Close() //nolint:errcheck
		cmd.Wait()    //nolint:errcheck
	}
	return stdin, closer, nil
}

// isTerminalFn is a package-level indirection so tests can simulate a TTY
// without an actual controlling terminal.
var isTerminalFn = func(f *os.File) bool {
	if f == nil {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
