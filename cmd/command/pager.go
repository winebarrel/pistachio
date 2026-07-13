package command

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sync"
)

// StartPager wraps w with a pager subprocess when all of the following hold:
//   - paging hasn't been disabled by --no-pager (pager != nil && *pager == false),
//   - either pager was explicitly forced via --pager (*pager == true) or w is a
//     TTY (so the user is actually viewing the output interactively),
//   - PISTA_PAGER is set to a non-empty command line.
//
// pager == nil means the user passed neither --pager nor --no-pager, so the
// TTY check decides. Otherwise *pager explicitly turns paging on or off, but
// an unset PISTA_PAGER still gates everything (matching the convention that
// the env var is the source of truth for *which* pager to run).
//
// Otherwise it returns w unchanged with a no-op close. The returned close
// function tears down the pager and waits for it to exit, so callers should
// always invoke it (typically via defer).
func StartPager(w io.Writer, pager *bool) (io.Writer, func(), error) {
	noop := func() {}
	if pager != nil && !*pager {
		return w, noop, nil
	}
	cmdline := os.Getenv("PISTA_PAGER")
	if cmdline == "" {
		return w, noop, nil
	}
	f, ok := w.(*os.File)
	if !ok {
		return w, noop, nil
	}
	forced := pager != nil && *pager
	if !forced && !isTerminalFn(f) {
		return w, noop, nil
	}

	// PISTA_PAGER is interpreted by the platform shell: sh on Unix,
	// cmd on Windows.
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", cmdline)
	} else {
		cmd = exec.Command("sh", "-c", cmdline)
	}
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
	// Idempotent so callers can pair an explicit close (to drain the pager
	// before a process-terminating call like kong.Context.FatalIfErrorf,
	// which bypasses defers via os.Exit) with `defer closer()` for panic
	// safety, without double-closing the pipe or double-waiting on the
	// subprocess.
	var once sync.Once
	closer := func() {
		once.Do(func() {
			stdin.Close() //nolint:errcheck
			cmd.Wait()    //nolint:errcheck
		})
	}
	return stdin, closer, nil
}

// isTerminalFn is a package-level indirection so tests can simulate a TTY
// without an actual controlling terminal.
var isTerminalFn = isTerminalDefault

func isTerminalDefault(f *os.File) bool {
	if f == nil {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
