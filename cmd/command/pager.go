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
	// A missing shell is reported now. The pager itself starts on the first
	// write, so a command that writes nothing to stdout never starts it.
	if cmd.Err != nil {
		return w, noop, fmt.Errorf("starting pager %q: %w", cmdline, cmd.Err)
	}
	cmd.Stdout = f
	cmd.Stderr = os.Stderr
	p := &lazyPager{cmd: cmd, cmdline: cmdline}
	// Idempotent so callers can pair an explicit close (to drain the pager
	// before a process-terminating call like kong.Context.FatalIfErrorf,
	// which bypasses defers via os.Exit) with `defer closer()` for panic
	// safety, without double-closing the pipe or double-waiting on the
	// subprocess.
	var once sync.Once
	closer := func() { once.Do(p.close) }
	return p, closer, nil
}

// lazyPager starts the pager on the first Write.
type lazyPager struct {
	cmd     *exec.Cmd
	cmdline string
	stdin   io.WriteCloser
	err     error
}

func (p *lazyPager) Write(b []byte) (int, error) {
	if p.stdin == nil && p.err == nil {
		p.err = p.start()
	}
	if p.err != nil {
		return 0, p.err
	}
	return p.stdin.Write(b)
}

func (p *lazyPager) start() error {
	stdin, err := p.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("opening pager pipe: %w", err)
	}
	if err := p.cmd.Start(); err != nil {
		stdin.Close() //nolint:errcheck
		return fmt.Errorf("starting pager %q: %w", p.cmdline, err)
	}
	p.stdin = stdin
	return nil
}

// close does nothing when the pager never started.
func (p *lazyPager) close() {
	if p.stdin == nil {
		return
	}
	p.stdin.Close() //nolint:errcheck
	p.cmd.Wait()    //nolint:errcheck
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
