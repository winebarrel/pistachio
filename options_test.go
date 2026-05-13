package pistachio

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptions_BeforeApply_NoColorEnvDisablesColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	o := &Options{}
	require.NoError(t, o.BeforeApply())
	assert.False(t, o.Color, "NO_COLOR must disable Color regardless of TTY state")
}

func TestOptions_BeforeApply_NoColorZeroValueDisables(t *testing.T) {
	// Per https://no-color.org/, presence regardless of value disables color.
	// "0" is intentionally not a magic "off" value.
	t.Setenv("NO_COLOR", "0")
	o := &Options{}
	require.NoError(t, o.BeforeApply())
	assert.False(t, o.Color, "NO_COLOR=0 still disables color per spec")
}

func TestOptions_BeforeApply_NoColorEmptyValueDisables(t *testing.T) {
	// Spec: presence regardless of value disables color, including the empty
	// string. os.Getenv cannot distinguish "unset" from "set to empty", so the
	// implementation uses os.LookupEnv.
	t.Setenv("NO_COLOR", "")
	o := &Options{}
	require.NoError(t, o.BeforeApply())
	assert.False(t, o.Color, "NO_COLOR set to empty must still disable color")
}

func TestOptions_BeforeApply_NoTTYDisablesColor(t *testing.T) {
	// Exercise the !TTY branch deterministically: unset NO_COLOR (so the
	// TTY check is what decides), and replace os.Stdout with the write end
	// of a pipe so isatty.IsTerminal returns false regardless of how the
	// test was launched (CI vs. interactive `go test`).
	unsetNoColor(t)
	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		r.Close()
		w.Close()
	})
	orig := os.Stdout
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = orig })

	o := &Options{}
	require.NoError(t, o.BeforeApply())
	assert.False(t, o.Color, "non-TTY stdout must disable Color")
}

// unsetNoColor removes NO_COLOR for the duration of a single test, restoring
// any pre-existing value on cleanup. t.Setenv has no "unset" counterpart, so
// we manage it manually here.
func unsetNoColor(t *testing.T) {
	t.Helper()
	prev, had := os.LookupEnv("NO_COLOR")
	if had {
		require.NoError(t, os.Unsetenv("NO_COLOR"))
		t.Cleanup(func() { _ = os.Setenv("NO_COLOR", prev) })
	}
}
