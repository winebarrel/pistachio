package pistachio

import (
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

func TestOptions_BeforeApply_NoColorAnyValueDisables(t *testing.T) {
	// Per https://no-color.org/, any non-empty value disables color.
	t.Setenv("NO_COLOR", "0")
	o := &Options{}
	require.NoError(t, o.BeforeApply())
	assert.False(t, o.Color, "NO_COLOR=0 still disables color per spec")
}

func TestOptions_BeforeApply_NoTTYDisablesColor(t *testing.T) {
	// `go test` runs with stdout piped (not a TTY), so isatty returns false
	// and BeforeApply must leave Color disabled even when NO_COLOR is unset.
	t.Setenv("NO_COLOR", "")
	o := &Options{}
	require.NoError(t, o.BeforeApply())
	assert.False(t, o.Color, "non-TTY stdout must disable Color")
}
