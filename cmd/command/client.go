package command

import (
	"github.com/alecthomas/kong"
	"github.com/winebarrel/pistachio"
)

// bindClient binds the client that Run receives. Each command that reads a
// database carries its own connection flags, so the client is built from the
// running command rather than once for the whole CLI. fmt reads no database
// and builds none.
func bindClient(kctx *kong.Context, options *pistachio.Options) {
	kctx.Bind(pistachio.NewClient(options))
}
