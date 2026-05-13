package main

import (
	"context"
	"io"
	"os"

	"github.com/alecthomas/kong"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/cmd/command"
)

var version string

var cli struct {
	pistachio.Options
	Version kong.VersionFlag

	Apply command.Apply `cmd:"" help:"Apply schema changes to the database."`
	Plan  command.Plan  `cmd:"" help:"Print the schema diff SQL without applying it."`
	Dump  command.Dump  `cmd:"" help:"Dump the current database schema as SQL."`
}

func main() {
	ctx := context.Background()
	// Parse first so cli.Options.Color is populated (BeforeApply seeds it
	// from isatty/NO_COLOR, then any explicit --color/--no-color overrides).
	kctx := kong.Parse(&cli, kong.Vars{"version": version})

	out := command.NewSQLWriter(os.Stdout, cli.Color)
	kctx.BindTo(ctx, (*context.Context)(nil))
	kctx.BindTo(out, (*io.Writer)(nil))

	client := pistachio.NewClient(&cli.Options)
	runErr := kctx.Run(client)
	// Flush write errors (broken pipe when piping to `head`, etc.) are
	// intentionally discarded so `pista plan | head` exits cleanly, matching
	// the prior behavior where each per-Fprintf write error was ignored.
	out.Flush() //nolint:errcheck
	kctx.FatalIfErrorf(runErr)
}
