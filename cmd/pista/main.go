package main

import (
	"context"
	"errors"
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
	Pager   *bool `name:"pager" negatable:"" help:"Force paging via $PISTA_PAGER even when stdout is not a TTY. PISTA_PAGER must be set."`

	Apply command.Apply `cmd:"" help:"Apply schema changes to the database."`
	Plan  command.Plan  `cmd:"" help:"Print the schema diff SQL without applying it."`
	Dump  command.Dump  `cmd:"" help:"Dump the current database schema as SQL."`
}

func main() {
	ctx := context.Background()
	kctx := kong.Parse(&cli,
		kong.Vars{"version": version},
		kong.BindTo(ctx, (*context.Context)(nil)),
		kong.BindTo(os.Stdout, (*io.Writer)(nil)),
	)

	w, closePager, err := command.StartPager(os.Stdout, cli.Pager)
	kctx.FatalIfErrorf(err)
	// Defer covers panics; the explicit closePager() below covers the
	// os.Exit path inside FatalIfErrorf so the pager always finishes
	// flushing before the parent exits.
	defer closePager()
	if w != io.Writer(os.Stdout) {
		kctx.BindTo(w, (*io.Writer)(nil))
	}

	client := pistachio.NewClient(&cli.Options)
	err = kctx.Run(client)
	closePager()
	// plan --check: diffs are reported via exit code 2, not as a fatal
	// error. The plan output has already been written at this point.
	if errors.Is(err, command.ErrPlanDiff) {
		kctx.Exit(2)
	}
	kctx.FatalIfErrorf(err)
}
