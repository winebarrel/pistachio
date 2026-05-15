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
	NoPager bool `name:"no-pager" help:"Disable paging of long output via $PISTA_PAGER."`

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

	w, closePager, err := command.StartPager(os.Stdout, cli.NoPager)
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
	kctx.FatalIfErrorf(err)
}
