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

// cli holds only what every command shares. The connection flags sit on the
// commands that open a connection, so kong does not offer them to fmt, which
// reads no database, or to parse, which needs the schema name alone.
type cli struct {
	Config  kong.ConfigFlag `short:"C" name:"config" placeholder:"FILE" env:"PISTA_CONFIG" help:"Load options from a YAML file."`
	Version kong.VersionFlag
	Pager   *bool `name:"pager" negatable:"" help:"Force paging via $PISTA_PAGER even when stdout is not a TTY. PISTA_PAGER must be set."`

	Apply command.Apply `cmd:"" help:"Apply schema changes to the database."`
	Plan  command.Plan  `cmd:"" help:"Print the schema diff SQL without applying it."`
	Diff  command.Diff  `cmd:"" help:"Print the DDL that takes one schema SQL file to another. No database is read."`
	Dump  command.Dump  `cmd:"" help:"Dump the current database schema as SQL."`
	Fmt   command.Fmt   `cmd:"" help:"Format schema SQL files in place."`
	Parse command.Parse `cmd:"" help:"Parse schema SQL files and print the result as JSON."`
}

func main() {
	run(os.Args[1:], os.Stdout, os.Stderr, os.Exit)
}

// run parses args and runs the command. The command writes to stdout, which
// is also where the pager writes when one is started, and stderr takes the
// usage and error messages. exit stands in for os.Exit and is not expected
// to return; a test passes one that panics.
func run(args []string, stdout, stderr io.Writer, exit func(int)) {
	var cli cli
	ctx := context.Background()
	parser, err := kong.New(&cli,
		kong.Vars{"version": version},
		kong.Configuration(pistachio.YAMLConfig),
		kong.Writers(stdout, stderr),
		kong.Exit(exit),
		kong.BindTo(ctx, (*context.Context)(nil)),
		kong.BindTo(stdout, (*io.Writer)(nil)),
	)
	if err != nil {
		panic(err)
	}
	kctx, err := parser.Parse(args)
	parser.FatalIfErrorf(err)

	w, closePager, err := command.StartPager(stdout, cli.Pager)
	kctx.FatalIfErrorf(err)
	// Defer covers panics; the explicit closePager() below covers the
	// os.Exit path inside FatalIfErrorf so the pager always finishes
	// flushing before the parent exits.
	defer closePager()
	if w != stdout {
		kctx.BindTo(w, (*io.Writer)(nil))
	}

	err = kctx.Run()
	closePager()
	// plan --check, diff --check and fmt --check report a difference as exit
	// code 2 instead of a fatal error. The output has already been written.
	if errors.Is(err, command.ErrPlanDiff) || errors.Is(err, command.ErrDiffChanges) || errors.Is(err, command.ErrFormatDiff) {
		kctx.Exit(2)
		return
	}
	kctx.FatalIfErrorf(err)
}
