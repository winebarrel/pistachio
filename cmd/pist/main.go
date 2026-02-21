package main

import (
	"github.com/alecthomas/kong"
	"github.com/winebarrel/pistachio"
)

var version string

var cli struct {
	pistachio.Options
	Version kong.VersionFlag

	Dump pistachio.Dump `cmd:"" help:"<TODO>"`
}

func main() {
	kctx := kong.Parse(&cli, kong.Vars{"version": version})
	err := kctx.Run(&cli.Options)
	kctx.FatalIfErrorf(err)
}
