// Command proxy serves internal/dsqlproxy, for the DSQL scenarios to run
// against a local PostgreSQL server. It prints the address it listens on and
// serves until it is stopped.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/winebarrel/pistachio/internal/dsqlproxy"
)

func main() {
	listen := flag.String("l", "127.0.0.1:0", "address to listen on")
	backend := flag.String("backend", "", "connection string of the PostgreSQL server behind the proxy")
	flag.Parse()

	if *backend == "" {
		fmt.Fprintln(os.Stderr, "proxy: -backend is required")
		os.Exit(2)
	}

	p, err := dsqlproxy.Start(context.Background(), *listen, *backend)
	if err != nil {
		fmt.Fprintf(os.Stderr, "proxy: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(p.Addr())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	p.Close() //nolint:errcheck
}
