// Command mpudp validates an MPUDP configuration. Network runtime assembly is
// intentionally deferred to later implementation loops.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/mofelee/mpudp"
	"github.com/mofelee/mpudp/config"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("mpudp", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the MPUDP YAML configuration")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "mpudp: -config is required")
		return 2
	}
	file, err := os.Open(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "mpudp: read configuration: %v\n", err)
		return 1
	}
	defer file.Close()
	cfg, err := config.Decode(file)
	if err != nil {
		fmt.Fprintf(stderr, "mpudp: %v\n", err)
		return 1
	}
	peer, err := mpudp.NewPeer(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "mpudp: %v\n", err)
		return 1
	}
	defer peer.Close()
	fmt.Fprintf(stdout, "configuration valid: mode=%s\n", peer.Mode())
	return 0
}
