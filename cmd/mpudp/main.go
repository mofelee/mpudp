// Command mpudp runs an MPUDP Peer until the process is asked to stop.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/mofelee/mpudp"
	"github.com/mofelee/mpudp/config"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := runContext(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func runContext(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runContextWithPeerFactory(ctx, args, stdout, stderr, func(ctx context.Context, cfg config.Config) (runtimePeer, error) {
		return mpudp.NewPeerContext(ctx, cfg)
	})
}

type runtimePeer interface {
	Mode() mpudp.Mode
	NewSession() (mpudp.Session, error)
	Errors() <-chan error
	Close() error
}

func runContextWithPeerFactory(ctx context.Context, args []string, stdout, stderr io.Writer, newPeer func(context.Context, config.Config) (runtimePeer, error)) int {
	if ctx == nil {
		fmt.Fprintln(stderr, "mpudp: context is required")
		return 2
	}
	flags := flag.NewFlagSet("mpudp", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the MPUDP YAML configuration")
	showVersion := flags.Bool("version", false, "print version and source commit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintf(stdout, "mpudp %s (commit %s)\n", version, commit)
		return 0
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
	cfg, err := config.Decode(file)
	_ = file.Close()
	if err != nil {
		fmt.Fprintf(stderr, "mpudp: %v\n", err)
		return 1
	}
	peer, err := newPeer(ctx, cfg)
	if err != nil {
		if ctx.Err() != nil {
			return 0
		}
		fmt.Fprintf(stderr, "mpudp: %v\n", err)
		return 1
	}
	if cfg.InitiatorEnabled() {
		if _, err := peer.NewSession(); err != nil {
			if ctx.Err() != nil {
				if closeErr := peer.Close(); closeErr != nil {
					fmt.Fprintf(stderr, "mpudp: close: %v\n", closeErr)
					return 1
				}
				return 0
			}
			fmt.Fprintf(stderr, "mpudp: start initiator Session: %v\n", err)
			_ = peer.Close()
			return 1
		}
	}
	fmt.Fprintf(stdout, "mpudp: running mode=%s\n", peer.Mode())
	for {
		select {
		case <-ctx.Done():
			if err := peer.Close(); err != nil {
				fmt.Fprintf(stderr, "mpudp: close: %v\n", err)
				return 1
			}
			return 0
		case err := <-peer.Errors():
			if err != nil {
				fmt.Fprintf(stderr, "mpudp: runtime: %v\n", err)
			}
		}
	}
}
