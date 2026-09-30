// Command murmurd runs the assembled Murmur-SQL database daemon:
// the embedded engine with MySQL- and PostgreSQL-protocol frontends,
// configured by a TOML file.
//
//	murmurd --config /etc/murmurd/config.toml
//	murmurd --generate-config > config.toml
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/marcgauthier/murmur/murmurd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "murmurd: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config.toml", "path to config.toml")
	generate := flag.Bool("generate-config", false, "print an example config.toml and exit")
	version := flag.Bool("version", false, "print the daemon version and exit")
	flag.Parse()

	if *version {
		fmt.Println(murmurd.Version)
		return nil
	}
	if *generate {
		fmt.Print(murmurd.ExampleConfig)
		return nil
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := murmurd.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	srv, err := murmurd.New(cfg, logger)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("murmurd started",
		"version", murmurd.Version,
		"node", cfg.Murmur.NodeID,
		"mysql", srv.MySQLAddr(),
		"postgres", srv.PGAddr())
	if err := srv.Run(ctx); err != nil {
		return err
	}
	logger.Info("murmurd stopped")
	return nil
}
