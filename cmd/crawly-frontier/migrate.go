package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/edevhub/crawly/internal/store/pgstore"
)

// migrate is the "migrate" command: it applies the pending migrations of this build
// ("up", the default) or lists them ("status").
func migrate(ctx context.Context, args []string, out io.Writer, log *slog.Logger) error {
	fs := flag.NewFlagSet("crawly-frontier migrate", flag.ContinueOnError)
	var dsn, table string
	fs.StringVar(&dsn, "postgres", os.Getenv(envPostgres),
		"PostgreSQL connection URL (or $"+envPostgres+"); it must reach the database directly, not through a transaction pooler")
	migrationsTableFlag(fs, &table)
	fs.Usage = func() {
		_, _ = fmt.Fprintln(fs.Output(), "usage: crawly-frontier migrate [flags] [up|status]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	action := "up"
	switch fs.NArg() {
	case 0:
	case 1:
		action = fs.Arg(0)
	default:
		return fmt.Errorf("unexpected arguments: %v", fs.Args()[1:])
	}
	if dsn == "" {
		return errors.New("no database: set -postgres or $" + envPostgres)
	}
	m, err := pgstore.NewMigrator(dsn, table, log)
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	switch action {
	case "up":
		applied, err := m.Up(ctx)
		for _, mig := range applied {
			_, _ = fmt.Fprintf(out, "applied %s\n", mig.Name)
		}
		if err != nil {
			return err
		}
		if len(applied) == 0 {
			_, _ = fmt.Fprintf(out, "the schema is up to date (version %d)\n", pgstore.SchemaVersion())
		}
		return nil
	case "status":
		all, err := m.Status(ctx)
		if err != nil {
			return err
		}
		for _, mig := range all {
			state := "pending"
			if !mig.AppliedAt.IsZero() {
				state = "applied " + mig.AppliedAt.UTC().Format(time.RFC3339)
			}
			_, _ = fmt.Fprintf(out, "%-40s %s\n", mig.Name, state)
		}
		return nil
	}
	return fmt.Errorf("unknown action %q: want up or status", action)
}
