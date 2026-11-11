package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store/pgstore/pgtest"
)

func TestMigrateCommand(t *testing.T) {
	dsn := pgtest.Empty(t)
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)
	const table = "frontier_history"
	run := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := migrate(ctx, append([]string{"-postgres", dsn, "-postgres-migrations-table", table}, args...), &out, log); err != nil {
			t.Fatalf("migrate %v: %v", args, err)
		}
		return out.String()
	}

	if out := run("status"); !strings.Contains(out, "pending") || strings.Contains(out, "applied") {
		t.Fatalf("status before the migration:\n%s", out)
	}
	if out := run(); !strings.Contains(out, "applied 00001_init.sql") {
		t.Fatalf("up:\n%s", out)
	}
	if out := run("up"); !strings.Contains(out, "up to date") {
		t.Fatalf("a second up:\n%s", out)
	}
	if out := run("status"); strings.Contains(out, "pending") {
		t.Fatalf("status after the migration:\n%s", out)
	}

	// The server finds the schema through the same table name, and not without it.
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, closeStore, err := openStore(ctx, config{postgres: dsn, migrationsTable: table}, pol, log)
	if err != nil {
		t.Fatal(err)
	}
	closeStore()
	if _, _, err := openStore(ctx, config{postgres: dsn}, pol, log); err == nil {
		t.Fatal("the server started against a table that holds no migrations")
	}

	if err := migrate(ctx, []string{"-postgres", dsn, "down"}, io.Discard, log); err == nil {
		t.Fatal("an unknown action was accepted")
	}
}
