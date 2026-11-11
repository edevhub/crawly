// Package pgtest gives tests a PostgreSQL database to themselves.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/edevhub/crawly/internal/store/pgstore"
)

// Env names the environment variable that holds the URL of a PostgreSQL database
// tests may use, for example postgres://postgres:crawly@localhost:55432/crawly.
const Env = "CRAWLY_TEST_POSTGRES"

// Require skips the test when Env is not set. Call it from the test's own goroutine
// before anything else uses Schema.
func Require(tb testing.TB) string {
	tb.Helper()
	base := os.Getenv(Env)
	if base == "" {
		tb.Skipf("%s is not set: no PostgreSQL to test against (make postgres starts one)", Env)
	}
	return base
}

// Schema creates a schema for the test, applies the migrations and returns a
// connection URL that uses it. The schema is dropped when the test ends. The test is
// skipped when Env is not set.
func Schema(tb testing.TB) string {
	tb.Helper()
	dsn := Empty(tb)
	Migrate(tb, dsn, "")
	return dsn
}

// Empty is Schema without the migrations: a database nobody has set up yet.
func Empty(tb testing.TB) string {
	tb.Helper()
	base := Require(tb)
	u, err := url.Parse(base)
	if err != nil {
		tb.Fatalf("%s: %v", Env, err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "crawly_test_" + hex.EncodeToString(b[:])
	exec(tb, base, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize())
	tb.Cleanup(func() { exec(tb, base, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE") })
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	return u.String()
}

// Migrate applies the migrations of this build, recorded in table.
func Migrate(tb testing.TB, dsn, table string) []pgstore.Migration {
	tb.Helper()
	m, err := pgstore.NewMigrator(dsn, table, nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	applied, err := m.Up(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	return applied
}

func exec(tb testing.TB, dsn, sql string) {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		tb.Fatalf("connecting to PostgreSQL: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, sql); err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
}
