package pgstore

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
)

// schemaDSN creates a schema of its own in the test database and returns a URL that
// uses it. (pgtest cannot be imported here: it imports this package.)
func schemaDSN(t *testing.T) string {
	t.Helper()
	base := os.Getenv("CRAWLY_TEST_POSTGRES")
	if base == "" {
		t.Skip("CRAWLY_TEST_POSTGRES is not set")
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	name := "crawly_lanes_backfill"
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+ident+" CASCADE; CREATE SCHEMA "+ident); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(ctx, base)
		if err == nil {
			_, _ = c.Exec(ctx, "DROP SCHEMA "+ident+" CASCADE")
			_ = c.Close(ctx)
		}
	})
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	return u.String()
}

// The backlogs that wait when migration 2 runs get a lane for tasks of unknown handler,
// with the readiness they had; backlogs without pending tasks get none.
func TestLanesBackfill(t *testing.T) {
	dsn := schemaDSN(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := p.UpTo(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO pools (name) VALUES ('default')`,
		`INSERT INTO queues (key, bucket, last_used_at) VALUES ('a', 1, now()), ('b', 2, now()), ('c', 3, now())`,
		`INSERT INTO backlogs (queue_key, run_no, pool_id, bucket, run_created_on, pending, ready_at) VALUES
			('a', 1, 1, 1, current_date, 3, now()),
			('b', 1, 1, 2, current_date, 2, NULL),
			('c', 1, 1, 3, current_date, 0, NULL)`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if _, err := p.Up(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT queue_key, handler, ready_at IS NULL FROM lanes ORDER BY queue_key")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	type lane struct {
		queue, handler string
		null           bool
	}
	var got []lane
	for rows.Next() {
		var l lane
		if err := rows.Scan(&l.queue, &l.handler, &l.null); err != nil {
			t.Fatal(err)
		}
		got = append(got, l)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []lane{{"a", "", false}, {"b", "", true}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("lanes %+v, want %+v", got, want)
	}
}
