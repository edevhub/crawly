package pgstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

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
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "crawly_migration_" + hex.EncodeToString(b[:]) // lower case: the search path folds it
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
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

// beforeLanes opens a schema as the build before migration 2 left it.
func beforeLanes(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()
	db, err := sql.Open("pgx", schemaDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.UpTo(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	return db, p
}

func mustExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, q := range stmts {
		if _, err := db.ExecContext(context.Background(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// laneReadiness lists a queue's lanes as "handler=null|time", by handler.
func laneReadiness(t *testing.T, db *sql.DB, queue string) map[string]*time.Time {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "SELECT handler, ready_at FROM lanes WHERE queue_key = $1", queue)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]*time.Time{}
	for rows.Next() {
		var h string
		var at sql.NullTime
		if err := rows.Scan(&h, &at); err != nil {
			t.Fatal(err)
		}
		if at.Valid {
			out[h] = &at.Time
		} else {
			out[h] = nil
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// backlogReady is a backlog's ready_at, which the build before reads.
func backlogReady(t *testing.T, db *sql.DB, queue string) *time.Time {
	t.Helper()
	var at sql.NullTime
	if err := db.QueryRowContext(context.Background(), "SELECT ready_at FROM backlogs WHERE queue_key = $1", queue).Scan(&at); err != nil {
		t.Fatal(err)
	}
	if !at.Valid {
		return nil
	}
	return &at.Time
}

// The backlogs that wait when migration 2 runs get a lane for tasks of unknown handler,
// with the readiness they had; backlogs without pending tasks get none.
func TestLanesBackfill(t *testing.T) {
	db, p := beforeLanes(t)
	mustExec(t, db,
		`INSERT INTO pools (name) VALUES ('default')`,
		`INSERT INTO queues (key, bucket, last_used_at) VALUES ('a', 1, now()), ('b', 2, now()), ('c', 3, now())`,
		`INSERT INTO backlogs (queue_key, run_no, pool_id, bucket, run_created_on, pending, ready_at) VALUES
			('a', 1, 1, 1, current_date, 3, now()),
			('b', 1, 1, 2, current_date, 2, NULL),
			('c', 1, 1, 3, current_date, 0, NULL)`)
	if _, err := p.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := laneReadiness(t, db, "a"); len(got) != 1 || got[""] == nil {
		t.Fatalf("a: %v", got)
	}
	if got := laneReadiness(t, db, "b"); len(got) != 1 || got[""] != nil {
		t.Fatalf("b: want a lane with no readiness: %v", got)
	}
	if got := laneReadiness(t, db, "c"); len(got) != 0 {
		t.Fatalf("c: want no lane: %v", got)
	}
}

// The build before migration 2 keeps working on the migrated schema while the two
// builds run side by side. Its statements below are copies from that build.
func TestMigrationKeepsPreviousBuildWorking(t *testing.T) {
	ctx := context.Background()
	db, p := beforeLanes(t)
	mustExec(t, db,
		`CREATE TABLE tasks_y2026m11 PARTITION OF tasks FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')`,
		`INSERT INTO pools (name) VALUES ('default')`,
		`INSERT INTO queues (key, bucket, last_used_at) VALUES ('a', 1, now()), ('b', 2, now())`)
	if _, err := p.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if err := buildTaskIndexes(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := buildTaskIndexes(ctx, db); err != nil { // finishing what exists is a no-op
		t.Fatal(err)
	}

	// What the build before reads and writes is still there.
	var n int
	for _, q := range []string{
		`SELECT count(*) FROM pg_attribute WHERE attrelid = 'backlogs'::regclass AND attname = 'ready_at' AND NOT attisdropped`,
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'backlogs_ready'`,
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'tasks' AND indexdef LIKE '%(run_no, queue_key, pool_id, priority DESC, task_id)%'`,
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'tasks' AND indexdef LIKE '%(run_no, queue_key, pool_id, not_before)%'`,
	} {
		if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil || n == 0 {
			t.Fatalf("%s: %d %v", q, n, err)
		}
	}
	// The partition that existed has the new indexes, built and attached, and so is the
	// parent's.
	for _, parent := range []string{"tasks_claim_h", "tasks_next_h"} {
		var valid bool
		var attached int
		if err := db.QueryRowContext(ctx, `SELECT indisvalid, (SELECT count(*) FROM pg_inherits WHERE inhparent = indexrelid)
			FROM pg_index WHERE indexrelid = $1::regclass`, parent).Scan(&valid, &attached); err != nil || !valid || attached != 1 {
			t.Fatalf("%s: valid %v, %d partitions attached: %v", parent, valid, attached, err)
		}
	}
	// A partition made later gets them from the parent.
	mustExec(t, db, `CREATE TABLE tasks_y2026m12 PARTITION OF tasks FOR VALUES FROM ('2026-12-01') TO ('2027-01-01')`)
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_inherits WHERE inhparent = 'tasks_claim_h'::regclass`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("a new partition has %d indexes of tasks_claim_h: %v", n, err)
	}

	// The build before adds work: it sets the backlog's ready_at and knows no lanes.
	// A task it inserts has no handler.
	oldUpsert := `INSERT INTO backlogs (queue_key, run_no, pool_id, bucket, run_created_on, pending, tasks_total, discovered, duplicates, over_budget, ready_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9, $10)
		ON CONFLICT (queue_key, run_no, pool_id) DO UPDATE SET
			pending = backlogs.pending + EXCLUDED.pending, tasks_total = backlogs.tasks_total + EXCLUDED.tasks_total,
			discovered = backlogs.discovered + EXCLUDED.discovered, duplicates = backlogs.duplicates + EXCLUDED.duplicates,
			over_budget = backlogs.over_budget + EXCLUDED.over_budget, ready_at = least(backlogs.ready_at, EXCLUDED.ready_at)`
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := db.ExecContext(ctx, oldUpsert, "a", 1, 1, 1, "2026-11-05", 2, 0, 0, 0, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tasks (run_created_on, task_id, run_no, queue_key, pool_id, fingerprint, depth, spec, created_at)
		VALUES ('2026-11-05', gen_random_uuid(), 1, 'a', 1, '\x00000000000000000000000000000001', 0, '\x', now())`); err != nil {
		t.Fatal(err)
	}
	var handler string
	if err := db.QueryRowContext(ctx, `SELECT handler FROM tasks WHERE queue_key = 'a'`).Scan(&handler); err != nil || handler != "" {
		t.Fatalf("a task the build before inserted has handler %q: %v", handler, err)
	}
	if got := laneReadiness(t, db, "a"); len(got) != 1 || got[""] == nil || !got[""].Equal(now) {
		t.Fatalf("the lane of unknown handler is missing or late after the build before added work: %v", got)
	}
	// Its claim reads the old index in the order of priority.
	plan := explain(t, db, `EXPLAIN SELECT task_id FROM tasks WHERE run_created_on = '2026-11-05' AND run_no = 1 AND queue_key = 'a' AND pool_id = 1
		AND state = 'pending' AND not_before <= now() ORDER BY priority DESC, task_id LIMIT 10`)
	if strings.Contains(plan, "Sort") {
		t.Fatalf("the build before sorts its claim:\n%s", plan)
	}

	// The build before changes readiness with a plain update: a lane follows it.
	later := now.Add(time.Hour)
	mustExec(t, db, `UPDATE backlogs SET ready_at = 'infinity' WHERE queue_key = 'a'`)
	if got := laneReadiness(t, db, "a"); got[""] == nil || !got[""].Equal(now) {
		t.Fatalf("infinity must leave the lane alone: %v", got)
	}
	// This build works on lanes: the backlog's ready_at, which the build before reads
	// to find work, follows them. The earliest lane counts.
	mustExec(t, db,
		`INSERT INTO lanes (queue_key, run_no, pool_id, handler, ready_at) VALUES ('b', 1, 1, 'page', now() + interval '1 hour'), ('b', 1, 1, 'item', now() + interval '2 hours')`,
		`INSERT INTO backlogs (queue_key, run_no, pool_id, bucket, run_created_on, pending) VALUES ('b', 1, 1, 2, '2026-11-05', 2)`)
	mustExec(t, db, `UPDATE lanes SET ready_at = ready_at WHERE queue_key = 'b'`)
	if got := backlogReady(t, db, "b"); got == nil || got.Before(later.Add(-time.Minute)) || got.After(later.Add(2*time.Minute)) {
		t.Fatalf("the backlog's ready_at %v: want the earliest lane, an hour from now", got)
	}
	mustExec(t, db, `UPDATE lanes SET ready_at = NULL WHERE queue_key = 'b'`)
	if got := backlogReady(t, db, "b"); got != nil {
		t.Fatalf("a paused backlog must not be ready: %v", got)
	}
	mustExec(t, db, `DELETE FROM lanes WHERE queue_key = 'b'`)
	if got := backlogReady(t, db, "b"); got != nil {
		t.Fatalf("no lanes, no readiness: %v", got)
	}
	// ... and the build before, resuming the run, wakes the lanes again.
	mustExec(t, db, `INSERT INTO lanes (queue_key, run_no, pool_id, handler, ready_at) VALUES ('b', 1, 1, 'page', NULL)`)
	mustExec(t, db, `UPDATE backlogs SET ready_at = now() WHERE queue_key = 'b'`)
	if got := laneReadiness(t, db, "b"); got["page"] == nil {
		t.Fatalf("the build before resumed the backlog and its lane stays paused: %v", got)
	}
}

// Rolling back: the down migration leaves the schema the build before runs on.
func TestMigrationDown(t *testing.T) {
	ctx := context.Background()
	db, p := beforeLanes(t)
	mustExec(t, db,
		`INSERT INTO pools (name) VALUES ('default')`,
		`INSERT INTO queues (key, bucket, last_used_at) VALUES ('a', 1, now())`,
		`INSERT INTO backlogs (queue_key, run_no, pool_id, bucket, run_created_on, pending, ready_at) VALUES ('a', 1, 1, 1, current_date, 3, now())`)
	if _, err := p.Up(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `UPDATE lanes SET ready_at = now() + interval '1 hour' WHERE queue_key = 'a'`)
	want := backlogReady(t, db, "a")
	if _, err := p.DownTo(ctx, 1); err != nil { // the migrations after it go first
		t.Fatal(err)
	}
	if got := backlogReady(t, db, "a"); got == nil || !got.Equal(*want) {
		t.Fatalf("after Down the backlog's ready_at is %v, want %v", got, want)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_class WHERE relname IN ('lanes', 'pool_handlers', 'unserved_sweep', 'tasks_claim_h')`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d objects of migration 2 are left: %v", n, err)
	}
	if _, err := p.Up(ctx); err != nil { // and it can be applied again
		t.Fatal(err)
	}
}

// explain returns the plan of a statement.
func explain(t *testing.T, db *sql.DB, stmt string) string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), stmt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan.String()
}
