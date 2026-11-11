package pgstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The queries that decide how fast a session finds work read one index range per
// handler, never a scan of the table: ADR 0002 says so, and these are its plans.
func TestQueryPlans(t *testing.T) {
	ctx := context.Background()
	dsn := schemaDSN(t)
	m, err := NewMigrator(dsn, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	if _, err := m.Up(ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE tasks_y2026m11 PARTITION OF tasks FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')`,
		`INSERT INTO pools (name) VALUES ('default')`,
		`INSERT INTO lanes (queue_key, run_no, pool_id, handler, ready_at)
			SELECT 'q' || q, 1, 1, 'h' || h, now() FROM generate_series(1, 300) q, generate_series(1, 5) h`,
		`INSERT INTO tasks (run_created_on, task_id, run_no, queue_key, pool_id, fingerprint, depth, spec, created_at, handler, priority)
			SELECT '2026-11-05', gen_random_uuid(), 1, 'q' || q, 1, decode(lpad(to_hex(n), 32, '0'), 'hex'), 0, '\x', now(), 'h' || h, n % 3
			FROM (SELECT q, h, row_number() OVER () AS n FROM generate_series(1, 300) q, generate_series(1, 5) h) s`,
		`ANALYSE lanes`, `ANALYSE tasks`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// The planner may prefer a scan of a table this small: with scans of tables off,
	// what is checked is that the query can be answered from the indexes it is built for.
	if _, err := conn.Exec(ctx, "SET enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	day := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	handlers := []string{"h1", "h2", "h3", ""}
	for _, tc := range []struct {
		name, sql string
		args      []any
		want      []string // index names the plan uses
		forbid    []string
	}{
		{"candidates of a session with handlers", candidatesByHandlerSQL,
			[]any{int16(1), now, 256, 64, handlers, 64}, []string{"lanes_ready"}, []string{"Seq Scan"}},
		{"claim", claimSQL,
			[]any{day, int64(1), "q1", int16(1), now, int64(1), "w", "v", now, now, 10, handlers},
			[]string{"pool_id_handler_priority"}, []string{"Seq Scan"}}, // the index of a partition made after the migration is named by its columns
		{"handlers that have lanes", unservedPairsSQL, []any{now}, []string{"lanes_ready"}, []string{"Seq Scan"}},
		{"a page of one handler's lanes", sweepLanesSQL, []any{int16(1), "h1", time.Time{}, "", int64(0), 50},
			[]string{"lanes_ready"}, []string{"Seq Scan", "Sort"}},
	} {
		rows, err := conn.Query(ctx, "EXPLAIN "+tc.sql, tc.args...)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		text := strings.Join(plan, "\n")
		for _, w := range tc.want {
			if !strings.Contains(text, w) {
				t.Errorf("%s: the plan does not use %s:\n%s", tc.name, w, text)
			}
		}
		for _, f := range tc.forbid {
			if strings.Contains(text, f) {
				t.Errorf("%s: the plan has %q:\n%s", tc.name, f, text)
			}
		}
	}
}
