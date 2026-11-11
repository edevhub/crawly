package pgstore_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/pgstore"
	"github.com/edevhub/crawly/internal/store/pgstore/pgtest"
	"github.com/edevhub/crawly/internal/store/storetest"
)

func labelled(label string) store.NewTask {
	nt := tasks("q", 1)[0]
	nt.Label = label
	return nt
}

// A run that has reached its label limit rejects further new labels without waiting for
// the run's label lock: a spider that keeps producing labels must not serialise the
// discovery of the whole run behind that lock.
func TestFullRunDoesNotTakeLabelLock(t *testing.T) {
	dsn := pgtest.Schema(t)
	s := open(t, dsn, limits(store.Limits{}))
	r := createRun(t, s, "r")
	res := must(s.AddTasks(ctx, store.AddRequest{Run: r, MaxLabels: 2, Tasks: []store.NewTask{labelled("a"), labelled("b")}}))(t)
	if res[0].Status != store.AddAdded || res[1].Status != store.AddAdded {
		t.Fatalf("%+v", res)
	}
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	tx := must(conn.Begin(ctx))(t)
	defer func() { _ = tx.Rollback(ctx) }()
	var no int64
	if err := tx.QueryRow(ctx, "SELECT run_no FROM runs WHERE run_id = 'r'").Scan(&no); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::int, $2::int)", pgstore.LabelLockClass, pgstore.LockKey(no)); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	res, err := s.AddTasks(ctx, store.AddRequest{Run: r, MaxLabels: 2, Tasks: []store.NewTask{labelled("c"), labelled("a")}})
	if err != nil || res[0].Status != store.AddInvalid || res[1].Status != store.AddAdded {
		t.Fatalf("over the limit while the lock is held: %+v, %v", res, err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("the batch waited %v", d)
	}
	// With room left, a new label has to take the lock, so it waits for it. This shows
	// that the test holds the right lock.
	if _, err := s.AddTasks(ctx, store.AddRequest{Run: r, MaxLabels: 3, Tasks: []store.NewTask{labelled("c")}}); err == nil {
		t.Fatal("a batch with room for its new label did not wait for the label lock")
	}
}

// The rows of the label statistics go with their run.
func TestRetentionDeletesLabelRows(t *testing.T) {
	dsn := pgtest.Schema(t)
	clock := storetest.NewClock()
	s := open(t, dsn, limits(store.Limits{}), pgstore.WithClock(clock.Now), pgstore.WithRetention(100*24*time.Hour, time.Hour))
	r := createRun(t, s, "r")
	must(s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: []store.NewTask{labelled("a")}}))(t)
	succeed(t, s, dispatch(t, s, 1)[0])
	must(s.SealRun(ctx, r))(t)
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	count := func(table string) (n int) {
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count("backlog_labels") != 1 || count("run_labels") != 1 {
		t.Fatal("the label rows were not written")
	}
	clock.Advance(25 * time.Hour)
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if count("backlog_labels") != 0 || count("run_labels") != 0 {
		t.Fatalf("label rows left after retention: %d, %d", count("backlog_labels"), count("run_labels"))
	}
}

// A database that has a newer migration but lacks an older one is not ready.
func TestSchemaWithAGap(t *testing.T) {
	dsn := pgtest.Schema(t)
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "UPDATE "+pgstore.DefaultMigrationsTable+" SET is_applied = false WHERE version_id = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := pgstore.New(ctx, dsn, limits(store.Limits{})); err == nil {
		t.Fatal("a database with a gap in its migrations was accepted")
	}
}
