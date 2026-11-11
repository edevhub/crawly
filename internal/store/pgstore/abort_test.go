package pgstore_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/pgstore"
	"github.com/edevhub/crawly/internal/store/pgstore/pgtest"
)

func exec(t *testing.T, dsn, sql string) {
	t.Helper()
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func fail(t *testing.T, s store.Store, a store.Assignment) {
	t.Helper()
	if st := must(s.Finish(ctx, store.Finish{Lease: a.Lease.Ref, Source: store.SourceWorker, Result: store.ResultFailed,
		Record: store.AttemptRecord{Code: "x"}}))(t); st != store.FinishAccepted {
		t.Fatalf("finish: %v", st)
	}
}

// Abort rules are accepted only while no replica that predates them is running: such a
// replica takes an aborted run for a live one. A replica that stopped reporting does not
// count, and a run that exists is answered as before.
func TestAbortRulesNeedEveryReplica(t *testing.T) {
	dsn := pgtest.Schema(t)
	s := open(t, dsn, limits(store.Limits{}))
	if err := s.Start(ctx, store.Replica{ID: "new", PolicyHash: "test"}); err != nil {
		t.Fatal(err)
	}
	// A replica of an earlier build registers without the column, which defaults to false.
	exec(t, dsn, "INSERT INTO replicas (replica_id, policy_hash, started_at, seen_at) VALUES ('old', 'test', now(), now())")
	rules := store.RunSpec{ID: "r", Hash: []byte("r"), Limits: store.RunLimits{MaxFailed: 3}}
	if _, err := s.CreateRun(ctx, rules); !errors.Is(err, store.ErrUnsupported) {
		t.Fatalf("rules with an old replica running: %v", err)
	}
	if _, err := s.GetRun(ctx, "r"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the refused run was created: %v", err)
	}
	if _, err := s.CreateRun(ctx, store.RunSpec{ID: "plain", Hash: []byte("plain")}); err != nil {
		t.Fatalf("a run without rules: %v", err)
	}
	exec(t, dsn, "UPDATE replicas SET seen_at = now() - interval '5 minutes' WHERE replica_id = 'old'")
	if _, err := s.CreateRun(ctx, rules); err != nil {
		t.Fatalf("the old replica stopped reporting: %v", err)
	}
	exec(t, dsn, "UPDATE replicas SET seen_at = now() WHERE replica_id = 'old'")
	if _, err := s.CreateRun(ctx, rules); err != nil {
		t.Fatalf("a retry of a run that exists: %v", err)
	}
}

// A database with data from before the migration is migrated in place, and an earlier
// replica that registered keeps the rules out until it is gone.
func TestMigrationOfPopulatedDatabase(t *testing.T) {
	dsn := pgtest.Empty(t)
	m := must(pgstore.NewMigrator(dsn, "", nil))(t)
	defer func() { _ = m.Close() }()
	if err := m.UpTo(ctx, 1); err != nil {
		t.Fatal(err)
	}
	exec(t, dsn, `INSERT INTO runs (run_id, spec_hash, created_on, policy, key_rule, created_at, state)
		VALUES ('old-open', 'x', current_date, '', 1, now(), 'open'), ('old-cancelled', 'y', current_date, '', 1, now(), 'cancelled')`)
	exec(t, dsn, "INSERT INTO policy_sets (hash, first_seen, activated_at) VALUES ('test', now(), now())")
	exec(t, dsn, "INSERT INTO replicas (replica_id, policy_hash, started_at, seen_at) VALUES ('old', 'test', now(), now())")
	applied := must(m.Status(ctx))(t)
	if applied[0].AppliedAt.IsZero() {
		t.Fatal("version 1 is not applied")
	}
	if _, err := m.Up(ctx); err != nil {
		t.Fatal(err)
	}
	s := open(t, dsn, limits(store.Limits{}))
	if r := must(s.GetRun(ctx, "old-open"))(t); r.State != store.RunOpen || r.Spec.Limits.HasAbortRules() {
		t.Fatalf("a run from before: %+v", r)
	}
	if _, err := s.CreateRun(ctx, store.RunSpec{ID: "n", Hash: []byte("n"), Limits: store.RunLimits{MaxFailed: 1}}); !errors.Is(err, store.ErrUnsupported) {
		t.Fatalf("rules while the registered old replica reports: %v", err)
	}
}

// Rolling back runs the Down step first: it turns aborted runs into cancelled ones, which
// the earlier builds know, and puts the index of draining runs back.
func TestRollbackTurnsAbortedRunsIntoCancelled(t *testing.T) {
	dsn := pgtest.Schema(t)
	s := open(t, dsn, limits(store.Limits{}))
	r := must(s.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r"), Limits: store.RunLimits{MaxFailed: 1}}))(t).Spec.ID
	must(s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 2)}))(t)
	a := dispatch(t, s, 1)[0]
	fail(t, s, a)
	if st := must(s.GetRun(ctx, r))(t); st.State != store.RunAborted {
		t.Fatalf("%v", st.State)
	}
	s.Close()

	m := must(pgstore.NewMigrator(dsn, "", nil))(t)
	defer func() { _ = m.Close() }()
	if err := m.DownTo(ctx, 1); err != nil {
		t.Fatal(err)
	}
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	var state, def string
	if err := conn.QueryRow(ctx, "SELECT state::text FROM runs WHERE run_id = 'r'").Scan(&state); err != nil || state != "cancelled" {
		t.Fatalf("state %q: %v", state, err)
	}
	if err := conn.QueryRow(ctx, "SELECT indexdef FROM pg_indexes WHERE indexname = 'runs_draining' AND schemaname = current_schema()").Scan(&def); err != nil {
		t.Fatal(err)
	}
	if want := "WHERE (state = ANY (ARRAY['cancelled'::run_state, 'expired'::run_state]))"; !contains(def, want) {
		t.Fatalf("the index is %s", def)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Many final failures of one run on many queues, at once, with AddTasks on the run all
// the time: no Finish or AddTasks fails (a lock timeout would), nothing deadlocks, and
// the counters are exact. The abort comes at the threshold, and only once.
func TestFailureStormOnOneRun(t *testing.T) {
	dsn := pgtest.Schema(t)
	s := open(t, dsn, limits(store.Limits{}))
	const queues = 64
	r := must(s.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r"),
		Limits: store.RunLimits{MaxFailed: queues + 1, MaxConsecutiveFailures: 1_000_000}}))(t).Spec.ID
	for i := range queues {
		must(s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks(fmt.Sprintf("q%d", i), 1)}))(t)
	}
	as := dispatch(t, s, queues)
	if len(as) != queues {
		t.Fatalf("dispatched %d", len(as))
	}
	stop := make(chan struct{})
	var adders sync.WaitGroup
	var addErrs []error
	var mu sync.Mutex
	for range 4 {
		adders.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks(fmt.Sprintf("extra%d", i%8), 1)}); err != nil {
					mu.Lock()
					addErrs = append(addErrs, err)
					mu.Unlock()
				}
			}
		})
	}
	errs := make([]error, queues)
	var wg sync.WaitGroup
	for i := range as {
		wg.Go(func() {
			st, err := s.Finish(ctx, store.Finish{Lease: as[i].Lease.Ref, Source: store.SourceWorker, Result: store.ResultFailed,
				Record: store.AttemptRecord{Code: "x"}})
			if err == nil && st != store.FinishAccepted {
				err = fmt.Errorf("status %v", st)
			}
			errs[i] = err
		})
	}
	wg.Wait()
	close(stop)
	adders.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("finish %d: %v", i, err)
		}
	}
	for _, err := range addErrs {
		t.Errorf("AddTasks: %v", err)
	}
	if got := must(s.GetRun(ctx, r))(t); got.State != store.RunOpen || got.Stats.Failed != queues {
		t.Fatalf("%v %+v", got.State, got.Stats)
	}
	// One more failure reaches the threshold: the counter was exact.
	must(s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("last", 1)}))(t)
	var last store.Assignment
	for _, a := range dispatch(t, s, 100) {
		last = a
		break
	}
	fail(t, s, last)
	if got := must(s.GetRun(ctx, r))(t); got.State != store.RunAborted || got.Abort == nil || got.Abort.Rule != store.AbortMaxFailed {
		t.Fatalf("%v %+v", got.State, got.Abort)
	}
	if n := s.Deadlocks(); n != 0 {
		t.Errorf("%d deadlocks retried", n)
	}
}

// A run without abort rules never touches its row: a lock on the row held by someone
// else does not stop its tasks from finishing.
func TestRulelessRunLeavesItsRowAlone(t *testing.T) {
	dsn := pgtest.Schema(t)
	s := open(t, dsn, limits(store.Limits{}))
	r := createRun(t, s, "r")
	must(s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 2)}))(t)
	as := dispatch(t, s, 2)

	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	tx := must(conn.Begin(ctx))(t)
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT 1 FROM runs WHERE run_id = 'r' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		fail(t, s, as[0])
		succeed(t, s, as[1])
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("finishing a task of a run without rules waited for the run's row")
	}
}
