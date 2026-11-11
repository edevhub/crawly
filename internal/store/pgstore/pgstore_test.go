package pgstore_test

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/pgstore"
	"github.com/edevhub/crawly/internal/store/pgstore/pgtest"
	"github.com/edevhub/crawly/internal/store/storetest"
)

var ctx = context.Background()

func open(t *testing.T, dsn string, pol store.Policies, opts ...pgstore.Option) *pgstore.Store {
	t.Helper()
	s, err := pgstore.New(ctx, dsn, pol, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// policies is a fixed policy set with a name, so a test can change the set.
type policies struct {
	hash string
	eff  store.Effective
}

func (p policies) Hash() string { return p.hash }

func (p policies) For(store.RunID, *policyv1.RunPolicy, store.QueueKey) store.Effective { return p.eff }

func limits(l store.Limits) policies {
	return policies{hash: "test", eff: store.Effective{Limits: l, Queue: l, Weight: 1,
		Lease: store.LeaseSettings{Duration: time.Minute, MaxDuration: 10 * time.Minute}}}
}

var fingerprints atomic.Uint64

func tasks(queue string, n int) []store.NewTask {
	out := make([]store.NewTask, n)
	for i := range out {
		binary.BigEndian.PutUint64(out[i].Fingerprint[:], fingerprints.Add(1))
		out[i].Queue = store.QueueKey(queue)
	}
	return out
}

func must[T any](v T, err error) func(*testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func createRun(t *testing.T, s store.Store, id string) store.RunID {
	t.Helper()
	must(s.CreateRun(ctx, store.RunSpec{ID: store.RunID(id), Hash: []byte(id)}))(t)
	return store.RunID(id)
}

func dispatch(t *testing.T, s store.Store, slots int) []store.Assignment {
	t.Helper()
	return must(s.Dispatch(ctx, store.DispatchRequest{Session: "s", Pool: store.DefaultPool, Slots: slots}))(t).Assignments
}

func succeed(t *testing.T, s store.Store, a store.Assignment) {
	t.Helper()
	if st := must(s.Finish(ctx, store.Finish{Lease: a.Lease.Ref, Source: store.SourceWorker, Result: store.ResultSucceeded}))(t); st != store.FinishAccepted {
		t.Fatalf("finish: %v", st)
	}
}

func TestConformance(t *testing.T) {
	pgtest.Require(t)
	storetest.Run(t, func(pol store.Policies, now func() time.Time) store.Store {
		return open(t, pgtest.Schema(t), pol, pgstore.WithClock(now))
	})
}

// A new replica on the same database sees the runs, the tasks and the leases of the
// one before it.
func TestSurvivesRestart(t *testing.T) {
	dsn := pgtest.Schema(t)
	first, err := pgstore.New(ctx, dsn, limits(store.Limits{}))
	if err != nil {
		t.Fatal(err)
	}
	r := createRun(t, first, "r")
	must(first.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 3)}))(t)
	held := dispatch(t, first, 1)
	if len(held) != 1 {
		t.Fatalf("dispatched %d", len(held))
	}
	first.Close()

	second := open(t, dsn, limits(store.Limits{}))
	if st := must(second.GetRun(ctx, r))(t).Stats; st.Pending != 2 || st.InFlight != 1 {
		t.Fatalf("after the restart: %+v", st)
	}
	info := must(second.Lease(ctx, held[0].Lease.Ref))(t)
	if info.Task != held[0].Info.Task || info.Run != r {
		t.Fatalf("the lease of the first replica reads %+v", info)
	}
	succeed(t, second, held[0])
	for _, a := range dispatch(t, second, 10) {
		succeed(t, second, a)
	}
	run := must(second.SealRun(ctx, r))(t)
	if run.State != store.RunCompleted || run.Stats.Succeeded != 3 {
		t.Fatalf("after finishing on the second replica: %v %+v", run.State, run.Stats)
	}
	task := must(second.GetTask(ctx, r, held[0].Info.Task, nil))(t)
	if task.State != store.TaskSucceeded || len(task.Attempts) != 1 {
		t.Fatalf("task: %+v", task)
	}
}

// Work added through one replica wakes the dispatch loop of another.
func TestNotificationsAcrossReplicas(t *testing.T) {
	dsn := pgtest.Schema(t)
	a, b := open(t, dsn, limits(store.Limits{})), open(t, dsn, limits(store.Limits{}))
	nctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notes := must(a.Notifications(nctx))(t)
	r := createRun(t, b, "r")
	must(b.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 1)}))(t)
	var runs, work bool
	for deadline := time.After(5 * time.Second); !runs || !work; {
		select {
		case n := <-notes:
			runs = runs || n.Runs
			work = work || n.Pool == store.DefaultPool
		case <-deadline:
			t.Fatalf("notified of a run event: %v, of work: %v", runs, work)
		}
	}
}

// Two replicas run a mixed workload on the same runs and queues: nothing deadlocks,
// the ledger matches the tasks, and every task is finished exactly once.
func TestReplicasUnderLoad(t *testing.T) {
	dsn := pgtest.Schema(t)
	pol := limits(store.Limits{Interval: time.Millisecond, Burst: 4, MaxConcurrency: 6})
	stores := []*pgstore.Store{open(t, dsn, pol), open(t, dsn, pol)}
	runs := []store.RunID{createRun(t, stores[0], "a"), createRun(t, stores[1], "b")}
	queues := []string{"q0", "q1", "q2", "q3", "q4"}

	var added, finished [2]atomic.Int64
	index := map[store.RunID]int{runs[0]: 0, runs[1]: 1}
	note := func(run store.RunID, res []store.AddResult) {
		for _, r := range res {
			if r.Status == store.AddAdded {
				added[index[run]].Add(1)
			}
		}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	fail := func(what string, err error) {
		if err != nil && !errors.Is(err, store.ErrRunClosed) {
			t.Errorf("%s: %v", what, err)
		}
	}
	// work dispatches and finishes with a mix of outcomes until told to stop, then
	// until the store has nothing left.
	work := func(s *pgstore.Store) {
		defer wg.Done()
		idle := 0
		for idle < 20 {
			res, err := s.Dispatch(ctx, store.DispatchRequest{Session: "s", Pool: store.DefaultPool, Slots: 1 + rand.IntN(4)})
			fail("dispatch", err)
			if len(res.Assignments) == 0 {
				select {
				case <-stop:
					idle++
				default:
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
			idle = 0
			for _, a := range res.Assignments {
				f := store.Finish{Lease: a.Lease.Ref, Source: store.SourceWorker, Result: store.ResultSucceeded,
					Record: store.AttemptRecord{Code: "ok"}}
				switch n := rand.IntN(20); {
				case n == 0:
					f.Result, f.Record.Code = store.ResultFailed, "failed"
				case n < 3:
					f.Result, f.RetryIn, f.Record.Code = store.ResultRetry, time.Millisecond, "retry"
				case n == 3:
					f.Result = store.ResultReleased
				case n == 4:
					f.Result, f.Block, f.Record.Code = store.ResultRetry, 2*time.Millisecond, "blocked"
				case n < 8 && a.Info.Depth < 2:
					found, err := s.AddTasks(ctx, store.AddRequest{Run: a.Info.Run, Parent: &a.Lease.Ref, Tasks: tasks(queues[rand.IntN(len(queues))], 2)})
					fail("discover", err)
					note(a.Info.Run, found)
				}
				st, err := s.Finish(ctx, f)
				fail("finish", err)
				if st != store.FinishAccepted {
					t.Errorf("finish of a live lease: %v", st)
				}
				if f.Result == store.ResultSucceeded || f.Result == store.ResultFailed {
					finished[index[a.Info.Run]].Add(1)
				}
			}
		}
	}
	for _, s := range stores {
		for range 4 {
			wg.Add(1)
			go work(s)
		}
	}
	// Seeds, pauses and maintenance run alongside.
	var side sync.WaitGroup
	side.Go(func() {
		for i := range 60 {
			s, run := stores[i%2], runs[rand.IntN(2)]
			res, err := s.AddTasks(ctx, store.AddRequest{Run: run, Tasks: tasks(queues[rand.IntN(len(queues))], 5)})
			fail("seed", err)
			note(run, res)
			time.Sleep(10 * time.Millisecond)
		}
	})
	side.Go(func() {
		for i := range 15 {
			_, err := stores[i%2].PauseRun(ctx, runs[1])
			fail("pause", err)
			time.Sleep(15 * time.Millisecond)
			_, err = stores[(i+1)%2].ResumeRun(ctx, runs[1])
			fail("resume", err)
			time.Sleep(25 * time.Millisecond)
		}
	})
	side.Go(func() {
		for i := range 30 {
			fail("maintain", stores[i%2].Maintain(ctx))
			time.Sleep(20 * time.Millisecond)
		}
	})
	side.Wait()
	for _, r := range runs {
		must(stores[0].SealRun(ctx, r))(t)
	}
	close(stop)
	wg.Wait()

	for i, r := range runs {
		run := must(stores[i].GetRun(ctx, r))(t)
		st := run.Stats
		if run.State != store.RunCompleted || st.Pending+st.Delayed+st.InFlight != 0 {
			t.Errorf("run %s: %v, %+v", r, run.State, st)
		}
		if got, want := int64(st.Succeeded+st.Failed), added[i].Load(); got != want || finished[i].Load() != want { //nolint:gosec // G115: small counts
			t.Errorf("run %s: %d tasks added, %d finished by the workers, %d finished in the stats", r, want, finished[i].Load(), got)
		}
	}
	t.Logf("tasks finished: %d and %d", finished[0].Load(), finished[1].Load())
	for i, s := range stores {
		if n := s.Deadlocks(); n != 0 {
			t.Errorf("replica %d retried %d deadlocks", i, n)
		}
	}
	audit := must(stores[0].Audit(ctx, 10_000))(t)
	if audit.LedgerDrift != 0 || audit.OrphanBacklogs != 0 {
		t.Errorf("ledger audit: %+v", audit)
	}
	var queuesInFlight int
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	if err := conn.QueryRow(ctx, "SELECT coalesce(sum(in_flight), 0)::int FROM queues").Scan(&queuesInFlight); err != nil {
		t.Fatal(err)
	}
	if queuesInFlight != 0 {
		t.Errorf("queues still count %d tasks in flight", queuesInFlight)
	}
}

// A resume sweep that the replica did not finish is finished by Maintain.
func TestMaintainFinishesResumeSweep(t *testing.T) {
	dsn := pgtest.Schema(t)
	s := open(t, dsn, limits(store.Limits{}))
	r := createRun(t, s, "r")
	must(s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 2)}))(t)
	// The state a replica leaves behind when it dies between the resume and its sweep.
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	must(conn.Exec(ctx, "UPDATE runs SET resuming = true; UPDATE lanes SET ready_at = NULL"))(t)
	if got := dispatch(t, s, 10); len(got) != 0 {
		t.Fatalf("dispatched %d before the sweep", len(got))
	}
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if got := dispatch(t, s, 10); len(got) != 2 {
		t.Fatalf("dispatched %d after Maintain finished the sweep", len(got))
	}
	var resuming bool
	if err := conn.QueryRow(ctx, "SELECT resuming FROM runs").Scan(&resuming); err != nil || resuming {
		t.Fatalf("the run is still marked as resuming: %v %v", resuming, err)
	}
}

// A replica that starts with another policy set makes waiting backlogs ready, so
// their readiness is worked out again under the new limits.
func TestPolicySetChange(t *testing.T) {
	dsn := pgtest.Schema(t)
	slow := limits(store.Limits{Interval: time.Hour, Burst: 1})
	old := open(t, dsn, slow)
	if err := old.Start(ctx, store.Replica{ID: "one", PolicyHash: slow.Hash()}); err != nil {
		t.Fatal(err)
	}
	r := createRun(t, old, "r")
	must(old.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 3)}))(t)
	if got := dispatch(t, old, 10); len(got) != 1 {
		t.Fatalf("dispatched %d at one an hour", len(got))
	}
	if got := dispatch(t, old, 10); len(got) != 0 {
		t.Fatalf("dispatched %d more inside the hour", len(got))
	}

	fast := limits(store.Limits{Interval: time.Hour, Burst: 1})
	fast.hash, fast.eff.Queue, fast.eff.Limits = "faster", store.Limits{}, store.Limits{}
	next := open(t, dsn, fast)
	if err := next.Start(ctx, store.Replica{ID: "two", PolicyHash: fast.Hash()}); err != nil {
		t.Fatal(err)
	}
	if got := dispatch(t, next, 10); len(got) != 2 {
		t.Fatalf("dispatched %d after the limits were lifted", len(got))
	}
}

// Ended runs and idle queues are removed after their retention.
func TestRetention(t *testing.T) {
	dsn := pgtest.Schema(t)
	clock := storetest.NewClock()
	s := open(t, dsn, limits(store.Limits{}), pgstore.WithClock(clock.Now), pgstore.WithRetention(100*24*time.Hour, time.Hour))
	r := createRun(t, s, "r")
	must(s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 1)}))(t)
	succeed(t, s, dispatch(t, s, 1)[0])
	if run := must(s.SealRun(ctx, r))(t); run.State != store.RunCompleted {
		t.Fatal(run.State)
	}
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	must(s.GetRun(ctx, r))(t)
	must(s.GetQueue(ctx, "q"))(t)

	clock.Advance(25 * time.Hour)
	if err := s.Maintain(ctx); err != nil { // deletes the run
		t.Fatal(err)
	}
	if _, err := s.GetRun(ctx, r); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the run after its retention: %v", err)
	}
	clock.Advance(25 * time.Hour)
	if err := s.Maintain(ctx); err != nil { // deletes the queue no backlog refers to
		t.Fatal(err)
	}
	if _, err := s.GetQueue(ctx, "q"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the idle queue: %v", err)
	}

	// The queue comes back with the next task for it, and the run ID is free again.
	again := createRun(t, s, "r")
	must(s.AddTasks(ctx, store.AddRequest{Run: again, Tasks: tasks("q", 1)}))(t)
	// The partitions of a month are dropped once every run in it is older than the
	// task retention, but not while one of them still has work: its tasks live there.
	held := dispatch(t, s, 1)
	if len(held) != 1 {
		t.Fatalf("dispatched %d from the queue that was created again", len(held))
	}
	succeed(t, s, held[0])
	clock.Advance(200 * 24 * time.Hour)
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	january := func() bool {
		var there bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass('tasks_y2026m01') IS NOT NULL").Scan(&there); err != nil {
			t.Fatal(err)
		}
		return there
	}
	if !january() {
		t.Fatal("the partition of January was dropped under a run that is still open")
	}
	if run := must(s.SealRun(ctx, again))(t); run.State != store.RunCompleted {
		t.Fatal(run.State)
	}
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if january() {
		t.Fatal("the partition of January is still there")
	}
}

// A backlog that waited for a slot when its run was paused is dispatched after the
// resume, although the slot was freed during the pause.
func TestResumeAfterSlotFreedDuringPause(t *testing.T) {
	s := open(t, pgtest.Schema(t), limits(store.Limits{MaxConcurrency: 1}))
	a, b := createRun(t, s, "a"), createRun(t, s, "b")
	must(s.AddTasks(ctx, store.AddRequest{Run: a, Tasks: tasks("q", 1)}))(t)
	must(s.AddTasks(ctx, store.AddRequest{Run: b, Tasks: tasks("q", 1)}))(t)
	held := dispatch(t, s, 10)
	if len(held) != 1 {
		t.Fatalf("dispatched %d with one slot on the queue", len(held))
	}
	if got := dispatch(t, s, 10); len(got) != 0 { // the other backlog now waits for the slot
		t.Fatalf("dispatched %d more", len(got))
	}
	waiting := a
	if held[0].Info.Run == a {
		waiting = b
	}
	must(s.PauseRun(ctx, waiting))(t)
	succeed(t, s, held[0])
	must(s.ResumeRun(ctx, waiting))(t)
	if err := s.Maintain(ctx); err != nil { // finishes the sweep if the resume left it
		t.Fatal(err)
	}
	if got := dispatch(t, s, 10); len(got) != 1 || got[0].Info.Run != waiting {
		t.Fatalf("after the resume: %+v", got)
	}
}

// A block that is lifted while a run is paused does not hold the run back after the
// resume.
func TestResumeAfterBlockLiftedDuringPause(t *testing.T) {
	s := open(t, pgtest.Schema(t), limits(store.Limits{}))
	r := createRun(t, s, "r")
	must(s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 1)}))(t)
	must(s.BlockQueue(ctx, "q", time.Now().Add(time.Hour), "test"))(t)
	if got := dispatch(t, s, 10); len(got) != 0 {
		t.Fatalf("dispatched %d from a blocked queue", len(got))
	}
	must(s.PauseRun(ctx, r))(t)
	must(s.BlockQueue(ctx, "q", time.Time{}, ""))(t)
	must(s.ResumeRun(ctx, r))(t)
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if got := dispatch(t, s, 10); len(got) != 1 {
		t.Fatalf("dispatched %d after the block was lifted and the run resumed", len(got))
	}
}

// A replica that cached a run forgets it when another replica deletes it after its
// retention, so it sees the run that takes the ID next.
func TestRunCacheAfterRetention(t *testing.T) {
	dsn := pgtest.Schema(t)
	clock := storetest.NewClock()
	cleaner := open(t, dsn, limits(store.Limits{}), pgstore.WithClock(clock.Now), pgstore.WithRetention(0, time.Hour))
	other := open(t, dsn, limits(store.Limits{}), pgstore.WithClock(clock.Now))
	must(other.Notifications(ctx))(t) // starts the listener

	r := createRun(t, cleaner, "r")
	first := must(other.GetRun(ctx, r))(t) // cached by the other replica
	must(cleaner.CancelRun(ctx, r))(t)
	clock.Advance(2 * time.Hour)
	if err := cleaner.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	createRun(t, cleaner, "r")
	must(cleaner.AddTasks(ctx, store.AddRequest{Run: r, Tasks: tasks("q", 2)}))(t)

	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := other.GetRun(ctx, r)
		if err == nil && run.Stats.Pending == 2 && run.State == store.RunOpen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the other replica still sees the deleted run: %+v %v (first %+v)", run, err, first.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got, _, err := other.ListTasks(ctx, store.TaskFilter{Run: r}, store.Page{}); err != nil || len(got) != 2 {
		t.Fatalf("listed %d tasks of the new run: %v", len(got), err)
	}
}
