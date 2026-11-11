// Package storetest is the conformance suite of ADR 0001: every store.Store
// implementation must pass it. Each test names the rule it proves.
package storetest

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	"github.com/edevhub/crawly/internal/store"
)

// Clock is a manual clock shared by the test and the store under test.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock returns a clock that starts at 2026-01-01 UTC.
func NewClock() *Clock { return &Clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }

// Now returns the current manual time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// Policies is a programmable store.Policies.
type Policies struct {
	mu      sync.Mutex
	Default store.Effective
	Runs    map[store.RunID]store.Effective
}

// Hash returns a fixed hash, as the policy set never changes.
func (p *Policies) Hash() string { return "test" }

// For returns the run's entry in Runs, or Default when it has none.
func (p *Policies) For(run store.RunID, _ *policyv1.RunPolicy, _ store.QueueKey) store.Effective {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.Runs[run]; ok {
		return e
	}
	return p.Default
}

// Factory builds an empty store that uses the given policies and clock.
type Factory func(pol store.Policies, now func() time.Time) store.Store

type env struct {
	t     *testing.T
	s     store.Store
	clock *Clock
	pol   *Policies
	n     uint64
}

var ctx = context.Background()

func newEnv(t *testing.T, f Factory, queue store.Limits) *env {
	t.Helper()
	c := NewClock()
	lease := store.LeaseSettings{Duration: time.Minute, MaxDuration: 10 * time.Minute}
	p := &Policies{Default: store.Effective{Limits: queue, Queue: queue, Weight: 1, Lease: lease}, Runs: map[store.RunID]store.Effective{}}
	return &env{t: t, s: f(p, c.Now), clock: c, pol: p}
}

func (e *env) run(id string) store.RunID {
	e.t.Helper()
	if _, err := e.s.CreateRun(ctx, store.RunSpec{ID: store.RunID(id), Hash: []byte(id)}); err != nil {
		e.t.Fatal(err)
	}
	return store.RunID(id)
}

func (e *env) task(queue, pool string) store.NewTask {
	e.n++
	var fp store.Fingerprint
	binary.BigEndian.PutUint64(fp[:], e.n)
	return store.NewTask{Fingerprint: fp, Queue: store.QueueKey(queue), Pool: store.PoolName(pool)}
}

func (e *env) seed(run store.RunID, queue, pool string, n int) {
	e.t.Helper()
	tasks := make([]store.NewTask, n)
	for i := range tasks {
		tasks[i] = e.task(queue, pool)
	}
	res, err := e.s.AddTasks(ctx, store.AddRequest{Run: run, Tasks: tasks})
	if err != nil {
		e.t.Fatal(err)
	}
	for _, r := range res {
		if r.Status != store.AddAdded {
			e.t.Fatalf("seed status %v", r.Status)
		}
	}
}

func (e *env) dispatch(pool string, slots int) []store.Assignment {
	e.t.Helper()
	res, err := e.s.Dispatch(ctx, store.DispatchRequest{Session: "s", Pool: store.PoolName(pool), Slots: slots})
	if err != nil {
		e.t.Fatal(err)
	}
	return res.Assignments
}

func (e *env) add(req store.AddRequest) {
	e.t.Helper()
	if _, err := e.s.AddTasks(ctx, req); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) seal(run store.RunID) {
	e.t.Helper()
	if _, err := e.s.SealRun(ctx, run); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) pause(run store.RunID) {
	e.t.Helper()
	if _, err := e.s.PauseRun(ctx, run); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) resume(run store.RunID) {
	e.t.Helper()
	if _, err := e.s.ResumeRun(ctx, run); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) maintain() {
	e.t.Helper()
	if err := e.s.Maintain(ctx); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) finish(a store.Assignment, result store.TaskResult) store.FinishStatus {
	e.t.Helper()
	st, err := e.s.Finish(ctx, store.Finish{Lease: a.Lease.Ref, Source: store.SourceWorker, Result: result})
	if err != nil {
		e.t.Fatal(err)
	}
	return st
}

func (e *env) state(run store.RunID) store.Run {
	e.t.Helper()
	r, err := e.s.GetRun(ctx, run)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

// Run runs the whole suite against the stores a factory builds.
func Run(t *testing.T, f Factory) {
	t.Helper()
	oneRPS := store.Limits{Interval: time.Second, Burst: 1}

	t.Run("SharedPoliteness", func(t *testing.T) {
		e := newEnv(t, f, oneRPS)
		a, b := e.run("a"), e.run("b")
		e.seed(a, "q", "default", 50)
		e.seed(b, "q", "default", 50)
		total := 0
		for range 100 { // 10 seconds in 100 ms steps
			total += len(e.dispatch("default", 10))
			e.clock.Advance(100 * time.Millisecond)
		}
		if total < 9 || total > 11 {
			t.Fatalf("two runs on one queue at 1 RPS dispatched %d in 10s", total)
		}
	})

	t.Run("OverrideReplacesAndStillCounts", func(t *testing.T) {
		e := newEnv(t, f, oneRPS)
		fast, slow := e.run("fast"), e.run("slow")
		own := e.pol.Default
		own.Own, own.Limits = true, store.Limits{Interval: 100 * time.Millisecond, Burst: 1}
		e.pol.Runs[fast] = own
		e.seed(fast, "q", "default", 30)
		e.seed(slow, "q", "default", 30)
		got := map[store.RunID]int{}
		for range 30 { // 3 seconds of override traffic
			as := e.dispatch("default", 10)
			for i := range as {
				got[as[i].Info.Run]++
			}
			e.clock.Advance(100 * time.Millisecond)
		}
		if got[fast] < 25 {
			t.Fatalf("override run got %d in 3s at 10 RPS", got[fast])
		}
		if got[slow] > 1 {
			t.Fatalf("other run got %d while the override saturated the queue", got[slow])
		}
		// The override run is exhausted: the other resumes within one interval.
		e.clock.Advance(time.Second)
		if as := e.dispatch("default", 10); len(as) != 1 || as[0].Info.Run != slow {
			t.Fatalf("other run did not resume within one interval: %d", len(as))
		}
	})

	t.Run("BlocksStopEveryRun", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		a, b := e.run("a"), e.run("b")
		own := e.pol.Default
		own.Own = true
		e.pol.Runs[b] = own
		e.seed(a, "q", "default", 3)
		e.seed(b, "q", "default", 3)
		first := e.dispatch("default", 1)
		st, _ := e.s.Finish(ctx, store.Finish{Lease: first[0].Lease.Ref, Source: store.SourceWorker,
			Result: store.ResultRetry, Block: time.Minute, Record: store.AttemptRecord{Code: "http_429"}})
		if st != store.FinishAccepted {
			t.Fatal(st)
		}
		if as := e.dispatch("default", 10); len(as) != 0 {
			t.Fatalf("blocked queue dispatched %d", len(as))
		}
		e.clock.Advance(61 * time.Second)
		if as := e.dispatch("default", 10); len(as) != 6 {
			t.Fatalf("after the block: %d", len(as))
		}
	})

	t.Run("PerQueueFairness", func(t *testing.T) {
		e := newEnv(t, f, oneRPS)
		a, b := e.run("a"), e.run("b")
		for i := range 50 {
			e.seed(a, "other"+string(rune('A'+i)), "default", 5)
		}
		e.seed(a, "shared", "default", 100)
		e.seed(b, "shared", "default", 100)
		got := map[store.RunID]int{}
		for range 60 {
			as := e.dispatch("default", 100)
			for i := range as {
				if as[i].Info.Queue == "shared" {
					got[as[i].Info.Run]++
				}
			}
			e.clock.Advance(time.Second)
		}
		if got[b] < 24 || got[b] > 36 {
			t.Fatalf("run on one queue got %d of 60 shared dispatches, the other %d", got[b], got[a])
		}
	})

	t.Run("NoBankedCredit", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		a, b := e.run("a"), e.run("b")
		e.seed(b, "q", "default", 200)
		if _, err := e.s.PauseRun(ctx, b); err != nil {
			t.Fatal(err)
		}
		e.seed(a, "q", "default", 1200)
		if got := len(e.dispatch("default", 1000)); got != 1000 {
			t.Fatalf("a alone: %d", got)
		}
		if _, err := e.s.ResumeRun(ctx, b); err != nil {
			t.Fatal(err)
		}
		got := map[store.RunID]int{}
		for range 100 {
			as := e.dispatch("default", 1)
			for i := range as {
				got[as[i].Info.Run]++
			}
		}
		if got[b] < 40 || got[b] > 60 {
			t.Fatalf("resumed run got %d of 100, want about half", got[b])
		}
	})

	t.Run("Pools", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q1", "browser", 3)
		if as := e.dispatch("default", 10); len(as) != 0 {
			t.Fatalf("default pool received %d browser tasks", len(as))
		}
		if as := e.dispatch("browser", 10); len(as) != 3 {
			t.Fatalf("browser pool received %d", len(as))
		}
	})

	t.Run("FairnessAcrossPools", func(t *testing.T) {
		e := newEnv(t, f, oneRPS)
		a, b := e.run("a"), e.run("b")
		e.seed(a, "q", "default", 100)
		e.seed(b, "q", "browser", 100)
		got := map[string]int{}
		for range 400 { // 40 seconds; the default pool always asks first
			got["default"] += len(e.dispatch("default", 1))
			got["browser"] += len(e.dispatch("browser", 1))
			e.clock.Advance(100 * time.Millisecond)
		}
		if got["browser"] < 15 || got["default"] < 15 {
			t.Fatalf("pools with free slots must share the queue: %v", got)
		}
		// Only the default pool asks now: it keeps the queue, less the refusal period.
		before := got["default"]
		for range 200 {
			got["default"] += len(e.dispatch("default", 1))
			e.clock.Advance(100 * time.Millisecond)
		}
		if n := got["default"] - before; n < 15 {
			t.Fatalf("a pool without workers starved the other: %d in 20s", n)
		}
	})

	t.Run("Fencing", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q", "default", 1)
		first := e.dispatch("default", 1)[0]
		e.clock.Advance(2 * time.Minute) // the lease expires
		if st := e.finish(first, store.ResultSucceeded); st != store.FinishStale {
			t.Fatalf("expired lease: %v", st)
		}
		exp, _ := e.s.ExpiredLeases(ctx, 10)
		if len(exp) != 1 {
			t.Fatalf("expired leases: %d", len(exp))
		}
		if st, _ := e.s.Finish(ctx, store.Finish{Lease: exp[0].Ref, Source: store.SourceReaper, Result: store.ResultRetry}); st != store.FinishAccepted {
			t.Fatalf("reaper: %v", st)
		}
		second := e.dispatch("default", 1)[0]
		if second.Lease.Ref.Attempt != 2 || second.Lease.Ref.ID == first.Lease.Ref.ID {
			t.Fatalf("second lease: %+v", second.Lease.Ref)
		}
		if st := e.finish(first, store.ResultSucceeded); st != store.FinishStale {
			t.Fatalf("stale attempt: %v", st)
		}
		if st, _ := e.s.Finish(ctx, store.Finish{Lease: store.LeaseRef{ID: "nope.1.x", Attempt: 1}, Source: store.SourceWorker}); st != store.FinishUnknown {
			t.Fatalf("unknown lease: %v", st)
		}
		if st := e.finish(second, store.ResultSucceeded); st != store.FinishAccepted {
			t.Fatalf("current lease: %v", st)
		}
	})

	t.Run("Termination", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q1", "default", 1)
		e.seed(r, "q2", "default", 1)
		as := e.dispatch("default", 2)
		if _, err := e.s.SealRun(ctx, r); err != nil {
			t.Fatal(err)
		}
		e.finish(as[0], store.ResultSucceeded)
		if st := e.state(r).State; st != store.RunSealed {
			t.Fatalf("one task left: %v", st)
		}
		e.finish(as[1], store.ResultFailed)
		if st := e.state(r); st.State != store.RunCompleted || st.Stats.Succeeded != 1 || st.Stats.Failed != 1 {
			t.Fatalf("after the last task: %+v", st)
		}
		empty := e.run("empty")
		if run, _ := e.s.SealRun(ctx, empty); run.State != store.RunCompleted {
			t.Fatalf("an empty sealed run completes at once: %v", run.State)
		}
	})

	t.Run("NoOrphanedTasks", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seal(r)
		res, err := e.s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: []store.NewTask{e.task("q", "")}})
		if err != nil || res[0].Status != store.AddRunClosed {
			t.Fatalf("seed after seal: %v %v", res, err)
		}
	})

	t.Run("ReleaseDelayed", func(t *testing.T) {
		// A task a worker hands back can be held for a while: it is not dispatched
		// before the delay passed, and still costs no attempt.
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q", "default", 1)
		first := e.dispatch("default", 1)[0]
		st, err := e.s.Finish(ctx, store.Finish{Lease: first.Lease.Ref, Source: store.SourceWorker,
			Result: store.ResultReleased, RetryIn: 30 * time.Second})
		if err != nil || st != store.FinishAccepted {
			t.Fatalf("release: %v %v", st, err)
		}
		if got := e.dispatch("default", 1); len(got) != 0 {
			t.Fatalf("dispatched before the delay passed: %+v", got)
		}
		e.clock.Advance(31 * time.Second)
		again := e.dispatch("default", 1)
		if len(again) != 1 || again[0].Lease.Ref.Attempt != first.Lease.Ref.Attempt {
			t.Fatalf("after the delay: %+v, want attempt %d again", again, first.Lease.Ref.Attempt)
		}
		if st := e.state(r).Stats; st.Attempts != 1 || st.InFlight != 1 {
			t.Fatalf("stats: %+v", st)
		}
	})

	t.Run("ReleaseUndelivered", func(t *testing.T) {
		// An assignment that never reached a worker is undone: the task is dispatchable
		// at once, no attempt is recorded and the attempt number is used again.
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q", "default", 1)
		first := e.dispatch("default", 1)[0]
		if st := e.finish(first, store.ResultReleased); st != store.FinishAccepted {
			t.Fatalf("release: %v", st)
		}
		if st := e.state(r).Stats; st.Pending != 1 || st.InFlight != 0 || st.Attempts != 0 {
			t.Fatalf("a released task must be pending again, without an attempt: %+v", st)
		}
		// The old lease is dead.
		if st := e.finish(first, store.ResultSucceeded); st == store.FinishAccepted {
			t.Fatal("a released lease was accepted")
		}
		again := e.dispatch("default", 1)
		if len(again) != 1 || again[0].Info.Task != first.Info.Task {
			t.Fatalf("the released task was not dispatched again: %+v", again)
		}
		if again[0].Lease.Ref.Attempt != first.Lease.Ref.Attempt || again[0].Lease.Ref.ID == first.Lease.Ref.ID {
			t.Fatalf("want the same attempt under a new lease, got %+v after %+v", again[0].Lease.Ref, first.Lease.Ref)
		}
		e.finish(again[0], store.ResultSucceeded)
		task, err := e.s.GetTask(ctx, r, first.Info.Task, nil)
		if err != nil || len(task.Attempts) != 1 {
			t.Fatalf("one attempt must be recorded, got %+v %v", task.Attempts, err)
		}
		if st := e.state(r).Stats; st.Attempts != 1 {
			t.Fatalf("the run must count one attempt: %+v", st)
		}
	})

	t.Run("ConcurrencyLimit", func(t *testing.T) {
		// max_concurrency bounds what is in flight on a queue, across runs, and a slot
		// frees when an attempt ends.
		e := newEnv(t, f, store.Limits{MaxConcurrency: 2})
		a, b := e.run("a"), e.run("b")
		e.seed(a, "q", "default", 3)
		e.seed(b, "q", "default", 3)
		first := e.dispatch("default", 10)
		if len(first) != 2 {
			t.Fatalf("dispatched %d with max_concurrency 2", len(first))
		}
		if more := e.dispatch("default", 10); len(more) != 0 {
			t.Fatalf("dispatched %d more while 2 are in flight", len(more))
		}
		e.finish(first[0], store.ResultSucceeded)
		if more := e.dispatch("default", 10); len(more) != 1 {
			t.Fatalf("one slot freed, dispatched %d", len(more))
		}
		// Another queue has its own limit.
		e.seed(a, "other", "default", 3)
		if more := e.dispatch("default", 10); len(more) != 2 {
			t.Fatalf("another queue dispatched %d, want 2", len(more))
		}
	})

	t.Run("LeaseMaxDuration", func(t *testing.T) {
		// Extensions stop at the lease's maximum duration (10 minutes here).
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q", "default", 1)
		as := e.dispatch("default", 1)[0]
		start := e.clock.Now()
		var last store.Lease
		for range 30 {
			e.clock.Advance(30 * time.Second)
			lease, st, err := e.s.ExtendLease(ctx, as.Lease.Ref, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if st != store.FinishAccepted {
				break
			}
			last = lease
		}
		if got := last.ExpiresAt.Sub(start); got != 10*time.Minute {
			t.Fatalf("the lease was extended to %v after its start, want exactly the 10m maximum", got)
		}
		if _, st, _ := e.s.ExtendLease(ctx, as.Lease.Ref, time.Minute); st == store.FinishAccepted {
			t.Fatal("a lease past its maximum duration was extended")
		}
	})

	t.Run("Lineage", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q", "default", 1)
		parent := e.dispatch("default", 1)[0]
		e.seal(r)
		res, err := e.s.AddTasks(ctx, store.AddRequest{Run: r, Parent: &parent.Lease.Ref, Tasks: []store.NewTask{e.task("q", ""), e.task("q2", "")}})
		if err != nil || res[0].Status != store.AddAdded {
			t.Fatalf("discovery on a sealed run: %v %v", res, err)
		}
		e.finish(parent, store.ResultRetry)
		if st := e.state(r); st.State != store.RunSealed || st.Stats.Discovered != 2 {
			t.Fatalf("accepted discoveries survive a parent retry: %+v", st)
		}
		if _, err := e.s.AddTasks(ctx, store.AddRequest{Run: r, Parent: &parent.Lease.Ref, Tasks: []store.NewTask{e.task("q", "")}}); !errors.Is(err, store.ErrLeaseStale) {
			t.Fatalf("discovery from a finished lease: %v", err)
		}
		child := e.dispatch("default", 10)
		if len(child) < 2 {
			t.Fatalf("the parent's retry and its discoveries must be dispatchable, got %d", len(child))
		}
		for i := range child {
			if child[i].Info.Task != parent.Info.Task && child[i].Info.Depth != 1 {
				t.Fatalf("depth %d", child[i].Info.Depth)
			}
		}
	})

	t.Run("DedupAndBudget", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		if _, err := e.s.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r"), Limits: store.RunLimits{MaxTasks: 2}}); err != nil {
			t.Fatal(err)
		}
		one := e.task("q", "")
		res, _ := e.s.AddTasks(ctx, store.AddRequest{Run: "r", Tasks: []store.NewTask{one, one, e.task("q", ""), e.task("q", "")}})
		want := []store.AddStatus{store.AddAdded, store.AddDuplicate, store.AddAdded, store.AddOverBudget}
		for i, r := range res {
			if r.Status != want[i] {
				t.Fatalf("result %d: %v, want %v", i, r.Status, want[i])
			}
		}
		if _, err := e.s.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("other")}); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("CreateRun with a different spec: %v", err)
		}
	})

	t.Run("EventOrder", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.pause(r)
		e.resume(r)
		e.seal(r)
		var types []store.RunEventType
		var cur store.EventCursor
		for {
			evs, _ := e.s.RunEvents(ctx, cur, 2)
			if len(evs) == 0 {
				break
			}
			for i := range evs {
				types = append(types, evs[i].Type)
				cur = evs[i].Cursor
			}
		}
		want := []store.RunEventType{store.EventCreated, store.EventPaused, store.EventResumed, store.EventSealed, store.EventCompleted}
		if len(types) != len(want) {
			t.Fatalf("events: %v", types)
		}
		for i := range want {
			if types[i] != want[i] {
				t.Fatalf("events: %v", types)
			}
		}
	})

	t.Run("Lifecycle", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q", "default", 4)
		held := e.dispatch("default", 1)[0]
		e.pause(r)
		if as := e.dispatch("default", 10); len(as) != 0 {
			t.Fatalf("paused run dispatched %d", len(as))
		}
		e.resume(r)
		if as := e.dispatch("default", 1); len(as) != 1 {
			t.Fatalf("resumed run dispatched %d", len(as))
		}
		run, _ := e.s.CancelRun(ctx, r)
		if run.State != store.RunCancelled {
			t.Fatal(run.State)
		}
		if as := e.dispatch("default", 10); len(as) != 0 {
			t.Fatalf("cancelled run dispatched %d", len(as))
		}
		if st := e.finish(held, store.ResultSucceeded); st != store.FinishStale {
			t.Fatalf("completion after cancel: %v", st)
		}
		e.maintain() // the backlog of a cancelled run drains eventually
		if st := e.state(r).Stats; st.Cancelled != 4 || st.Pending != 0 || st.InFlight != 0 {
			t.Fatalf("drained: %+v", st)
		}
	})

	t.Run("ExpiryAndDeadline", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		nt := e.task("q", "")
		nt.TTL = time.Minute
		e.add(store.AddRequest{Run: r, Tasks: []store.NewTask{nt}})
		e.seal(r)
		e.clock.Advance(2 * time.Minute)
		if as := e.dispatch("default", 1); len(as) != 0 {
			t.Fatal("an expired task was dispatched")
		}
		e.maintain() // expired tasks are removed eventually
		if st := e.state(r); st.State != store.RunCompleted || st.Stats.Expired != 1 {
			t.Fatalf("after TTL: %+v", st)
		}
		if _, err := e.s.CreateRun(ctx, store.RunSpec{ID: "d", Hash: []byte("d"), Limits: store.RunLimits{Deadline: e.clock.Now().Add(time.Minute)}}); err != nil {
			t.Fatal(err)
		}
		e.seed("d", "q", "default", 2)
		e.clock.Advance(2 * time.Minute)
		e.maintain()
		if st := e.state("d"); st.State != store.RunExpired || st.Stats.Expired != 2 {
			t.Fatalf("after the deadline: %+v", st)
		}
	})

	t.Run("PriorityWithinBacklog", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		low, high := e.task("q", ""), e.task("q", "")
		high.Priority = 5
		e.add(store.AddRequest{Run: r, Tasks: []store.NewTask{low, high}})
		if as := e.dispatch("default", 1); as[0].Info.Fingerprint != high.Fingerprint {
			t.Fatal("higher priority must be dispatched first")
		}
	})

	t.Run("RetryDelayAndLeaseExtension", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seed(r, "q", "default", 1)
		a := e.dispatch("default", 1)[0]
		e.clock.Advance(50 * time.Second)
		l, st, _ := e.s.ExtendLease(ctx, a.Lease.Ref, time.Minute)
		if st != store.FinishAccepted || !l.ExpiresAt.Equal(e.clock.Now().Add(time.Minute)) {
			t.Fatalf("extend: %v %v", st, l.ExpiresAt)
		}
		e.clock.Advance(50 * time.Second) // past the first expiry, inside the extension
		s2, _ := e.s.Finish(ctx, store.Finish{Lease: a.Lease.Ref, Source: store.SourceWorker, Result: store.ResultRetry, RetryIn: 30 * time.Second})
		if s2 != store.FinishAccepted {
			t.Fatal(s2)
		}
		res, _ := e.s.Dispatch(ctx, store.DispatchRequest{Pool: "default", Slots: 1})
		if len(res.Assignments) != 0 || !res.NextReadyAt.Equal(e.clock.Now().Add(30*time.Second)) {
			t.Fatalf("retry must wait: %d, next %v", len(res.Assignments), res.NextReadyAt)
		}
		e.clock.Advance(30 * time.Second)
		if as := e.dispatch("default", 1); len(as) != 1 || as[0].Lease.Ref.Attempt != 2 {
			t.Fatal("retry not dispatched after its delay")
		}
	})

	t.Run("Notifications", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		nctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch, err := e.s.Notifications(nctx)
		if err != nil {
			t.Fatal(err)
		}
		r := e.run("r")
		e.seed(r, "q", "browser", 1)
		deadline := time.After(2 * time.Second)
		for {
			select {
			case n := <-ch:
				if n.Pool == "browser" {
					return
				}
			case <-deadline:
				t.Fatal("no notification for new work")
			}
		}
	})
}
