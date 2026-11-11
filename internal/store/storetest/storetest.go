// Package storetest is the conformance suite of ADR 0001: every store.Store
// implementation must pass it. Each test names the rule it proves.
package storetest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
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

// seedHandler adds n tasks of one handler and priority to the default pool.
func (e *env) seedHandler(run store.RunID, queue, handler string, priority int32, n int) {
	e.t.Helper()
	tasks := make([]store.NewTask, n)
	for i := range tasks {
		tasks[i] = e.task(queue, "default")
		tasks[i].Handler, tasks[i].Priority, tasks[i].Spec = handler, priority, []byte(handler)
	}
	if _, err := e.s.AddTasks(ctx, store.AddRequest{Run: run, Tasks: tasks}); err != nil {
		e.t.Fatal(err)
	}
}

// dispatchFor dispatches for a session that can run the handlers.
func (e *env) dispatchFor(handlers []string, slots int) store.DispatchResult {
	e.t.Helper()
	res, err := e.s.Dispatch(ctx, store.DispatchRequest{Session: "s", Pool: "default", Handlers: handlers, Slots: slots})
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

// specs lists what the assignments carry: the tests put the handler's name there.
func specs(as []store.Assignment) []string {
	out := make([]string, len(as))
	for i := range as {
		out[i] = string(as[i].Spec)
	}
	return out
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

	t.Run("HandlerSelection", func(t *testing.T) {
		// A session is given the tasks of its handlers and none of the others; one
		// that names no handler is given everything; a task with no handler is for
		// every session.
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seedHandler(r, "q", "listing", 0, 2)
		e.seedHandler(r, "q", "product", 0, 3)
		e.seedHandler(r, "q", "", 0, 1)
		got := e.dispatchFor([]string{"listing"}, 10).Assignments
		if len(got) != 3 {
			t.Fatalf("a session with listing was given %d tasks, want the 2 listings and the one with no handler", len(got))
		}
		got = e.dispatchFor([]string{"other", "product"}, 10).Assignments
		if len(got) != 3 {
			t.Fatalf("a session with product was given %d tasks, want 3", len(got))
		}
		e.seedHandler(r, "q", "listing", 0, 1)
		e.seedHandler(r, "q", "product", 0, 1)
		if got = e.dispatchFor(nil, 10).Assignments; len(got) != 2 {
			t.Fatalf("a session that names no handler was given %d tasks, want 2", len(got))
		}
	})

	t.Run("HandlerPriority", func(t *testing.T) {
		// Priority orders a session's tasks across the handlers it can run.
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seedHandler(r, "q", "a", 1, 2)
		e.seedHandler(r, "q", "b", 5, 2)
		e.seedHandler(r, "q", "c", 9, 2)
		if got := specs(e.dispatchFor([]string{"a", "b"}, 3).Assignments); !slices.Equal(got, []string{"b", "b", "a"}) {
			t.Fatalf("dispatched %v, want both of b, then one of a", got)
		}
		if got := specs(e.dispatchFor([]string{"a", "b"}, 10).Assignments); !slices.Equal(got, []string{"a"}) {
			t.Fatalf("the rest: %v", got)
		}
	})

	t.Run("UnrunnableWorkWaitsForFree", func(t *testing.T) {
		// Tasks no session can run cost no politeness slot, and do not stand in the way
		// of the ones a session can run, however many there are.
		e := newEnv(t, f, oneRPS)
		r := e.run("r")
		e.seedHandler(r, "q", "ghost", 9, 300)
		e.seedHandler(r, "q", "page", 0, 2)
		e.seedHandler(r, "other", "ghost", 9, 300)
		for range 5 {
			res := e.dispatchFor([]string{"page"}, 10)
			if len(res.Assignments) > 1 {
				t.Fatalf("the rate admits one task, got %d", len(res.Assignments))
			}
			if len(res.Assignments) == 1 {
				if st := e.finish(res.Assignments[0], store.ResultSucceeded); st != store.FinishAccepted {
					t.Fatal(st)
				}
			}
			e.clock.Advance(time.Second)
		}
		if st := e.state(r).Stats; st.Succeeded != 2 || st.Attempts != 2 {
			t.Fatalf("the page tasks must have run, and only they: %+v", st)
		}
		// The waiting tasks have not used a slot: each queue admits one at once.
		if res := e.dispatchFor([]string{"ghost"}, 10); len(res.Assignments) != 2 {
			t.Fatalf("a session with the handler was given %d tasks after the others ran, want one per queue", len(res.Assignments))
		}
		// With nothing it can run, a session is told to wait for a notification.
		if res := e.dispatchFor([]string{"nobody"}, 10); len(res.Assignments) != 0 || !res.NextReadyAt.IsZero() {
			t.Fatalf("nothing to run: %+v", res)
		}
	})

	t.Run("HandlerLaneAfterRetry", func(t *testing.T) {
		// A task that goes back to its backlog stays with its handler.
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seedHandler(r, "q", "a", 0, 1)
		first := e.dispatchFor([]string{"a"}, 1).Assignments
		if len(first) != 1 {
			t.Fatal("not dispatched")
		}
		if st, err := e.s.Finish(ctx, store.Finish{Lease: first[0].Lease.Ref, Source: store.SourceWorker,
			Result: store.ResultRetry, RetryIn: 10 * time.Second}); err != nil || st != store.FinishAccepted {
			t.Fatalf("%v %v", st, err)
		}
		if res := e.dispatchFor([]string{"b"}, 1); len(res.Assignments) != 0 {
			t.Fatal("a session without the handler was given the retry")
		}
		res := e.dispatchFor([]string{"a"}, 1)
		if len(res.Assignments) != 0 || !res.NextReadyAt.Equal(e.clock.Now().Add(10*time.Second)) {
			t.Fatalf("the retry must wait for its delay: %d, next %v", len(res.Assignments), res.NextReadyAt)
		}
		e.clock.Advance(10 * time.Second)
		if got := e.dispatchFor([]string{"a"}, 1).Assignments; len(got) != 1 || got[0].Lease.Ref.Attempt != 2 {
			t.Fatalf("after the delay: %+v", got)
		}
	})

	t.Run("PausedRunKeepsItsHandlers", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seedHandler(r, "q", "a", 0, 2)
		e.seedHandler(r, "q", "b", 0, 2)
		e.pause(r)
		if got := e.dispatchFor([]string{"a"}, 10).Assignments; len(got) != 0 {
			t.Fatalf("a paused run dispatched %d", len(got))
		}
		e.resume(r)
		if got := e.dispatchFor([]string{"b"}, 10).Assignments; len(got) != 2 {
			t.Fatalf("after the resume: %d, want the 2 tasks of b", len(got))
		}
		if got := e.dispatchFor([]string{"a"}, 10).Assignments; len(got) != 2 {
			t.Fatalf("after the resume: %d, want the 2 tasks of a", len(got))
		}
	})

	t.Run("UnservedHandlerFails", func(t *testing.T) {
		// A task waits for a session with its handler. When no session has had the
		// handler for the grace period, it fails with code no_handler; a handler that a
		// session reports is never failed.
		const grace = time.Hour
		e := newEnv(t, f, store.Limits{})
		if err := e.s.Start(ctx, store.Replica{ID: "r1", PolicyHash: "test"}); err != nil {
			t.Fatal(err)
		}
		r := e.run("r")
		e.seal(r)
		open := e.run("open")
		e.seedHandler(open, "q", "ghost", 0, 2)
		e.seedHandler(open, "q", "page", 0, 1)
		sweep := func() store.Unserved {
			t.Helper()
			if err := e.s.ServeHandlers(ctx, []store.PoolHandlers{{Pool: "default", Handlers: []string{"page"}}}); err != nil {
				t.Fatal(err)
			}
			u, err := e.s.SweepUnserved(ctx, grace)
			if err != nil {
				t.Fatal(err)
			}
			return u
		}
		want := store.HandlerRef{Pool: "default", Handler: "ghost"}
		if u := sweep(); len(u.Failed) != 0 || len(u.Waiting) != 1 || u.Waiting[0].HandlerRef != want {
			t.Fatalf("at the start: %+v", u)
		}
		e.clock.Advance(grace - time.Minute)
		if u := sweep(); len(u.Failed) != 0 || len(u.Waiting) != 1 {
			t.Fatalf("before the grace: %+v", u)
		}
		e.seedHandler(open, "q", "ghost", 0, 1) // a task that is young
		e.clock.Advance(2 * time.Minute)
		u := sweep()
		if len(u.Failed) != 1 || u.Failed[0].HandlerRef != want || u.Failed[0].Tasks != 2 || len(u.Waiting) != 1 {
			t.Fatalf("after the grace: %+v, want the 2 old tasks failed and the young one waiting", u)
		}
		st := e.state(open).Stats
		if st.Failed != 2 || st.ByCode[store.CodeNoHandler] != 2 || st.Pending != 2 || st.Attempts != 0 {
			t.Fatalf("stats: %+v", st)
		}
		e.clock.Advance(grace)
		if u := sweep(); len(u.Failed) != 1 || u.Failed[0].Tasks != 1 || len(u.Waiting) != 0 {
			t.Fatalf("the young task: %+v", u)
		}
		// The handler that is served kept its task.
		if got := e.dispatchFor([]string{"page"}, 10).Assignments; len(got) != 1 {
			t.Fatalf("the served handler's task: %d", len(got))
		}
		tasks, _, err := e.s.ListTasks(ctx, store.TaskFilter{Run: open, Code: store.CodeNoHandler}, store.Page{})
		if err != nil || len(tasks) != 3 || tasks[0].State != store.TaskFailed {
			t.Fatalf("the failed tasks must be found by their code: %v %v", tasks, err)
		}
	})

	t.Run("NoFailureAfterRestart", func(t *testing.T) {
		// Nothing fails before the grace period has passed since a replica started,
		// whatever the tasks' age: the workers may not have reconnected yet.
		const grace = time.Hour
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seedHandler(r, "q", "ghost", 0, 1)
		e.clock.Advance(3 * grace)
		if err := e.s.Start(ctx, store.Replica{ID: "r1", PolicyHash: "test"}); err != nil {
			t.Fatal(err)
		}
		e.clock.Advance(grace / 2)
		if u, err := e.s.SweepUnserved(ctx, grace); err != nil || len(u.Failed) != 0 || len(u.Waiting) != 1 {
			t.Fatalf("%+v %v", u, err)
		}
		e.clock.Advance(grace)
		if u, err := e.s.SweepUnserved(ctx, grace); err != nil || len(u.Failed) != 1 {
			t.Fatalf("%+v %v", u, err)
		}
	})

	t.Run("LegacySessionServesItsPool", func(t *testing.T) {
		// A session that names no handlers can run any task of its pool, so while one
		// is reported, and for the grace period after, no handler of the pool is
		// unserved: the sweep must not fail the tasks that session is running.
		const grace = time.Hour
		e := newEnv(t, f, store.Limits{})
		if err := e.s.Start(ctx, store.Replica{ID: "r1", PolicyHash: "test"}); err != nil {
			t.Fatal(err)
		}
		r := e.run("r")
		e.seedHandler(r, "q", "ghost", 0, 1)
		e.clock.Advance(2 * grace)
		report := func(handlers ...string) {
			t.Helper()
			if err := e.s.ServeHandlers(ctx, []store.PoolHandlers{{Pool: "default", Handlers: handlers}}); err != nil {
				t.Fatal(err)
			}
		}
		sweep := func() store.Unserved {
			t.Helper()
			u, err := e.s.SweepUnserved(ctx, grace)
			if err != nil {
				t.Fatal(err)
			}
			return u
		}
		report("") // the session
		for range 4 {
			e.clock.Advance(grace / 2)
			report("")
			if u := sweep(); len(u.Failed) != 0 || len(u.Waiting) != 0 {
				t.Fatalf("a task that a session without handlers can run was swept: %+v", u)
			}
		}
		e.clock.Advance(grace / 2) // the session is gone, and reported within the grace period
		if u := sweep(); len(u.Failed) != 0 {
			t.Fatalf("failed within the grace period of the last report: %+v", u)
		}
		e.clock.Advance(grace)
		if u := sweep(); len(u.Failed) != 1 {
			t.Fatalf("after the grace period: %+v", u)
		}
	})

	t.Run("SweepReachesEveryLane", func(t *testing.T) {
		// A round looks at a bounded number of lanes and the next one goes on from
		// there, so no lane is left out however many wait. Here the lanes of tasks
		// that are too young to fail come first, and are more than a round takes on.
		const grace, lanes, young = time.Hour, 450, 250
		e := newEnv(t, f, store.Limits{})
		if err := e.s.Start(ctx, store.Replica{ID: "r1", PolicyHash: "test"}); err != nil {
			t.Fatal(err)
		}
		r := e.run("r")
		for i := range lanes {
			ghost := e.task(fmt.Sprintf("old%03d", i), "default")
			ghost.Handler, ghost.Spec, ghost.NotBefore = "ghost", []byte("ghost"), e.clock.Now().Add(240*time.Hour)
			e.add(store.AddRequest{Run: r, Tasks: []store.NewTask{ghost}})
		}
		e.clock.Advance(2 * grace)
		for i := range young {
			e.seedHandler(r, fmt.Sprintf("new%03d", i), "ghost", 0, 1)
		}
		total, rounds := 0, 0
		for total < lanes && rounds < 8 {
			rounds++
			e.clock.Advance(2 * time.Minute)
			u, err := e.s.SweepUnserved(ctx, grace)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range u.Failed {
				total += f.Tasks
			}
		}
		if total != lanes {
			t.Fatalf("%d of %d tasks failed in %d rounds", total, lanes, rounds)
		}
	})

	t.Run("UnrunnableWorkInAnotherPoolHasNoFirstRefusal", func(t *testing.T) {
		// Another pool's backlog has first refusal on a shared queue, but only if one
		// of that pool's sessions can run something of it: work no session has is not
		// worth waiting for.
		oneAtATime := store.Limits{Interval: time.Second, Burst: 1}
		dispatch := func(e *env) store.DispatchResult {
			res, err := e.s.Dispatch(ctx, store.DispatchRequest{Session: "s", Pool: "zeta", Slots: 1})
			if err != nil {
				t.Fatal(err)
			}
			return res
		}
		setup := func(served ...string) *env {
			e := newEnv(t, f, oneAtATime)
			r := e.run("r")
			unrunnable := e.task("q", "alpha") // the pool that sorts first wins ties
			unrunnable.Handler, unrunnable.Spec = "z", []byte("z")
			e.add(store.AddRequest{Run: r, Tasks: []store.NewTask{unrunnable}})
			e.seed(r, "q", "zeta", 1)
			if err := e.s.ServeHandlers(ctx, []store.PoolHandlers{{Pool: "alpha", Handlers: served}}); err != nil {
				t.Fatal(err)
			}
			return e
		}
		if res := dispatch(setup("a")); len(res.Assignments) != 1 {
			t.Fatalf("held for work that no session of the other pool can run: %+v", res)
		}
		if res := dispatch(setup("z")); len(res.Assignments) != 0 {
			t.Fatalf("the other pool, which can run its work, had no first refusal: %+v", res)
		}
	})

	t.Run("CapacityFreedWakesEveryLane", func(t *testing.T) {
		// When a slot is freed, every handler's tasks that waited for it can be taken,
		// not only the first handler's.
		e := newEnv(t, f, store.Limits{MaxConcurrency: 1})
		r := e.run("r")
		for _, h := range []string{"a", "b", "c"} {
			e.seedHandler(r, "q", h, 0, 1)
		}
		first := e.dispatchFor([]string{"a"}, 1).Assignments
		if len(first) != 1 {
			t.Fatal("not dispatched")
		}
		if res := e.dispatchFor([]string{"b", "c"}, 1); len(res.Assignments) != 0 {
			t.Fatalf("the queue is full: %d", len(res.Assignments))
		}
		if st := e.finish(first[0], store.ResultSucceeded); st != store.FinishAccepted {
			t.Fatal(st)
		}
		if got := specs(e.dispatchFor([]string{"c"}, 1).Assignments); !slices.Equal(got, []string{"c"}) {
			t.Fatalf("the handler after the first did not wake: %v", got)
		}
	})

	t.Run("LaterTaskDoesNotDelayReadyOnes", func(t *testing.T) {
		// A task that is due later does not move the readiness of its handler's
		// tasks that are due now.
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.seedHandler(r, "q", "a", 0, 1)
		later := e.task("q", "default")
		later.Handler, later.Spec, later.NotBefore = "a", []byte("a"), e.clock.Now().Add(time.Hour)
		e.add(store.AddRequest{Run: r, Tasks: []store.NewTask{later}})
		if got := e.dispatchFor([]string{"a"}, 10).Assignments; len(got) != 1 {
			t.Fatalf("%d tasks, want the one that is due", len(got))
		}
	})

	t.Run("RoutingProperty", func(t *testing.T) {
		// Whatever the order of adds, dispatches, finishes and time, a session is never
		// given a task of a handler it did not name, and nothing is lost or left waiting
		// for ever: once the work is drained by sessions of one handler each, every task
		// has run. (A rate or a concurrency that wakes only some lanes, or a readiness
		// that is late, leaves tasks behind.)
		handlers := []string{"h0", "h1", "h2", "h3"}
		for seed := range uint64(4) {
			rng := rand.New(rand.NewPCG(seed, 7)) //nolint:gosec // G404: a fixed seed makes the run repeatable
			e := newEnv(t, f, store.Limits{MaxConcurrency: 2})
			r := e.run("r")
			total := 0
			add := func(n int) {
				tasks := make([]store.NewTask, n)
				for i := range tasks {
					h := handlers[rng.IntN(len(handlers))]
					if rng.IntN(6) == 0 {
						h = ""
					}
					tasks[i] = e.task(fmt.Sprintf("q%d", rng.IntN(3)), "default")
					tasks[i].Handler, tasks[i].Spec, tasks[i].Priority = h, []byte(h), int32(rng.IntN(3)) //nolint:gosec // G115: below 3
					tasks[i].NotBefore = e.clock.Now().Add(time.Duration(rng.IntN(3)) * 10 * time.Second)
				}
				e.add(store.AddRequest{Run: r, Tasks: tasks})
				total += n
			}
			var open []store.Assignment
			take := func(set []string, res store.DispatchResult) {
				t.Helper()
				for i := range res.Assignments {
					if h := string(res.Assignments[i].Spec); len(set) > 0 && h != "" && !slices.Contains(set, h) {
						t.Fatalf("seed %d: a session with %v was given a task of %q", seed, set, h)
					}
				}
				open = append(open, res.Assignments...)
			}
			finishAll := func() {
				for i := range open {
					if st := e.finish(open[i], store.ResultSucceeded); st != store.FinishAccepted {
						t.Fatalf("seed %d: %v", seed, st)
					}
				}
				open = nil
			}
			add(30)
			for step := range 120 {
				switch rng.IntN(4) {
				case 0:
					add(rng.IntN(4))
				case 1, 2:
					var set []string
					for _, h := range handlers {
						if rng.IntN(2) == 0 {
							set = append(set, h)
						}
					}
					res, err := e.s.Dispatch(ctx, store.DispatchRequest{Session: "s", Pool: "default", Handlers: set, Slots: 1 + rng.IntN(3)})
					if err != nil {
						t.Fatal(err)
					}
					take(set, res)
				default:
					if len(open) > 0 {
						i := rng.IntN(len(open))
						if st := e.finish(open[i], store.ResultSucceeded); st != store.FinishAccepted {
							t.Fatalf("seed %d: %v", seed, st)
						}
						open = slices.Delete(open, i, i+1)
					}
				}
				if step%20 == 19 {
					finishAll() // before a lease runs out
				}
				e.clock.Advance(time.Duration(rng.IntN(1500)) * time.Millisecond)
			}
			finishAll()
			want := uint64(total)
			for range 200 {
				progress := false
				for _, h := range handlers {
					res := e.dispatchFor([]string{h}, 10)
					progress = progress || len(res.Assignments) > 0
					take([]string{h}, res)
				}
				finishAll()
				if !progress {
					e.clock.Advance(10 * time.Second)
				}
				if st := e.state(r).Stats; st.Succeeded == want {
					break
				}
			}
			if st := e.state(r).Stats; st.Succeeded != want {
				t.Fatalf("seed %d: %d of %d tasks ran: %+v", seed, st.Succeeded, total, st)
			}
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
