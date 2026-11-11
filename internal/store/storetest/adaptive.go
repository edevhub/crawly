package storetest

import (
	"testing"
	"time"

	"github.com/edevhub/crawly/internal/sched"
	"github.com/edevhub/crawly/internal/store"
)

// adaptive is the AIMD pacing of the tests: a rate cut in half per strain step, down to
// one request in 8 seconds, and a quarter of the base rate back per recovery step.
var adaptive = sched.Adaptive{Enabled: true, MaxInterval: 8 * time.Second, Decrease: 0.5, Recovery: 0.25}

// finishWith ends an attempt; strain marks it as a sign of strain on the site.
func (e *env) finishWith(a store.Assignment, result store.TaskResult, strain bool) {
	e.t.Helper()
	st, err := e.s.Finish(ctx, store.Finish{Lease: a.Lease.Ref, Source: store.SourceWorker, Result: result, Strain: strain,
		RetryIn: time.Millisecond})
	if err != nil || st != store.FinishAccepted {
		e.t.Fatalf("finish: %v %v", st, err)
	}
}

func (e *env) queue() store.Queue {
	e.t.Helper()
	q, err := e.s.GetQueue(ctx, "q")
	if err != nil {
		e.t.Fatal(err)
	}
	return q
}

// wantIntervalNear is wantInterval for an interval the arithmetic rounds.
func (e *env) wantIntervalNear(want time.Duration) {
	e.t.Helper()
	if got := e.queue().Strain.Interval; got < want-5*time.Millisecond || got > want+5*time.Millisecond {
		e.t.Fatalf("the queue's interval is %v, want about %v", got, want)
	}
}

func (e *env) wantInterval(want time.Duration) {
	e.t.Helper()
	if got := e.queue().Strain.Interval; got != want {
		e.t.Fatalf("the queue's interval is %v, want %v", got, want)
	}
}

// adaptivePacing is the conformance of adaptive pacing (ADR 0001): the queue's rate is
// cut by a sign of strain and restored by successes, as one state shared by every run.
func adaptivePacing(t *testing.T, f Factory) {
	t.Helper()
	oneRPS := store.Limits{Interval: time.Second, Burst: 1}
	env := func(t *testing.T) *env {
		t.Helper()
		e := newEnv(t, f, oneRPS)
		e.pol.Default.Adaptive = adaptive
		return e
	}

	t.Run("StrainSlowsTheQueueAndSuccessRestoresIt", func(t *testing.T) {
		e := env(t)
		r := e.run("r")
		e.seed(r, "q", "default", 30)
		// Each strain step halves the rate, down to the ceiling.
		for _, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second} {
			as := e.dispatch("default", 5)
			if len(as) != 1 {
				t.Fatalf("dispatched %d, want 1", len(as))
			}
			e.clock.Advance(100 * time.Millisecond)
			e.finishWith(as[0], store.ResultRetry, true)
			e.wantInterval(want)
			e.clock.Advance(9 * time.Second)
		}
		if q := e.queue(); q.Strain.Steps != 3 || q.Strain.Last.IsZero() {
			t.Fatalf("three steps and a last strain were expected: %+v", q.Strain)
		}
		// One request at a time while slowed, whatever the time: a second is refused
		// by concurrency.
		held := e.dispatch("default", 5)
		if len(held) != 1 {
			t.Fatalf("dispatched %d", len(held))
		}
		e.clock.Advance(20 * time.Second)
		if as := e.dispatch("default", 5); len(as) != 0 {
			t.Fatalf("a slowed queue dispatched %d with one in flight", len(as))
		}
		// A success dispatched in this epoch is a recovery step: the rate grows by a
		// quarter of the base rate each time (1/8, 3/8, 5/8, 7/8, 9/8 requests a second).
		e.clock.Advance(100 * time.Millisecond)
		e.finishWith(held[0], store.ResultSucceeded, false)
		for _, want := range []time.Duration{2666 * time.Millisecond, 1600 * time.Millisecond, 1142 * time.Millisecond, 0} {
			e.wantIntervalNear(want)
			e.clock.Advance(10 * time.Second)
			as := e.dispatch("default", 5)
			if len(as) != 1 {
				t.Fatalf("dispatched %d, want 1", len(as))
			}
			e.clock.Advance(100 * time.Millisecond)
			e.finishWith(as[0], store.ResultSucceeded, false)
		}
		e.wantInterval(0)
	})

	t.Run("AnEpochIsOneStep", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{Interval: time.Second, Burst: 3})
		e.pol.Default.Adaptive = adaptive
		r := e.run("r")
		e.seed(r, "q", "default", 20)
		first := e.dispatch("default", 5)
		if len(first) != 3 {
			t.Fatalf("dispatched %d, want the burst of 3", len(first))
		}
		e.clock.Advance(100 * time.Millisecond)
		// Requests that failed together are one step, not three.
		e.finishWith(first[0], store.ResultRetry, true)
		e.finishWith(first[1], store.ResultRetry, true)
		e.wantInterval(2 * time.Second)
		if q := e.queue(); q.Strain.Steps != 1 {
			t.Fatalf("%d steps for failures that came together", q.Strain.Steps)
		}
		// A success of the same epoch does not recover what the step just took.
		e.finishWith(first[2], store.ResultSucceeded, false)
		e.wantInterval(2 * time.Second)
		// A failure that is not a sign of strain changes nothing.
		e.clock.Advance(10 * time.Second)
		next := e.dispatch("default", 5)
		if len(next) != 1 {
			t.Fatalf("dispatched %d, want 1 on a slowed queue", len(next))
		}
		e.clock.Advance(100 * time.Millisecond)
		e.finishWith(next[0], store.ResultRetry, false)
		e.wantInterval(2 * time.Second)
	})

	t.Run("RunLimitsAreSlowedToo", func(t *testing.T) {
		e := env(t)
		a, fast := e.run("a"), e.run("fast")
		own := e.pol.Default
		own.Own, own.Limits = true, store.Limits{Interval: 100 * time.Millisecond, Burst: 1}
		e.pol.Runs[fast] = own
		e.seed(a, "q", "default", 1)
		// The other run's failure slows the queue.
		first := e.dispatch("default", 1)
		e.clock.Advance(100 * time.Millisecond)
		e.finishWith(first[0], store.ResultRetry, true)
		e.seed(fast, "q", "default", 20)
		e.wantIntervalNear(2 * time.Second)
		// The run that overrode the limits waits for the queue's interval, with one request at a time.
		e.clock.Advance(2 * time.Second)
		as := e.dispatch("default", 5)
		if len(as) != 1 || as[0].Info.Run != fast {
			t.Fatalf("dispatched %d", len(as))
		}
		e.clock.Advance(200 * time.Millisecond)
		if more := e.dispatch("default", 5); len(more) != 0 {
			t.Fatalf("a run with limits of its own dispatched %d, one request is in flight", len(more))
		}
		e.finishWith(as[0], store.ResultSucceeded, false)
		e.clock.Advance(500 * time.Millisecond)
		if more := e.dispatch("default", 5); len(more) != 0 {
			t.Fatalf("a run with limits of its own dispatched %d inside the queue's interval", len(more))
		}
	})

	t.Run("SwitchedOffChangesNothing", func(t *testing.T) {
		e := newEnv(t, f, oneRPS)
		r := e.run("r")
		e.seed(r, "q", "default", 5)
		as := e.dispatch("default", 5)
		e.clock.Advance(100 * time.Millisecond)
		e.finishWith(as[0], store.ResultRetry, true)
		if q := e.queue(); q.Strain.Interval != 0 || q.Strain.Steps != 0 {
			t.Fatalf("adaptive pacing is off, yet: %+v", q.Strain)
		}
		e.clock.Advance(time.Second)
		if got := e.dispatch("default", 5); len(got) != 1 {
			t.Fatalf("dispatched %d at the policy's rate", len(got))
		}
	})

	t.Run("UnblockResetsTheSlowdown", func(t *testing.T) {
		e := env(t)
		r := e.run("r")
		e.seed(r, "q", "default", 5)
		as := e.dispatch("default", 5)
		e.clock.Advance(100 * time.Millisecond)
		e.finishWith(as[0], store.ResultRetry, true)
		e.wantInterval(2 * time.Second)
		if as := e.dispatch("default", 5); len(as) != 0 {
			t.Fatalf("dispatched %d inside the slowed interval", len(as))
		}
		q, err := e.s.BlockQueue(ctx, "q", time.Time{}, "")
		if err != nil {
			t.Fatal(err)
		}
		if q.Strain.Interval != 0 {
			t.Fatalf("the slowdown survived the unblock: %v", q.Strain.Interval)
		}
		if as := e.dispatch("default", 5); len(as) != 1 {
			t.Fatalf("dispatched %d after the reset", len(as))
		}
	})
}
