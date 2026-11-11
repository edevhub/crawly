package storetest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/edevhub/crawly/internal/store"
)

// runLimited creates the run "r" with limits.
func (e *env) runLimited(lim store.RunLimits) store.RunID {
	e.t.Helper()
	if _, err := e.s.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r"), Limits: lim}); err != nil {
		e.t.Fatal(err)
	}
	return "r"
}

// finishWith ends an attempt with a code.
func (e *env) finishWith(a store.Assignment, result store.TaskResult, code string, block time.Duration) store.FinishStatus {
	e.t.Helper()
	st, err := e.s.Finish(ctx, store.Finish{Lease: a.Lease.Ref, Source: store.SourceWorker, Result: result,
		Block: block, Record: store.AttemptRecord{Code: code}})
	if err != nil {
		e.t.Fatal(err)
	}
	return st
}

// events lists the types of the run event log.
func (e *env) events() []store.RunEventType {
	e.t.Helper()
	var types []store.RunEventType
	var cur store.EventCursor
	for {
		evs, err := e.s.RunEvents(ctx, cur, 100)
		if err != nil {
			e.t.Fatal(err)
		}
		if len(evs) == 0 {
			return types
		}
		for i := range evs {
			types = append(types, evs[i].Type)
			cur = evs[i].Cursor
		}
	}
}

func count(types []store.RunEventType, want store.RunEventType) int {
	n := 0
	for _, t := range types {
		if t == want {
			n++
		}
	}
	return n
}

// wantAborted checks the run is aborted for the rule, by the given task.
func (e *env) wantAborted(run store.RunID, rule store.AbortRule, code string, task store.TaskID) store.Run {
	e.t.Helper()
	r := e.state(run)
	if r.State != store.RunAborted || r.Abort == nil || r.Abort.Rule != rule || r.Abort.Code != code || r.Abort.Task != task {
		e.t.Fatalf("run %v abort %+v; want aborted by rule %v, code %q, task %v", r.State, r.Abort, rule, code, task)
	}
	if r.FinishedAt.IsZero() {
		e.t.Fatal("an aborted run has a finish time")
	}
	return r
}

// wantDropped checks that the aborted run's tasks were all dropped as cancelled.
func (e *env) wantDropped(run store.RunID, cancelled uint64) {
	e.t.Helper()
	e.maintain() // the backlog of an ended run drains eventually
	if st := e.state(run).Stats; st.Cancelled != cancelled || st.Pending != 0 || st.Delayed != 0 || st.InFlight != 0 {
		e.t.Fatalf("after the abort: %+v; want %d cancelled and nothing else left", st, cancelled)
	}
}

func abortSuite(t *testing.T, f Factory) {
	t.Helper()

	t.Run("AbortOnCode", func(t *testing.T) {
		// A retry that carries a listed code ends the run, with the queue block that
		// came with it applied; the leases of the run are revoked and its tasks dropped.
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{AbortOnCodes: []string{"blocked", "captcha"}})
		e.seed(r, "q", "default", 5)
		e.seal(r)
		as := e.dispatch("default", 3)
		if st := e.finishWith(as[0], store.ResultRetry, "timeout", 0); st != store.FinishAccepted {
			t.Fatal(st)
		}
		if got := e.state(r); got.State != store.RunSealed || got.Abort != nil {
			t.Fatalf("a code that is not listed ended the run: %+v", got)
		}
		if st := e.finishWith(as[1], store.ResultSucceeded, "", 0); st != store.FinishAccepted {
			t.Fatal(st)
		}
		if st := e.finishWith(as[2], store.ResultRetry, "blocked", time.Hour); st != store.FinishAccepted {
			t.Fatal(st)
		}
		got := e.wantAborted(r, store.AbortOnCode, "blocked", as[2].Info.Task)
		if got.Stats.Succeeded != 1 || got.Stats.ByCode["blocked"] != 1 || got.Stats.Failed != 0 {
			t.Fatalf("the outcomes applied before the abort are kept: %+v", got.Stats)
		}
		// 5 tasks: one succeeded, four are dropped (the retried one among them).
		e.wantDropped(r, 4)
		if q, err := e.s.GetQueue(ctx, "q"); err != nil || q.BlockedUntil.IsZero() || q.BlockReason != "blocked" {
			t.Fatalf("the site stays blocked: %+v %v", q, err)
		}
		types := e.events()
		if count(types, store.EventAborted) != 1 || types[len(types)-1] != store.EventAborted {
			t.Fatalf("events: %v", types)
		}
		if as := e.dispatch("default", 10); len(as) != 0 {
			t.Fatalf("an aborted run dispatched %d", len(as))
		}
		// An aborted run is final: it cannot be paused, and a cancel changes nothing.
		if _, err := e.s.PauseRun(ctx, r); err == nil {
			t.Fatal("an aborted run was paused")
		}
		if run, err := e.s.CancelRun(ctx, r); err != nil || run.State != store.RunAborted {
			t.Fatalf("cancel after abort: %v %v", run.State, err)
		}
	})

	t.Run("AbortOnCodeFinalFailureAndRevokedLease", func(t *testing.T) {
		// A final failure with a listed code counts as well; the leases still held by
		// workers are fenced afterwards, as after a cancel.
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{AbortOnCodes: []string{"blocked"}})
		e.seed(r, "q", "default", 3)
		as := e.dispatch("default", 2)
		e.finishWith(as[0], store.ResultFailed, "blocked", 0)
		e.wantAborted(r, store.AbortOnCode, "blocked", as[0].Info.Task)
		if st := e.finishWith(as[1], store.ResultSucceeded, "", 0); st != store.FinishStale {
			t.Fatalf("an outcome after the abort: %v", st)
		}
		e.wantDropped(r, 2)
		if got := e.state(r).Stats; got.Failed != 1 || got.Succeeded != 0 {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("ReleasedNeverTrips", func(t *testing.T) {
		// A task handed back records no attempt, so its code cannot end the run.
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{AbortOnCodes: []string{"no_handler"}, MaxFailed: 1})
		e.seed(r, "q", "default", 1)
		a := e.dispatch("default", 1)[0]
		if st := e.finishWith(a, store.ResultReleased, "no_handler", 0); st != store.FinishAccepted {
			t.Fatal(st)
		}
		if got := e.state(r); got.State != store.RunOpen || got.Abort != nil {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("AbortMaxFailed", func(t *testing.T) {
		// Tasks that failed for good count; retries do not.
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{MaxFailed: 3})
		e.seed(r, "q", "default", 8)
		as := e.dispatch("default", 6)
		e.finishWith(as[0], store.ResultRetry, "x", 0)
		e.finishWith(as[1], store.ResultFailed, "x", 0)
		e.finishWith(as[2], store.ResultSucceeded, "", 0)
		e.finishWith(as[3], store.ResultFailed, "x", 0)
		if got := e.state(r); got.State != store.RunOpen {
			t.Fatalf("two failures of three: %v", got.State)
		}
		e.finishWith(as[4], store.ResultFailed, "y", 0)
		e.wantAborted(r, store.AbortMaxFailed, "y", as[4].Info.Task)
		// 8 tasks: 3 failed, 1 succeeded; the retried one, the in-flight one and two never run are dropped.
		e.wantDropped(r, 4)
	})

	t.Run("AbortMaxConsecutiveFailures", func(t *testing.T) {
		// A success resets the count, a retry does not touch it.
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{MaxConsecutiveFailures: 3})
		e.seed(r, "q", "default", 10)
		as := e.dispatch("default", 10)
		steps := []struct {
			result store.TaskResult
			code   string
		}{
			{store.ResultFailed, "x"}, {store.ResultFailed, "x"}, {store.ResultSucceeded, ""}, // reset
			{store.ResultFailed, "x"}, {store.ResultRetry, "x"}, {store.ResultFailed, "x"}, // a retry between
		}
		for i, s := range steps {
			e.finishWith(as[i], s.result, s.code, 0)
			if got := e.state(r); got.State != store.RunOpen {
				t.Fatalf("step %d ended the run: %v", i, got.State)
			}
		}
		e.finishWith(as[6], store.ResultFailed, "z", 0)
		e.wantAborted(r, store.AbortConsecutiveFailures, "z", as[6].Info.Task)
		// 10 tasks: 5 failed, 1 succeeded, the rest dropped.
		e.wantDropped(r, 4)
	})

	t.Run("AbortRulesNeedARunThatIsNotOver", func(t *testing.T) {
		// A run cancelled first stays cancelled: its in-flight outcome is fenced.
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{MaxFailed: 1})
		e.seed(r, "q", "default", 2)
		a := e.dispatch("default", 1)[0]
		if _, err := e.s.CancelRun(ctx, r); err != nil {
			t.Fatal(err)
		}
		e.finishWith(a, store.ResultFailed, "x", 0)
		if got := e.state(r); got.State != store.RunCancelled || got.Abort != nil {
			t.Fatalf("%+v", got)
		}
		if n := count(e.events(), store.EventAborted); n != 0 {
			t.Fatalf("%d abort events for a cancelled run", n)
		}
	})

	t.Run("AbortOnTheLastTask", func(t *testing.T) {
		// A sealed run whose last task trips a rule is aborted, not completed.
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{MaxFailed: 1})
		e.seed(r, "q", "default", 1)
		e.seal(r)
		a := e.dispatch("default", 1)[0]
		e.finishWith(a, store.ResultFailed, "x", 0)
		e.wantAborted(r, store.AbortMaxFailed, "x", a.Info.Task)
	})

	t.Run("ConcurrentTripGivesOneAbort", func(t *testing.T) {
		// Workers that trip a rule at the same moment, on different queues, end the run
		// once: one event, one reason, whichever rule is used.
		for name, lim := range map[string]store.RunLimits{
			"code":        {AbortOnCodes: []string{"blocked"}},
			"max_failed":  {MaxFailed: 3},
			"consecutive": {MaxConsecutiveFailures: 3},
		} {
			t.Run(name, func(t *testing.T) {
				e := newEnv(t, f, store.Limits{})
				r := e.runLimited(lim)
				const n = 12
				for i := range n {
					e.seed(r, fmt.Sprintf("q%d", i), "default", 1)
				}
				as := e.dispatch("default", n)
				if len(as) != n {
					t.Fatalf("dispatched %d of %d", len(as), n)
				}
				var wg sync.WaitGroup
				for i := range as {
					wg.Go(func() { e.finishWith(as[i], store.ResultFailed, "blocked", 0) })
				}
				wg.Wait()
				got := e.state(r)
				if got.State != store.RunAborted || got.Abort == nil || got.Abort.Task == "" {
					t.Fatalf("%v %+v", got.State, got.Abort)
				}
				if n := count(e.events(), store.EventAborted); n != 1 {
					t.Fatalf("%d abort events", n)
				}
				// The reason names a task that was part of the race.
				found := false
				for i := range as {
					found = found || as[i].Info.Task == got.Abort.Task
				}
				if !found {
					t.Fatalf("the reason names %v, which was not among the tasks", got.Abort.Task)
				}
				e.maintain()
				if st := e.state(r).Stats; st.Pending != 0 || st.InFlight != 0 {
					t.Fatalf("%+v", st)
				}
			})
		}
	})

	t.Run("ConcurrentSuccessesAndFailures", func(t *testing.T) {
		// Half the outcomes fail and half succeed, at once, with limits no order can
		// reach: nothing is tripped by mistake and every outcome is counted. (The reset
		// of the streak is tested in order, in AbortMaxConsecutiveFailures.)
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{MaxConsecutiveFailures: 100})
		const n = 12
		for i := range n {
			e.seed(r, fmt.Sprintf("q%d", i), "default", 1)
		}
		as := e.dispatch("default", n)
		var wg sync.WaitGroup
		for i := range as {
			wg.Go(func() {
				if i%2 == 0 {
					e.finishWith(as[i], store.ResultFailed, "x", 0)
				} else {
					e.finishWith(as[i], store.ResultSucceeded, "", 0)
				}
			})
		}
		wg.Wait()
		if got := e.state(r); got.State != store.RunOpen || got.Stats.Failed != n/2 || got.Stats.Succeeded != n/2 {
			t.Fatalf("%v %+v", got.State, got.Stats)
		}
	})

	t.Run("LeaseExpiriesAreNotTheSitesFailures", func(t *testing.T) {
		// A task that fails for good because its lease expired is the worker fleet's
		// failure: it counts toward neither failure limit, so a fleet outage does not end
		// the run, but a code the run lists still does.
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{MaxFailed: 1, MaxConsecutiveFailures: 1})
		e.seed(r, "q", "default", 4)
		as := e.dispatch("default", 2)
		e.clock.Advance(2 * time.Minute) // past the lease
		for i := range as {
			st, err := e.s.Finish(ctx, store.Finish{Lease: as[i].Lease.Ref, Source: store.SourceReaper, Result: store.ResultFailed,
				Record: store.AttemptRecord{Code: "lease_expired"}})
			if err != nil || st != store.FinishAccepted {
				t.Fatalf("reaper: %v %v", st, err)
			}
		}
		if got := e.state(r); got.State != store.RunOpen || got.Stats.Failed != 2 {
			t.Fatalf("lease expiries ended the run or went uncounted: %v %+v", got.State, got.Stats)
		}
		// A task the worker fails for good does count: the one still pending, dispatched now.
		last := e.dispatch("default", 1)[0]
		e.finishWith(last, store.ResultFailed, "x", 0)
		e.wantAborted(r, store.AbortConsecutiveFailures, "x", last.Info.Task)
	})

	t.Run("LeaseExpiredCodeCanBeListed", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.runLimited(store.RunLimits{AbortOnCodes: []string{"lease_expired"}})
		e.seed(r, "q", "default", 2)
		a := e.dispatch("default", 1)[0]
		e.clock.Advance(2 * time.Minute)
		if st, err := e.s.Finish(ctx, store.Finish{Lease: a.Lease.Ref, Source: store.SourceReaper, Result: store.ResultRetry,
			Record: store.AttemptRecord{Code: "lease_expired"}}); err != nil || st != store.FinishAccepted {
			t.Fatalf("reaper: %v %v", st, err)
		}
		e.wantAborted(r, store.AbortOnCode, "lease_expired", a.Info.Task)
	})

	t.Run("BlockedQueuesOnTheRun", func(t *testing.T) {
		// A run's stats name the blocked queues its tasks wait on, the busiest first,
		// bounded; the block of an operator shows as well, and a lifted block does not.
		e := newEnv(t, f, store.Limits{})
		r, other := e.run("r"), e.run("other")
		e.seed(r, "calm", "default", 2)
		e.seed(other, "elsewhere", "default", 1)
		if got := e.state(r).Stats.BlockedQueues; len(got) != 0 {
			t.Fatalf("nothing is blocked: %+v", got)
		}
		until := e.clock.Now().Add(time.Hour)
		if _, err := e.s.BlockQueue(ctx, "elsewhere", until, "operator"); err != nil {
			t.Fatal(err)
		}
		if got := e.state(r).Stats.BlockedQueues; len(got) != 0 {
			t.Fatalf("a queue the run has no tasks on: %+v", got)
		}
		for i := range store.MaxBlockedQueues + 5 {
			e.seed(r, fmt.Sprintf("site%02d", i), "default", i+1)
			if _, err := e.s.BlockQueue(ctx, store.QueueKey(fmt.Sprintf("site%02d", i)), until, "blocked"); err != nil {
				t.Fatal(err)
			}
		}
		got := e.state(r).Stats.BlockedQueues
		if len(got) != store.MaxBlockedQueues {
			t.Fatalf("%d queues listed, want the bound %d", len(got), store.MaxBlockedQueues)
		}
		top := store.MaxBlockedQueues + 4
		if got[0].Queue != store.QueueKey(fmt.Sprintf("site%02d", top)) || got[0].Waiting != uint64(top+1) ||
			got[0].Reason != "blocked" || !got[0].Until.Equal(until) {
			t.Fatalf("the busiest queue first: %+v", got[0])
		}
		for i := 1; i < len(got); i++ {
			if got[i].Waiting >= got[i-1].Waiting {
				t.Fatalf("not ordered by waiting tasks: %+v", got)
			}
		}
		// The run of the blocked queue's other tenant sees it too.
		e.seed(other, "site03", "default", 2)
		if got := e.state(other).Stats.BlockedQueues; len(got) != 2 {
			t.Fatalf("%+v", got)
		}
		e.clock.Advance(2 * time.Hour)
		if got := e.state(r).Stats.BlockedQueues; len(got) != 0 {
			t.Fatalf("the blocks ended: %+v", got)
		}
	})
}
