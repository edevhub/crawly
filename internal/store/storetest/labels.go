package storetest

import (
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/edevhub/crawly/internal/store"
)

// labelled is a new task with a label.
func (e *env) labelled(queue, label string) store.NewTask {
	nt := e.task(queue, "")
	nt.Label = label
	return nt
}

// addLabelled adds the tasks and returns the status of each.
func (e *env) addLabelled(run store.RunID, limit int, parent *store.LeaseRef, tasks ...store.NewTask) []store.AddStatus {
	e.t.Helper()
	res, err := e.s.AddTasks(ctx, store.AddRequest{Run: run, Parent: parent, Tasks: tasks, MaxLabels: limit})
	if err != nil {
		e.t.Fatal(err)
	}
	out := make([]store.AddStatus, len(res))
	for i := range res {
		out[i] = res[i].Status
	}
	return out
}

func (e *env) fail(a store.Assignment, code string) {
	e.t.Helper()
	st, err := e.s.Finish(ctx, store.Finish{Lease: a.Lease.Ref, Source: store.SourceWorker, Result: store.ResultFailed,
		Record: store.AttemptRecord{Code: code}})
	if err != nil || st != store.FinishAccepted {
		e.t.Fatalf("finish: %v %v", st, err)
	}
}

func (e *env) byLabel(run store.RunID) map[string]store.LabelStats {
	e.t.Helper()
	m := e.state(run).Stats.ByLabel
	out := make(map[string]store.LabelStats, len(m))
	for k, l := range m {
		if len(l.FailedByCode) == 0 {
			l.FailedByCode = nil
		}
		out[k] = l
	}
	return out
}

func sameLabels(got, want map[string]store.LabelStats) bool {
	if len(got) != len(want) {
		return false
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok || g.Succeeded != w.Succeeded || g.Failed != w.Failed || g.Expired != w.Expired || g.Cancelled != w.Cancelled ||
			!maps.Equal(g.FailedByCode, w.FailedByCode) {
			return false
		}
	}
	return true
}

func (e *env) wantLabels(run store.RunID, want map[string]store.LabelStats) {
	e.t.Helper()
	if got := e.byLabel(run); !sameLabels(got, want) {
		e.t.Fatalf("by label: got %+v, want %+v", got, want)
	}
}

// runLabels is the part of the suite for task labels.
func runLabels(t *testing.T, f Factory) {
	t.Helper()
	t.Run("LabelCountsFinalStatesOnce", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		// Two queues, so that the counters of one label are kept apart and summed.
		e.addLabelled(r, 10, nil, e.labelled("q1", "news"), e.labelled("q2", "news"), e.labelled("q1", "news"),
			e.labelled("q2", "shop"), e.labelled("q1", ""))
		e.wantLabels(r, map[string]store.LabelStats{"news": {}, "shop": {}}) // no task has ended: zeros, and no entry for "no label"

		as := e.dispatch("default", 10)
		if len(as) != 5 {
			t.Fatalf("dispatched %d", len(as))
		}
		var retried store.Assignment
		for i := range as {
			a := as[i]
			switch {
			case a.Info.Label == "":
				e.finish(a, store.ResultSucceeded)
			case a.Info.Label == "shop":
				e.fail(a, "http_500")
			case retried.Info.Task == "":
				retried = a
			default:
				e.finish(a, store.ResultSucceeded)
			}
		}
		// A retry and a release are not final.
		if st, err := e.s.Finish(ctx, store.Finish{Lease: retried.Lease.Ref, Source: store.SourceWorker, Result: store.ResultRetry,
			Record: store.AttemptRecord{Code: "http_503"}}); err != nil || st != store.FinishAccepted {
			t.Fatalf("retry: %v %v", st, err)
		}
		e.wantLabels(r, map[string]store.LabelStats{
			"news": {Succeeded: 2},
			"shop": {Failed: 1, FailedByCode: map[string]uint64{"http_500": 1}},
		})
		again := e.dispatch("default", 10)
		if len(again) != 1 || again[0].Info.Label != "news" {
			t.Fatalf("the retry was not dispatched with its label: %+v", again)
		}
		if st, err := e.s.Finish(ctx, store.Finish{Lease: again[0].Lease.Ref, Source: store.SourceWorker, Result: store.ResultReleased}); err != nil || st != store.FinishAccepted {
			t.Fatalf("release: %v %v", st, err)
		}
		e.wantLabels(r, map[string]store.LabelStats{
			"news": {Succeeded: 2},
			"shop": {Failed: 1, FailedByCode: map[string]uint64{"http_500": 1}},
		})
		// The last attempt ends with a lease that expires: the reaper fails the task.
		last := e.dispatch("default", 1)
		if len(last) != 1 {
			t.Fatal("the released task was not dispatched again")
		}
		e.clock.Advance(2 * time.Minute)
		if st, err := e.s.Finish(ctx, store.Finish{Lease: last[0].Lease.Ref, Source: store.SourceReaper, Result: store.ResultFailed,
			Record: store.AttemptRecord{Code: "lease_expired"}}); err != nil || st != store.FinishAccepted {
			t.Fatalf("reaper: %v %v", st, err)
		}
		e.wantLabels(r, map[string]store.LabelStats{
			"news": {Succeeded: 2, Failed: 1, FailedByCode: map[string]uint64{"lease_expired": 1}},
			"shop": {Failed: 1, FailedByCode: map[string]uint64{"http_500": 1}},
		})
		st := e.state(r).Stats
		if st.Succeeded != 3 || st.Failed != 2 {
			t.Fatalf("run counters: %+v", st)
		}
	})

	t.Run("LabelCountsDroppedTasks", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		// Cancel: one task in flight, one pending, in two queues.
		c := e.run("c")
		e.addLabelled(c, 10, nil, e.labelled("q1", "a"), e.labelled("q2", "a"), e.labelled("q1", "b"))
		held := e.dispatch("default", 1)
		if _, err := e.s.CancelRun(ctx, c); err != nil {
			t.Fatal(err)
		}
		if st := e.finish(held[0], store.ResultSucceeded); st != store.FinishStale {
			t.Fatalf("completion after cancel: %v", st)
		}
		e.maintain()
		cancelled := e.byLabel(c)
		if cancelled["a"].Cancelled+cancelled["b"].Cancelled != 3 || cancelled["a"].Cancelled != 2 || cancelled["b"].Cancelled != 1 {
			t.Fatalf("cancelled: %+v", cancelled)
		}
		if cancelled["a"].Succeeded+cancelled["b"].Succeeded != 0 {
			t.Fatalf("a result after the cancel was counted: %+v", cancelled)
		}

		// TTL: a pending task expires.
		ttl := e.run("ttl")
		nt := e.labelled("q", "slow")
		nt.TTL = time.Minute
		e.addLabelled(ttl, 10, nil, nt, e.labelled("q", "slow"))
		e.clock.Advance(2 * time.Minute)
		e.maintain()
		e.wantLabels(ttl, map[string]store.LabelStats{"slow": {Expired: 1}})
		e.dispatch("default", 10) // the second task is leased
		e.seal(ttl)

		// Deadline: pending and in-flight tasks expire.
		if _, err := e.s.CreateRun(ctx, store.RunSpec{ID: "d", Hash: []byte("d"),
			Limits: store.RunLimits{Deadline: e.clock.Now().Add(time.Minute)}}); err != nil {
			t.Fatal(err)
		}
		e.addLabelled("d", 10, nil, e.labelled("q1", "x"), e.labelled("q2", "x"), e.labelled("q2", ""))
		e.dispatch("default", 1)
		e.clock.Advance(2 * time.Minute)
		e.maintain()
		e.maintain()
		e.wantLabels("d", map[string]store.LabelStats{"x": {Expired: 2}})
		if st := e.state("d").Stats; st.Expired != 3 {
			t.Fatalf("run counters: %+v", st)
		}
	})

	t.Run("LabelCap", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		got := e.addLabelled(r, 2, nil, e.labelled("q", "a"), e.labelled("q", "b"), e.labelled("q", "c"), e.labelled("q", "a"), e.labelled("q", ""))
		want := []store.AddStatus{store.AddAdded, store.AddAdded, store.AddInvalid, store.AddAdded, store.AddAdded}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("statuses %v, want %v", got, want)
			}
		}
		// A later batch, and discovered tasks, count against the same labels.
		if got := e.addLabelled(r, 2, nil, e.labelled("q", "c"), e.labelled("q", "b")); got[0] != store.AddInvalid || got[1] != store.AddAdded {
			t.Fatalf("second batch: %v", got)
		}
		parent := e.dispatch("default", 1)[0]
		if got := e.addLabelled(r, 2, &parent.Lease.Ref, e.labelled("q", "d"), e.labelled("q", "a")); got[0] != store.AddInvalid || got[1] != store.AddAdded {
			t.Fatalf("discovered: %v", got)
		}
		e.wantLabels(r, map[string]store.LabelStats{"a": {}, "b": {}})
		// A duplicate does not take a label; no cap means no limit.
		dup := e.labelled("q", "e")
		dup.Fingerprint = parent.Info.Fingerprint
		if got := e.addLabelled(r, 3, nil, dup); got[0] != store.AddDuplicate {
			t.Fatalf("duplicate: %v", got)
		}
		many := make([]store.NewTask, 20)
		for i := range many {
			many[i] = e.labelled("q", fmt.Sprintf("l%d", i))
		}
		for _, s := range e.addLabelled(r, 0, nil, many...) {
			if s != store.AddAdded {
				t.Fatalf("without a cap: %v", s)
			}
		}
		if n := len(e.byLabel(r)); n != 22 {
			t.Fatalf("%d labels", n)
		}
	})

	t.Run("LabelCapUnderConcurrency", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		const workers, limit = 12, 4
		var mu sync.Mutex
		accepted := 0
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			nt := e.labelled(fmt.Sprintf("q%d", i), fmt.Sprintf("l%d", i))
			go func() {
				defer wg.Done()
				res, err := e.s.AddTasks(ctx, store.AddRequest{Run: r, Tasks: []store.NewTask{nt}, MaxLabels: limit})
				if err != nil {
					t.Error(err)
					return
				}
				if res[0].Status == store.AddAdded {
					mu.Lock()
					accepted++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if n := len(e.byLabel(r)); n != limit || accepted != limit {
			t.Fatalf("%d labels, %d accepted, cap %d", n, accepted, limit)
		}
	})

	t.Run("RejectedTasksAreCounted", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		bad := e.labelled("q", "")
		bad.Invalid = true
		got := e.addLabelled(r, 1, nil, e.labelled("q", "a"), bad, e.labelled("q", "b"), e.labelled("q", "c"), e.labelled("q", "a"))
		if got[1] != store.AddInvalid || got[2] != store.AddInvalid || got[3] != store.AddInvalid || got[4] != store.AddAdded {
			t.Fatalf("statuses %v", got)
		}
		if n := e.state(r).Stats.Invalid; n != 3 {
			t.Fatalf("invalid = %d, want 3", n)
		}
		// A batch to a closed run is refused as a whole and counts nothing.
		e.seal(r)
		e.addLabelled(r, 1, nil, bad, e.labelled("q", "z"))
		if n := e.state(r).Stats.Invalid; n != 3 {
			t.Fatalf("invalid after a refused batch = %d", n)
		}
	})

	// The labels of a run never add up to more than the run's own counters, on any read:
	// a scheduler subtracts one from the other.
	t.Run("LabelTotalsNeverExceedRunCounters", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		const n = 300
		tasks := make([]store.NewTask, n)
		for i := range tasks {
			tasks[i] = e.labelled(fmt.Sprintf("q%d", i%7), "l")
		}
		e.addLabelled(r, 0, nil, tasks...)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				run, err := e.s.GetRun(ctx, r)
				if err != nil {
					t.Error(err)
					return
				}
				l := run.Stats.ByLabel["l"]
				if l.Succeeded > run.Stats.Succeeded || l.Failed > run.Stats.Failed {
					t.Errorf("labels ahead of the run: %+v, succeeded %d, failed %d", l, run.Stats.Succeeded, run.Stats.Failed)
					return
				}
			}
		})
		for done := 0; done < n; {
			as := e.dispatch("default", 20)
			for i := range as {
				if done%5 == 0 {
					e.fail(as[i], "x")
				} else {
					e.finish(as[i], store.ResultSucceeded)
				}
				done++
			}
		}
		close(stop)
		wg.Wait()
		if l := e.byLabel(r)["l"]; l.Succeeded+l.Failed != n {
			t.Fatalf("final: %+v", l)
		}
	})

	t.Run("LabelOnTasks", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		e.addLabelled(r, 10, nil, e.labelled("q", "a"), e.labelled("q", "b"), e.labelled("q", "a"), e.labelled("q", ""))
		for _, c := range []struct {
			label string
			want  int
		}{{"a", 2}, {"b", 1}, {"", 4}, {"none", 0}} {
			tasks, _, err := e.s.ListTasks(ctx, store.TaskFilter{Run: r, Label: c.label}, store.Page{})
			if err != nil || len(tasks) != c.want {
				t.Fatalf("label %q: %d tasks, %v; want %d", c.label, len(tasks), err, c.want)
			}
			for i := range tasks {
				if st := &tasks[i]; c.label != "" && st.Label != c.label {
					t.Fatalf("task has label %q in the listing of %q", st.Label, c.label)
				}
			}
		}
		// Filters combine.
		a := e.dispatch("default", 1)[0]
		e.finish(a, store.ResultSucceeded)
		tasks, _, err := e.s.ListTasks(ctx, store.TaskFilter{Run: r, Label: "a", States: []store.TaskState{store.TaskSucceeded, store.TaskPending}}, store.Page{})
		if err != nil || len(tasks) != 2 {
			t.Fatalf("label and states: %+v %v", tasks, err)
		}
		if tasks, _, err = e.s.ListTasks(ctx, store.TaskFilter{Run: r, Label: "b", States: []store.TaskState{store.TaskSucceeded}}, store.Page{}); err != nil || len(tasks) != map[bool]int{true: 1}[a.Info.Label == "b"] {
			t.Fatalf("label and states: %+v %v", tasks, err)
		}
		got, err := e.s.GetTask(ctx, r, a.Info.Task, nil)
		if err != nil || got.Label != a.Info.Label {
			t.Fatalf("GetTask label %q, lease label %q, %v", got.Label, a.Info.Label, err)
		}
	})
}
