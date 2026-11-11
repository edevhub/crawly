package storetest

import (
	"testing"
	"time"

	"github.com/edevhub/crawly/internal/store"
)

// poolStatsLanes is the conformance of PoolStats where readiness is kept per handler
// and where a queue is slowed by strain.
func poolStatsLanes(t *testing.T, f Factory) {
	t.Helper()

	t.Run("PoolStatsFollowsTheReadyLane", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{})
		r := e.run("r")
		first := e.clock.Now()
		e.seedHandler(r, "q", "a", 0, 1)
		e.clock.Advance(10 * time.Second)
		second := e.clock.Now()
		e.seedHandler(r, "q", "b", 0, 1)
		e.clock.Advance(time.Second)
		if d := poolStats(t, e)["default"]; d.Pending != 2 || len(d.Due) != 1 || !d.Due[0].Since.Equal(first) {
			t.Fatalf("both lanes are ready, the older one counts: %+v", d)
		}
		// The oldest lane is worked: the queue is due since the other one.
		if as := e.dispatchFor([]string{"a"}, 5); len(as.Assignments) != 1 {
			t.Fatalf("dispatched %d", len(as.Assignments))
		}
		// Since is not before the lane's own task: a store may count it from the
		// dispatch round that passed the lane by.
		d := poolStats(t, e)["default"]
		if d.Pending != 1 || d.InFlight != 1 || len(d.Due) != 1 || d.Due[0].Since.Before(second) {
			t.Fatalf("the lane of the handler that is not served stays due: %+v", d)
		}
		if as := e.dispatchFor([]string{"b"}, 5); len(as.Assignments) != 1 {
			t.Fatalf("dispatched %d", len(as.Assignments))
		}
		if d := poolStats(t, e)["default"]; d.Pending != 0 || len(d.Due) != 0 {
			t.Fatalf("nothing is left to serve: %+v", d)
		}
	})

	t.Run("PoolStatsHoldsASlowedQueueBack", func(t *testing.T) {
		e := newEnv(t, f, store.Limits{Interval: time.Second, Burst: 1, MaxConcurrency: 2})
		e.pol.Default.Adaptive = adaptive
		r := e.run("r")
		e.seed(r, "q", "default", 3)
		as := e.dispatch("default", 5)
		if len(as) != 1 {
			t.Fatalf("dispatched %d", len(as))
		}
		e.clock.Advance(100 * time.Millisecond)
		e.finishStrain(as[0], store.ResultRetry, true)
		e.wantInterval(2 * time.Second)
		// Slowed to one request in two seconds: the queue's work is delayed until then.
		if d := poolStats(t, e)["default"]; len(d.Due) != 0 || d.Delayed == 0 {
			t.Fatalf("a slowed queue is not due before its pace admits: %+v", d)
		}
		e.clock.Advance(3 * time.Second)
		if d := poolStats(t, e)["default"]; len(d.Due) != 1 || d.Delayed != 0 {
			t.Fatalf("the queue is due once the slowed pace admits: %+v", d)
		}
	})
}
