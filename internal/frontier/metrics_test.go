package frontier

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
)

func TestCapped(t *testing.T) {
	c := newCapped(2)
	for _, tc := range []struct{ in, want string }{{"a", "a"}, {"b", "b"}, {"c", otherLabel}, {"a", "a"}, {"d", otherLabel}, {"b", "b"}} {
		if got := c.label(tc.in); got != tc.want {
			t.Errorf("label(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestSeriesStayBounded is the cardinality rule: however many queues, codes, workers
// and pools there are, the series of the event metrics are bounded by the policies and
// the caps, never by the queue keys.
func TestSeriesStayBounded(t *testing.T) {
	pol, err := policy.Parse([]byte(`{"policies": [
		{"name": "a", "match": {"queueKey": "*.a.com"}, "limits": {"ratePerSecond": 1}},
		{"name": "b", "match": {"queueKey": "*.b.com"}, "limits": {"ratePerSecond": 1}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	m, err := newMetrics(prometheus.NewRegistry(), pol.Class)
	if err != nil {
		t.Fatal(err)
	}
	const queues = 3000
	for i := range queues {
		for _, key := range []string{fmt.Sprintf("h%d.a.com", i), fmt.Sprintf("h%d.b.com", i), fmt.Sprintf("site%d.org", i)} {
			info := store.LeaseInfo{Queue: store.QueueKey(key), Pool: "default", Worker: store.WorkerInfo{ID: fmt.Sprintf("worker-%d", i)}}
			m.dispatch("default", []store.Assignment{{Info: info, Waited: time.Second}})
			m.finished(info, taskv1.Outcome_KIND_RETRY, store.Finish{Result: store.ResultRetry, Block: time.Minute,
				Record: store.AttemptRecord{Code: fmt.Sprintf("code-%d", i), Latency: time.Second}})
			m.expired(info, store.Finish{Result: store.ResultRetry, Record: store.AttemptRecord{Code: "lease_expired"}})
		}
	}
	for i := range 300 {
		m.undelivered(store.PoolName(fmt.Sprintf("pool_%d", i)))
	}
	policies := 3 // a, b, builtin
	for _, tc := range []struct {
		name string
		c    prometheus.Collector
		want int
	}{
		{"dispatched", m.dispatched, policies},
		{"dispatch wait", m.dispatchWait, policies},
		{"blocks", m.blocks, policies},
		{"fetch", m.fetch, policies},
		{"lease expiries by worker", m.expiries, maxWorkers + 1},
		{"released by pool", m.released, maxPools + 1},
		// The capped codes, the reserved ones and "other"; no pool, so no product.
		{"outcomes", m.outcomes, maxCodes + len(reservedCodes) + 1},
	} {
		if got := series(t, tc.c); got > tc.want {
			t.Errorf("%s: %d series after %d queues, want at most %d", tc.name, got, queues*3, tc.want)
		}
	}
	if got := series(t, m.dispatched); got != policies {
		t.Errorf("dispatched: %d series, want one per policy class (%d)", got, policies)
	}
	if got := counterValue(t, m.blocks, "a"); got != queues {
		t.Errorf("blocks of policy a: %v, want %d", got, queues)
	}
}

// series counts the series a collector exposes.
func series(t *testing.T, c prometheus.Collector) int {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatal(err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, mf := range mfs {
		n += len(mf.GetMetric())
	}
	return n
}

// counterValue is the value of the counter with the given label values.
func counterValue(t *testing.T, c *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	var out dto.Metric
	if err := c.WithLabelValues(labels...).Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetCounter().GetValue()
}

// gatherAll gathers a registry and fails on any error: a scrape that errors loses every
// series of the replica.
func gatherAll(t *testing.T, reg prometheus.Gatherer) map[string][]*dto.Metric {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("the scrape fails: %v", err)
	}
	out := map[string][]*dto.Metric{}
	for _, mf := range mfs {
		out[mf.GetName()] = mf.GetMetric()
	}
	return out
}

// valueOf is the gauge value of the series with the given label pairs.
func valueOf(t *testing.T, g map[string][]*dto.Metric, name string, labels ...string) (float64, bool) {
	t.Helper()
	for _, m := range g[name] {
		have := map[string]string{}
		for _, l := range m.GetLabel() {
			have[l.GetName()] = l.GetValue()
		}
		match := true
		for i := 0; i+1 < len(labels); i += 2 {
			match = match && have[labels[i]] == labels[i+1]
		}
		if match {
			return m.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

func testServer(t *testing.T, st store.Store) (*Server, *prometheus.Registry) {
	t.Helper()
	pol, err := policy.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if st == nil {
		st = memstore.New(pol)
	}
	reg := prometheus.NewRegistry()
	srv, err := New(Config{Store: st, Policies: pol, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	return srv, reg
}

// TestScrapePastThePoolCap: pools past the cap are folded into one label, and the
// scrape still works, with their figures added up.
func TestScrapePastThePoolCap(t *testing.T) {
	ctx := t.Context()
	srv, reg := testServer(t, nil)
	const pools = maxPools + 3
	if _, err := srv.store.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	for i := range pools {
		name := store.PoolName(fmt.Sprintf("pool_%d", i))
		var fp store.Fingerprint
		fp[0], fp[1] = byte(i), 1
		if _, err := srv.store.AddTasks(ctx, store.AddRequest{Run: "r", Tasks: []store.NewTask{{Fingerprint: fp, Queue: "q", Pool: name}}}); err != nil {
			t.Fatal(err)
		}
		srv.pool(ctx, name) // a pool of this replica as well as of the store
	}
	srv.state.refresh(ctx)
	g := gatherAll(t, reg)
	if n, ok := valueOf(t, g, "crawly_frontier_pool_tasks", "pool", otherLabel, "state", "pending"); !ok || n != 3 {
		t.Errorf("pending tasks of the folded pools: %v %v, want 3", n, ok)
	}
	if n, ok := valueOf(t, g, "crawly_frontier_pool_tasks", "pool", "pool_0", "state", "pending"); !ok || n != 1 {
		t.Errorf("pending tasks of a pool within the cap: %v %v", n, ok)
	}
	if _, ok := valueOf(t, g, "crawly_frontier_oldest_due_task_age_seconds", "pool", otherLabel); !ok {
		t.Error("the folded pools have due work and no age")
	}
	if n, ok := valueOf(t, g, "crawly_frontier_sessions", "pool", otherLabel); !ok || n != 0 {
		t.Errorf("sessions of the folded pools: %v %v", n, ok)
	}
}

// failingStats is a store whose pool statistics can be made to fail.
type failingStats struct {
	store.Store
	fail atomic.Bool
}

func (f *failingStats) PoolStats(ctx context.Context, limit int) ([]store.PoolStats, error) {
	if f.fail.Load() {
		return nil, store.ErrUnavailable
	}
	return f.Store.PoolStats(ctx, limit)
}

// TestStatsRefreshFailureKeepsTheLastGoodFigures: a store that cannot be read does not
// make the global gauges disappear, and the refresh's health says so.
func TestStatsRefreshFailureKeepsTheLastGoodFigures(t *testing.T) {
	ctx := t.Context()
	pol, _ := policy.New(nil)
	st := &failingStats{Store: memstore.New(pol)}
	srv, reg := testServer(t, st)
	if _, err := st.CreateRun(ctx, store.RunSpec{ID: "r", Hash: []byte("r")}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddTasks(ctx, store.AddRequest{Run: "r", Tasks: []store.NewTask{{Queue: "q", Pool: "default"}}}); err != nil {
		t.Fatal(err)
	}

	g := gatherAll(t, reg)
	if _, ok := valueOf(t, g, "crawly_frontier_pool_stats_up"); ok {
		t.Error("before the first refresh there is nothing to say about it")
	}
	if _, ok := valueOf(t, g, "crawly_frontier_pool_tasks"); ok {
		t.Error("before the first refresh the store's gauges are absent, not zero")
	}
	srv.state.refresh(ctx)
	g = gatherAll(t, reg)
	first, _ := valueOf(t, g, "crawly_frontier_pool_stats_last_success_timestamp_seconds")
	if up, _ := valueOf(t, g, "crawly_frontier_pool_stats_up"); up != 1 || first == 0 {
		t.Fatalf("after a good refresh: up %v, last success %v", up, first)
	}
	if n, ok := valueOf(t, g, "crawly_frontier_pool_tasks", "pool", "default", "state", "pending"); !ok || n != 1 {
		t.Fatalf("pending: %v %v", n, ok)
	}

	st.fail.Store(true)
	time.Sleep(1100 * time.Millisecond) // a second clock tick, so that a changed timestamp would show
	srv.state.refresh(ctx)
	g = gatherAll(t, reg)
	if up, _ := valueOf(t, g, "crawly_frontier_pool_stats_up"); up != 0 {
		t.Errorf("after a failed refresh: up %v", up)
	}
	if last, _ := valueOf(t, g, "crawly_frontier_pool_stats_last_success_timestamp_seconds"); last != first {
		t.Errorf("the last success moved from %v to %v on a failure", first, last)
	}
	if n, ok := valueOf(t, g, "crawly_frontier_pool_tasks", "pool", "default", "state", "pending"); !ok || n != 1 {
		t.Errorf("the last good figure must stay: %v %v", n, ok)
	}
	if _, ok := valueOf(t, g, "crawly_frontier_oldest_due_task_age_seconds", "pool", "default"); !ok {
		t.Error("the last good age must stay")
	}
}

// TestReleaseCodesAreReserved: no stream of worker codes can push the Frontier's own
// codes into "other".
func TestReleaseCodesAreReserved(t *testing.T) {
	m, err := newMetrics(prometheus.NewRegistry(), func(string) string { return "builtin" })
	if err != nil {
		t.Fatal(err)
	}
	info := store.LeaseInfo{Queue: "q", Pool: "default"}
	for i := range 5 * maxCodes {
		m.finished(info, taskv1.Outcome_KIND_FAILED, store.Finish{Result: store.ResultFailed, Record: store.AttemptRecord{Code: fmt.Sprintf("c%d", i)}})
	}
	for _, code := range []string{CodeNoHandler, CodeShutdown} {
		m.finished(info, taskv1.Outcome_KIND_RELEASED, store.Finish{Result: store.ResultReleased, Record: store.AttemptRecord{Code: code}})
		if got := counterValue(t, m.released, "default", code); got != 1 {
			t.Errorf("released %s = %v, want 1", code, got)
		}
		if got := counterValue(t, m.outcomes, "released", code, "released"); got != 1 {
			t.Errorf("outcome %s = %v, want 1", code, got)
		}
	}
	m.expired(info, store.Finish{Result: store.ResultRetry, Record: store.AttemptRecord{Code: "lease_expired"}})
	if got := counterValue(t, m.outcomes, "retry", "lease_expired", "retry"); got != 1 {
		t.Errorf("lease_expired outcome = %v", got)
	}
	if got := counterValue(t, m.released, "default", otherLabel); got != 0 {
		t.Errorf("a reserved release code was folded into other: %v", got)
	}
}

func TestRecentForgetsWhatIsNotSeen(t *testing.T) {
	now := time.Unix(0, 0)
	var evicted []string
	r := newRecent(2, time.Hour, func(v string) { evicted = append(evicted, v) })
	r.now = func() time.Time { return now }
	for _, tc := range []struct{ in, want string }{{"a", "a"}, {"b", "b"}, {"c", otherLabel}} {
		if got := r.label(tc.in); got != tc.want {
			t.Fatalf("label(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	now = now.Add(30 * time.Minute)
	r.label("a") // seen again: kept
	now = now.Add(45 * time.Minute)
	if got := r.label("c"); got != "c" {
		t.Errorf("after b was forgotten, c = %q", got)
	}
	if !slices.Equal(evicted, []string{"b"}) {
		t.Errorf("evicted %v, want [b]", evicted)
	}
	now = now.Add(2 * time.Hour)
	r.sweep()
	slices.Sort(evicted)
	if !slices.Equal(evicted, []string{"a", "b", "c"}) {
		t.Errorf("after a sweep evicted %v", evicted)
	}
}

// TestExpirySeriesGoWithTheWorker: a forgotten worker's series are deleted.
func TestExpirySeriesGoWithTheWorker(t *testing.T) {
	m, err := newMetrics(prometheus.NewRegistry(), func(string) string { return "builtin" })
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	m.workers.now = func() time.Time { return now }
	info := store.LeaseInfo{Queue: "q", Pool: "default", Worker: store.WorkerInfo{ID: "pod-1"}}
	m.expired(info, store.Finish{Result: store.ResultRetry})
	if n := series(t, m.expiries); n != 1 {
		t.Fatalf("series: %d", n)
	}
	now = now.Add(2 * workerWindow)
	m.workers.sweep()
	if n := series(t, m.expiries); n != 0 {
		t.Errorf("the series of a worker not seen for %v stayed: %d", 2*workerWindow, n)
	}
}

func TestErrorKind(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{store.ErrUnavailable, "unavailable"},
		{fmt.Errorf("x: %w", store.ErrUnavailable), "unavailable"},
		{context.DeadlineExceeded, "timeout"},
		{errors.New("boom"), "other"},
	} {
		if got := errorKind(tc.err); got != tc.want {
			t.Errorf("errorKind(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// TestStoreErrorsAreCountedAsFailuresOnly: answers are not failures.
func TestStoreErrorsAreCountedAsFailuresOnly(t *testing.T) {
	m, err := newMetrics(prometheus.NewRegistry(), func(string) string { return "builtin" })
	if err != nil {
		t.Fatal(err)
	}
	ts := timedStore{m: m}
	for _, e := range []error{nil, store.ErrNotFound, store.ErrConflict, store.ErrRunClosed, store.ErrLeaseStale, store.ErrLeaseUnknown, context.Canceled} {
		ts.observe("Finish", time.Now(), e)
	}
	ts.observe("Finish", time.Now(), store.ErrUnavailable)
	ts.observe("Finish", time.Now(), context.DeadlineExceeded)
	ts.observe("Finish", time.Now(), errors.New("boom"))
	for kind, want := range map[string]float64{"unavailable": 1, "timeout": 1, "other": 1} {
		if got := counterValue(t, m.storeErrors, "Finish", kind); got != want {
			t.Errorf("store errors of kind %s = %v, want %v", kind, got, want)
		}
	}
}

// TestEveryStoreMethodStartsAtZero: the list the error series are created from covers
// every method of the store, so none is added without its series.
func TestEveryStoreMethodStartsAtZero(t *testing.T) {
	typ := reflect.TypeFor[store.Store]()
	for method := range typ.Methods() {
		name := method.Name
		if name == "Notifications" { // a channel, not a call that is timed
			continue
		}
		if !slices.Contains(storeMethods, name) {
			t.Errorf("store method %s is timed but not in storeMethods", name)
		}
	}
}

func TestSplitMethod(t *testing.T) {
	if s, m := splitMethod("/crawly.frontier.run.v1.RunService/CreateRun"); s != "crawly.frontier.run.v1.RunService" || m != "CreateRun" {
		t.Errorf("%q %q", s, m)
	}
}
