package frontier

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/policy"
	"github.com/edevhub/crawly/internal/store"
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
		// The capped codes and "other" of the retries, and one series per capped pool for
		// the assignments that were never delivered.
		{"outcomes", m.outcomes, maxCodes + 1 + maxPools + 1},
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
