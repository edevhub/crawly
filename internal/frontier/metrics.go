package frontier

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
	"github.com/edevhub/crawly/internal/store"
)

// The Frontier's metrics (README, Metrics). Every metric is registered on the registry
// the caller gives the Frontier; the Frontier opens no listener of its own.
//
// Labels keep the series bounded:
//
//   - policy is the most specific global policy that matches the queue (policy.Set.Class),
//     never the queue key, because queue keys are hosts and there is no bound on them;
//   - pool, worker and code come from the workers, so each is capped at a fixed number of
//     distinct values per process, and values past the cap are folded into "other";
//   - nothing is labelled by run, task or URL.
const (
	namespace = "crawly_frontier"

	// maxPools, maxWorkers and maxCodes cap the distinct values of a label that workers
	// choose. Generous for a real deployment, finite for a faulty one.
	maxPools   = 100
	maxWorkers = 200
	maxCodes   = 100

	// otherLabel stands for the values past a cap.
	otherLabel = "other"

	// dueLimit is how many due queues the store lists per pool for the oldest-due
	// gauge, and statsTTL how long one reading of the store's state serves scrapes.
	dueLimit = 1000
	statsTTL = 5 * time.Second
	// statsTimeout bounds one reading of the store's state during a scrape.
	statsTimeout = 5 * time.Second
)

// codeUndelivered is the release code of an assignment that never reached its worker.
const codeUndelivered = "undelivered"

// capped admits at most max distinct values and folds the rest into "other".
type capped struct {
	mu   sync.Mutex
	max  int
	seen map[string]struct{}
}

func newCapped(limit int) *capped { return &capped{max: limit, seen: map[string]struct{}{}} }

func (c *capped) label(v string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[v]; ok {
		return v
	}
	if len(c.seen) >= c.max {
		return otherLabel
	}
	c.seen[v] = struct{}{}
	return v
}

// metrics holds the event metrics of a replica. The gauges are read at scrape time by
// stateCollector.
type metrics struct {
	class func(queue string) string // the policy group of a queue

	pools, workers, codes *capped

	dispatched   *prometheus.CounterVec
	dispatchWait *prometheus.HistogramVec
	outcomes     *prometheus.CounterVec
	attempts     *prometheus.CounterVec
	released     *prometheus.CounterVec
	expiries     *prometheus.CounterVec
	blocks       *prometheus.CounterVec
	fetch        *prometheus.HistogramVec
	added        *prometheus.CounterVec
	opened       *prometheus.CounterVec
	storeTime    *prometheus.HistogramVec
	storeErrors  *prometheus.CounterVec
	handled      *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer, class func(string) string) (*metrics, error) {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Name: name, Help: help, Buckets: buckets}, labels)
	}
	m := &metrics{class: class, pools: newCapped(maxPools), workers: newCapped(maxWorkers), codes: newCapped(maxCodes),
		dispatched: counter("dispatched_tasks_total", "Tasks handed to workers, by pool and the policy of the queue.", "pool", "policy"),
		dispatchWait: histogram("dispatch_wait_seconds",
			"How long a task was eligible before it was dispatched, by the policy of its queue: what politeness, blocks and busy workers cost.",
			[]float64{0.01, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600, 14400}, "policy"),
		outcomes: counter("task_outcomes_total",
			"Outcomes workers reported, by pool, the kind and code the worker gave, and the result the Frontier decided on (succeeded, retry, failed, released).",
			"pool", "kind", "code", "result"),
		attempts: counter("attempts_total", "Attempts that ended with an outcome or a lease expiry. A release is not an attempt.", "pool"),
		released: counter("released_tasks_total",
			"Tasks handed back to the Frontier, by pool and code (no_handler, shutdown, or undelivered for an assignment that never reached its worker).",
			"pool", "code"),
		expiries: counter("lease_expiries_total", "Leases the reaper found expired, by the worker that held them and the pool.", "worker", "pool"),
		blocks:   counter("queue_blocks_total", "Queue blocks that outcomes set, by the policy of the queue.", "policy"),
		fetch: histogram("fetch_duration_seconds", "Fetch duration workers reported, by the policy of the queue.",
			[]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}, "policy"),
		added: counter("tasks_added_total",
			"Tasks submitted, by source (seed, discovered) and result (added, duplicate, over_budget, run_closed, invalid).", "source", "status"),
		opened: counter("sessions_opened_total", "Worker sessions opened on this replica, by pool.", "pool"),
		storeTime: histogram("store_call_duration_seconds", "Duration of store calls, by method.",
			[]float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}, "method"),
		storeErrors: counter("store_errors_total",
			"Store calls that failed, by method and kind (unavailable, other). Answers such as not found and a stale lease are not failures.",
			"method", "kind"),
		handled: counter("grpc_handled_total", "gRPC calls this replica finished, by method and status code.", "method", "code"),
	}
	for _, c := range []prometheus.Collector{m.dispatched, m.dispatchWait, m.outcomes, m.attempts, m.released, m.expiries, m.blocks,
		m.fetch, m.added, m.opened, m.storeTime, m.storeErrors, m.handled} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// outcomeKinds names the kinds a worker can report; anything else is "unknown", so that
// a faulty worker cannot make up label values.
var outcomeKinds = map[taskv1.Outcome_Kind]string{
	taskv1.Outcome_KIND_SUCCESS: "success", taskv1.Outcome_KIND_RETRY: "retry",
	taskv1.Outcome_KIND_FAILED: "failed", taskv1.Outcome_KIND_RELEASED: "released",
}

var results = map[store.TaskResult]string{
	store.ResultSucceeded: "succeeded", store.ResultFailed: "failed", store.ResultRetry: "retry", store.ResultReleased: "released",
}

var addStatuses = map[store.AddStatus]string{
	store.AddAdded: "added", store.AddDuplicate: "duplicate", store.AddOverBudget: "over_budget",
	store.AddRunClosed: "run_closed", store.AddInvalid: "invalid",
}

func name(m map[taskv1.Outcome_Kind]string, k taskv1.Outcome_Kind) string {
	if v, ok := m[k]; ok {
		return v
	}
	return "unknown"
}

// dispatch records the assignments of one Dispatch call.
func (m *metrics) dispatch(pool store.PoolName, as []store.Assignment) {
	p := m.pools.label(string(pool))
	for i := range as {
		class := m.class(string(as[i].Info.Queue))
		m.dispatched.WithLabelValues(p, class).Inc()
		m.dispatchWait.WithLabelValues(class).Observe(as[i].Waited.Seconds())
	}
}

// finished records an outcome the store accepted. kind is the worker's.
func (m *metrics) finished(info store.LeaseInfo, kind taskv1.Outcome_Kind, f store.Finish) {
	pool, class := m.pools.label(string(info.Pool)), m.class(string(info.Queue))
	result := results[f.Result]
	code := m.codes.label(f.Record.Code)
	m.outcomes.WithLabelValues(pool, name(outcomeKinds, kind), code, result).Inc()
	if f.Result == store.ResultReleased {
		m.released.WithLabelValues(pool, code).Inc()
	} else {
		m.attempts.WithLabelValues(pool).Inc()
	}
	if f.Block > 0 {
		m.blocks.WithLabelValues(class).Inc()
	}
	if f.Record.Latency > 0 {
		m.fetch.WithLabelValues(class).Observe(f.Record.Latency.Seconds())
	}
}

// expired records a lease the reaper ended.
func (m *metrics) expired(info store.LeaseInfo, f store.Finish) {
	pool := m.pools.label(string(info.Pool))
	worker := info.Worker.ID
	if worker == "" {
		worker = "unknown"
	}
	m.expiries.WithLabelValues(m.workers.label(worker), pool).Inc()
	m.finished(info, taskv1.Outcome_KIND_RETRY, f)
}

// undelivered records an assignment returned because it never reached its worker.
func (m *metrics) undelivered(pool store.PoolName) {
	p := m.pools.label(string(pool))
	m.released.WithLabelValues(p, codeUndelivered).Inc()
	m.outcomes.WithLabelValues(p, "released", codeUndelivered, "released").Inc()
}

// tasksAdded records the results of adding tasks.
func (m *metrics) tasksAdded(source string, res []store.AddResult) {
	for _, r := range res {
		m.added.WithLabelValues(source, addStatuses[r.Status]).Inc()
	}
}

// grpcHandled records a finished call.
func (m *metrics) grpcHandled(method string, err error) {
	m.handled.WithLabelValues(method, status.Code(err).String()).Inc()
}

// MetricsOptions are the gRPC server options that count the calls the replica handles.
// They add to ServerOptions.
func (s *Server) MetricsOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			resp, err := h(ctx, req)
			s.m.grpcHandled(info.FullMethod, err)
			return resp, err
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			err := h(srv, ss)
			s.m.grpcHandled(info.FullMethod, err)
			return err
		}),
	}
}

// ---- gauges, read at scrape time

// stateCollector reports what the replica holds in memory (per replica) and what the
// store holds (global: every replica reports the same).
type stateCollector struct {
	s *Server

	sessions, credits, assignments *prometheus.Desc
	tasks, oldestDue, info         *prometheus.Desc

	mu     sync.Mutex
	read   time.Time // wall clock of the last reading
	at     time.Time // the store clock at the last reading
	stats  []store.PoolStats
	failed bool
}

func newStateCollector(s *Server) *stateCollector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(namespace+"_"+name, help, labels, nil)
	}
	return &stateCollector{s: s,
		sessions:    desc("sessions", "Worker sessions open on this replica, by pool. Per replica: sum over replicas.", "pool"),
		credits:     desc("credits", "Execution slots workers granted on this replica and have not used, by pool. Per replica: sum over replicas.", "pool"),
		assignments: desc("assignments", "Assignments held by sessions of this replica and not completed, by pool. Per replica: sum over replicas.", "pool"),
		tasks: desc("pool_tasks",
			"Tasks of a pool across all runs, by state: pending (ready to dispatch), delayed (waiting for a time, a slot or a resume), in_flight. Global: every replica reports the same, use max.",
			"pool", "state"),
		oldestDue: desc("oldest_due_task_age_seconds",
			"Age of the longest waiting ready work in a pool, by the policy of its queue: how long nothing but a worker has been missing. Absent when no work is due. Global: use max.",
			"pool", "policy"),
		info: desc("replica_info", "Always 1; names the replica and the policy set it runs.", "replica", "policy_set"),
	}
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.sessions, c.credits, c.assignments, c.tasks, c.oldestDue, c.info} {
		ch <- d
	}
}

// poolStats reads the store's state, at most once per statsTTL, with the store clock at
// the reading. ok is false when the store could not be read.
func (c *stateCollector) poolStats() (stats []store.PoolStats, now time.Time, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.read.IsZero() || time.Since(c.read) >= statsTTL {
		ctx, cancel := context.WithTimeout(context.Background(), statsTimeout)
		defer cancel()
		at, err := c.s.store.Now(ctx)
		if err == nil {
			c.stats, err = c.s.store.PoolStats(ctx, dueLimit)
		}
		c.read, c.at, c.failed = time.Now(), at, err != nil
		if err != nil {
			c.stats = nil
			c.s.log.Warn("metrics: reading the pool statistics", "err", err)
		}
	}
	return c.stats, c.at.Add(time.Since(c.read)), !c.failed
}

type localPool struct{ sessions, credits, assignments int }

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.s
	ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1, s.cfg.ReplicaID, s.pol.Hash())
	local := map[string]*localPool{}
	s.mu.Lock()
	pools := make([]*pool, 0, len(s.pools))
	for _, p := range s.pools {
		pools = append(pools, p)
	}
	s.mu.Unlock()
	for _, p := range pools {
		lp := &localPool{}
		p.mu.Lock()
		for sess := range p.sessions {
			lp.sessions++
			lp.credits += sess.credits
			lp.assignments += len(sess.leases)
		}
		p.mu.Unlock()
		local[s.m.pools.label(string(p.name))] = lp
	}
	stats, now, ok := c.poolStats() // without a reading the global gauges are left out, not zero
	reported := map[string]bool{}
	task := func(pool, state string, n uint64) {
		ch <- prometheus.MustNewConstMetric(c.tasks, prometheus.GaugeValue, float64(n), pool, state)
	}
	for i := range stats {
		ps := &stats[i]
		pool := s.m.pools.label(string(ps.Pool))
		reported[pool] = true
		if local[pool] == nil {
			local[pool] = &localPool{} // a pool with work and no session here still has none
		}
		task(pool, "pending", ps.Pending)
		task(pool, "delayed", ps.Delayed)
		task(pool, "in_flight", ps.InFlight)
		oldest := map[string]time.Time{}
		for _, d := range ps.Due {
			class := s.m.class(string(d.Queue))
			if at, seen := oldest[class]; !seen || d.Since.Before(at) {
				oldest[class] = d.Since
			}
		}
		for class, since := range oldest {
			ch <- prometheus.MustNewConstMetric(c.oldestDue, prometheus.GaugeValue, max(now.Sub(since), 0).Seconds(), pool, class)
		}
	}
	for pool, lp := range local {
		if ok && !reported[pool] {
			// A pool this replica knows that has no work: zero, so that it can be told from a pool nobody reports.
			task(pool, "pending", 0)
			task(pool, "delayed", 0)
			task(pool, "in_flight", 0)
		}
		ch <- prometheus.MustNewConstMetric(c.sessions, prometheus.GaugeValue, float64(lp.sessions), pool)
		ch <- prometheus.MustNewConstMetric(c.credits, prometheus.GaugeValue, float64(lp.credits), pool)
		ch <- prometheus.MustNewConstMetric(c.assignments, prometheus.GaugeValue, float64(lp.assignments), pool)
	}
}
